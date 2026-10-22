"""Client-stack adapters: real pinned libraries driven by the policy IR.

Stacks (outermost -> innermost):
  [budget gate?] -> [tenacity wrapper?] -> [requests/urllib3 transport]

Transport entries are observed at HTTPConnectionPool._make_request(); retry
decisions and server receipts are separate channels. Connect-phase entries
may produce zero receipts. Receipt and effect bookkeeping is authored, not
independently validated production ground truth.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import requests
import urllib3
from tenacity import (
    Retrying,
    RetryError as TenacityRetryError,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
    wait_none,
    retry_any,
)
from urllib3.util.retry import Retry

from .ir import Layer, Policy, scope_errors
from .ledger import BudgetExhausted, OperationLedger, RequestBudget
from .quota import PhysicalQuotaExhausted


class LedgerRetry(Retry):
    """urllib3 Retry that records every retry decision.

    urllib3 calls increment() once per *candidate* retry; a returned
    Retry object means another attempt follows, a raised MaxRetryError
    means the budget is exhausted. We log both outcomes.
    """

    def __init__(self, *args, ledger: OperationLedger | None = None,
                 budget: RequestBudget | None = None,
                 layer_name: str = "urllib3", **kw):
        super().__init__(*args, **kw)
        self._op_ledger = ledger
        self._budget = budget
        self._layer_name = layer_name
        self._gated = False

    def _child(self, **kw) -> "LedgerRetry":
        kw.setdefault("ledger", self._op_ledger)
        kw.setdefault("budget", self._budget)
        kw.setdefault("layer_name", self._layer_name)
        return kw

    def new(self, **kw):  # urllib3 clones via new()
        return super().new(**self._child(**kw))

    def increment(self, method=None, url=None, response=None, error=None,
                  _pool=None, _stacktrace=None):
        try:
            new_retry = super().increment(method, url, response, error,
                                          _pool, _stacktrace)
        except Exception as e:  # MaxRetryError: budget exhausted, no send
            if self._op_ledger is not None:
                self._op_ledger.record(
                    self._layer_name, "retry_exhausted",
                    error=type(e).__name__,
                    status=getattr(response, "status", None))
            raise
        return new_retry

    # urllib3 calls sleep()/sleep_for_retry() after increment() and BEFORE
    # sending the next attempt — that is the correct point to enforce
    # "no NEW attempt after deadline" (the wait itself may cross it) and
    # to consume the shared attempt budget exactly once per real send.
    def sleep(self, response=None):
        super().sleep(response)
        self._gate()

    def sleep_for_retry(self, response=None):
        super().sleep_for_retry(response)
        self._gate()

    def _gate(self):
        # sleep() internally calls sleep_for_retry() when it honors a
        # Retry-After header; sleep_for_retry() is also called directly
        # for redirects. A per-decision flag covers both call paths
        # without double-consuming the shared budget or double-counting
        # the scheduled retry.
        if self._gated:
            return
        self._gated = True
        if self._budget is not None and not self._budget.try_consume(
            self._op_ledger.op_id if self._op_ledger else "?",
            self._layer_name,
        ):
            if self._op_ledger is not None:
                self._op_ledger.record(
                    self._layer_name, "budget_refusal",
                    reason=self._budget.refusals[-1]["reason"])
            raise BudgetExhausted(self._budget.refusals[-1]["reason"])
        if self._op_ledger is not None:
            self._op_ledger.record(self._layer_name, "retry_scheduled")


def build_urllib3_retry(layer: Layer, ledger: OperationLedger | None = None,
                        budget: RequestBudget | None = None) -> LedgerRetry:
    """Translate an IR urllib3 layer into a real urllib3 Retry."""
    ctrs = layer.type_counters or (None, None, None, None)
    total = None if layer.max_attempts is None else layer.max_attempts - 1
    return LedgerRetry(
        total=total,
        connect=ctrs[0], read=ctrs[1], status=ctrs[2], other=ctrs[3],
        allowed_methods=layer.predicate.methods,
        status_forcelist=sorted(layer.predicate.on_statuses) or None,
        backoff_factor=layer.backoff.base_s
        if layer.backoff.kind == "urllib3_exp" else 0.0,
        raise_on_status=layer.raise_on_status,
        respect_retry_after_header=layer.respect_retry_after,
        retry_after_max=layer.retry_after_max_s or 21600,
        ledger=ledger, budget=budget, layer_name=layer.name,
    )


@dataclass
class OpResult:
    op_id: str
    outcome: str  # success | http_error | exception | budget_exhausted
    status: int | None
    transport_attempts: int
    wrapper_invocations: int
    wall_s: float
    error: str | None


class RequestsTransport:
    """Innermost stack node: one requests call (urllib3 retries inside)."""

    def __init__(self, layer: Layer, base_url: str, cell_id: str,
                 ledger: OperationLedger | None = None,
                 budget: RequestBudget | None = None, quota=None):
        self.layer = layer
        self.base_url = base_url
        self.cell_id = cell_id
        self.ledger = ledger
        self.session = requests.Session()
        retry = build_urllib3_retry(layer, ledger, budget)
        class ObservedPool(urllib3.connectionpool.HTTPConnectionPool):
            def _make_request(pool_self, *args, **kwargs):
                if quota is not None:
                    quota.consume()
                if ledger is not None:
                    ledger.record("transport", "physical_attempt_start")
                return super()._make_request(*args, **kwargs)

        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        # Copy the instance mapping: never modify urllib3's global registry.
        adapter.poolmanager.pool_classes_by_scheme = {
            **adapter.poolmanager.pool_classes_by_scheme, "http": ObservedPool}
        self.session.trust_env = False
        self.session.mount("http://", adapter)
        self.timeout = (
            layer.timeout.connect_s if layer.timeout.connect_s is not None else 2.0,
            layer.timeout.read_s if layer.timeout.read_s is not None else 5.0,
        )
        # shared budget is also consumed by the initial send of each
        # urllib3 invocation — one consumption per transport attempt
        self.budget = budget

    def send_once(self, op_id: str, method: str, path: str,
                  body: bytes | None = None) -> requests.Response:
        if self.ledger:
            self.ledger.record(self.layer.name, "invoke")
        if self.budget is not None and not self.budget.try_consume(
            op_id, self.layer.name
        ):
            if self.ledger:
                self.ledger.record(self.layer.name, "budget_refusal",
                                   reason=self.budget.refusals[-1]["reason"])
            raise BudgetExhausted(self.budget.refusals[-1]["reason"])
        if self.ledger:
            self.ledger.record(self.layer.name, "attempt_start")
        try:
            return self.session.request(
                method, self.base_url + path,
                headers={"X-Op-Id": op_id, "X-Cell-Id": self.cell_id},
                data=body, timeout=self.timeout)
        finally:
            if self.ledger:
                self.ledger.record(self.layer.name, "attempt_end")


def _tenacity_wait(layer: Layer):
    b = layer.backoff
    if b.kind == "none":
        return wait_none()
    if b.kind == "fixed":
        return wait_fixed(b.base_s)
    if b.kind == "tenacity_exp":
        options = {"multiplier": b.base_s, "min": b.min_s}
        if b.max_s is not None:
            options["max"] = b.max_s
        return wait_exponential(**options)
    return wait_none()


def _tenacity_stop(layer: Layer, budget: RequestBudget | None):
    stops = []
    if layer.max_attempts is not None:
        stops.append(stop_after_attempt(layer.max_attempts))
    if layer.soft_deadline_s is not None:
        stops.append(stop_after_delay(layer.soft_deadline_s))
    if not stops:
        return stop_after_attempt(10**9)  # unbounded within the run
    s = stops[0]
    for extra in stops[1:]:
        s = s | extra
    if budget is not None and budget.deadline_mono is not None:
        s = s | stop_after_delay(
            max(budget.deadline_mono - time.monotonic(), 0.0))
    return s


def _tenacity_pred(layer: Layer):
    preds = []
    if layer.predicate.on_connect or layer.predicate.on_read \
            or layer.predicate.on_other:
        preds.append(retry_if_exception_type(
            (requests.RequestException, urllib3.exceptions.HTTPError, TenacityRetryError)))
    if layer.predicate.on_statuses:
        allowed = set(layer.predicate.on_statuses)
        preds.append(retry_if_result(
            lambda r: getattr(r, "status_code", r) in allowed))
    if not preds:
        return retry_if_result(lambda r: False)
    p = preds[0]
    for extra in preds[1:]:
        p = retry_any(p, extra)
    return p


def execute_op(policy: Policy, base_url: str, cell_id: str,
               op_id: str, body: bytes | None = None, quota=None) -> tuple:
    """Run one logical operation through the real stack.

    Returns (OpResult, OperationLedger). Layer order in the IR is
    outermost-first; a "budget" layer creates the shared RequestBudget
    consumed by the inner layers' retry decisions.
    """
    if errors := scope_errors(policy):
        raise ValueError(errors[0])
    ledger = OperationLedger(op_id)
    layers = list(policy.layers)

    budget = None
    for l in layers:
        if l.kind == "budget":
            budget = RequestBudget(l.budget_attempts or 0, l.deadline_s)
            ledger.record("budget", "budget_created",
                          deadline_mono=budget.deadline_mono)
    retry_layers = [l for l in layers if l.kind != "budget"]

    # innermost transport
    t_layer = retry_layers[-1]
    transport = RequestsTransport(t_layer, base_url, cell_id, ledger, budget, quota)
    fn = lambda: transport.send_once(op_id, policy.method, "/op", body)

    # wrap outer layers (excluding transport) innermost-out
    for layer in reversed(retry_layers[:-1]):
        inner_fn = fn
        if layer.kind == "status_check":
            def checked(inner):
                def call():
                    response = inner()
                    ledger.record("status_check", "response_checked",
                                  status=response.status_code)
                    response.raise_for_status()
                    return response
                return call
            fn = checked(inner_fn)
        elif layer.kind == "tenacity":
            r = Retrying(
                stop=_tenacity_stop(layer, budget),
                wait=_tenacity_wait(layer),
                retry=_tenacity_pred(layer),
                reraise=True,
            )
            def make(inner, retrying, lname):
                def call():
                    n = [0]
                    def wrapped():
                        n[0] += 1
                        ledger.record(lname, "invoke", n=n[0])
                        return inner()
                    return retrying(wrapped)
                return call
            fn = make(inner_fn, r, layer.name)
        else:
            raise ValueError(f"unsupported outer layer kind {layer.kind}")

    t0 = time.monotonic()
    outcome, status, err = "exception", None, None
    try:
        resp = fn()
        status = resp.status_code
        outcome = "success" if status < 400 else "http_error"
    except BudgetExhausted as e:
        outcome, err = "budget_exhausted", str(e)
    except PhysicalQuotaExhausted as e:
        outcome, err = "quota_exhausted", str(e)
    except TenacityRetryError as e:
        # tenacity result-path exhaustion wraps the last Response;
        # reraise=True only applies to exceptions
        outcome, err = "exception", "RetryError(result)"
        try:
            status = e.last_attempt.result().status_code
        except Exception:
            pass
    except requests.RequestException as e:
        outcome, err = "exception", type(e).__name__
    except urllib3.exceptions.HTTPError as e:
        outcome, err = "exception", type(e).__name__
    finally:
        # Release keep-alive connections before the server completion drain.
        transport.session.close()
    wall = time.monotonic() - t0

    # transport attempts = one send per transport-layer call plus every
    # retry urllib3 actually scheduled (exhausted increments send nothing)
    attempts = ledger.count("physical_attempt_start")
    result = OpResult(
        op_id=op_id, outcome=outcome, status=status,
        transport_attempts=attempts,
        wrapper_invocations=ledger.count("invoke"),
        wall_s=wall, error=err)
    ledger.record("op", "result", outcome=outcome, status=status,
                  attempts=attempts, wall_s=round(wall, 6))
    return result, ledger
