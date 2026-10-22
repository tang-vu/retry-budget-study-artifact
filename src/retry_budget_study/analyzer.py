"""Static analyzer over the policy IR.

Computes conditional bounds for a composed policy under a persistent
outcome class, then a contract verdict. Bounds are *conditional*: they
carry the assumptions under which they hold (see protocol/contract.md A1-A4).

Signal model: each sub-stack returns to its caller one of
  "success" | "exception" | ("response", status)
which determines the outer layer's eligibility decision — this is what
makes composition non-trivial (urllib3 raise_on_status=False changes a
status outcome from "exception" to "response", rerouting it through the
outer layer's result predicate instead of its exception predicate).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .ir import Layer, Outcome, Policy, scope_errors

INF = float("inf")

# Assumption A3: conservative loopback attempt-cost floors/ceilings used
# when a cell does not provide tighter measured bounds. MIN values
# under-estimate attempt cost (safe for deadline->attempt bounds);
# MAX values over-estimate it (safe for elapsed bounds).
COST_FLOOR_S = {
    "conn_err": 0.0005,
    "read_err": 0.0005,
    "other_err": 0.0005,
    "status": 0.0005,
    "success": 0.0005,
}
COST_CEIL_S = {
    "conn_err": 0.02,
    "read_err": None,  # per-attempt cost = read timeout; set per cell
    "other_err": 0.02,
    "status": 0.02,
    "success": 0.02,
}


@dataclass(frozen=True)
class Contract:
    max_transport_attempts: int | None = None
    max_wall_s: float | None = None
    max_effects: int | None = None

    @staticmethod
    def from_dict(d: dict) -> "Contract":
        return Contract(
            d.get("max_transport_attempts"),
            d.get("max_wall_s"),
            d.get("max_effects"),
        )


@dataclass
class ExecResult:
    """Result of one call into a sub-stack under persistent outcome O."""

    attempts_min: float
    attempts_max: float
    elapsed_min_s: float
    elapsed_max_s: float
    signal: object  # "success" | "exception" | ("response", status)
    transport_cost_min: float = 0.0  # per-attempt cost bounds at the
    transport_cost_max: float = 0.0  # innermost transport node
    # max single wait any layer schedules between two consecutive sends;
    # in sequential composition inter-send gaps contain at most one wait
    # (exhausted layers return immediately without sleeping)
    wait_gap_max: float = 0.0
    # whether an inner budget layer already owns the request scope —
    # deployed semantics are innermost-wins, so outer budget layers
    # over a scoped inner are inert, not cumulative
    budget_scoped: bool = False


@dataclass
class Warning:
    code: str
    severity: str  # info | warn | critical
    message: str


@dataclass
class AnalysisResult:
    policy_name: str
    outcome: str
    verdict: str  # GUARANTEED | POSSIBLE_BREACH | WILL_BREACH | UNBOUNDED | UNKNOWN
    attempts_min: float | None
    attempts_max: float | None
    elapsed_min_s: float | None
    elapsed_max_s: float | None
    effects_max: float | None = None
    warnings: list = field(default_factory=list)
    assumptions: list = field(default_factory=list)
    repairs: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "policy": self.policy_name,
            "outcome": self.outcome,
            "verdict": self.verdict,
            "attempts_min": self.attempts_min,
            "attempts_max": self.attempts_max,
            "elapsed_min_s": self.elapsed_min_s,
            "elapsed_max_s": self.elapsed_max_s,
            "effects_max": self.effects_max,
            "warnings": [w.__dict__ for w in self.warnings],
            "assumptions": self.assumptions,
            "repairs": self.repairs,
        }


def _transport_cost(outcome: Outcome, read_timeout_s: float | None,
                    connect_timeout_s: float | None = None) -> tuple:
    """(min,max) seconds for one transport attempt under outcome."""
    floor = COST_FLOOR_S.get(outcome.kind, 0.0005)
    if outcome.kind == "conn_err":
        # on Windows loopback a dead port does not RST quickly: the SYN
        # is retransmitted by the OS and the attempt typically runs until
        # the configured connect timeout fires (measured ~2s @ 2s timeout)
        hi = connect_timeout_s if connect_timeout_s is not None else 2.0
        return (floor, hi + 0.05)
    if outcome.kind == "read_err":
        rt = read_timeout_s if read_timeout_s is not None else 0.5
        if outcome.delay_s and outcome.delay_s > 0:
            # server holds delay_s then acts; a read error surfaces at
            # min(hold, timeout) — the timeout CUTS the delay short
            dur = min(outcome.delay_s, rt)
            return (max(dur - 0.02, floor), dur + 0.02)
        # delay_s==0: drop/RST surfaces near-instantly
        return (floor, 0.02)
    # status/success/other: server holds delay_s then responds, so the
    # client waits the full delay; plus a small response ceiling
    lo = floor + outcome.delay_s
    hi = COST_CEIL_S.get(outcome.kind, 0.02)
    if hi is None:
        hi = 0.02
    return (lo, hi + outcome.delay_s)


def _u3_eligible(layer: Layer, outcome: Outcome, method: str) -> bool:
    """Is this outcome retryable by a urllib3 layer for `method`?"""
    p = layer.predicate
    if outcome.kind in ("status", "read_err") and p.methods is not None and method.upper() not in p.methods:
        return False
    if outcome.kind == "conn_err":
        return p.on_connect
    if outcome.kind == "read_err":
        return p.on_read
    if outcome.kind == "other_err":
        return p.on_other
    if outcome.kind == "status":
        if outcome.status in p.on_statuses:
            return True
        if (
            layer.respect_retry_after
            and outcome.retry_after_s is not None
            and outcome.status in p.ra_statuses
        ):
            return True
        return False
    return False


def _u3_type_cap(layer: Layer, outcome: Outcome) -> float:
    """Attempt cap inside one urllib3 invocation under persistent outcome.

    urllib3 decrements `total` AND the matching per-type counter; the
    first to exhaust stops retries, so the cap is the min of the two.
    """
    cap = float(layer.max_attempts) if layer.max_attempts is not None else INF
    if layer.type_counters is not None:
        c, r, s, o = layer.type_counters
        ctr = {
            "conn_err": c,
            "read_err": r,
            "other_err": o,
            "status": s,
        }.get(outcome.kind)
        if ctr is not None:
            cap = min(cap, float(1 + ctr))
    return cap


def _u3_wait(layer: Layer, outcome: Outcome, retry_index: int) -> float:
    ra = outcome.retry_after_s if layer.respect_retry_after else None
    kind = "retry_after" if ra is not None else layer.backoff.kind
    if kind == "retry_after":
        return layer.backoff.delay_for(
            retry_index, retry_after_s=ra, ra_max_s=layer.retry_after_max_s
        )
    return layer.backoff.delay_for(retry_index)


def _max_layer_wait(wait_fn, n_max: float) -> float:
    """Largest single wait a layer can schedule before a retry."""
    if n_max == INF or n_max <= 1:
        if n_max == INF:
            # unbounded retries: sample far enough for capped exp to peak
            k_hi = 1001
        else:
            return 0.0
    else:
        k_hi = int(n_max)
    return max(wait_fn(k) for k in range(1, k_hi)) if k_hi > 1 else 0.0


def _exec(layers: tuple, i: int, outcome: Outcome, method: str,
          read_timeout_s: float | None, warnings: list,
          connect_timeout_s: float | None = None) -> ExecResult:
    """Bound one invocation of sub-stack layers[i:] under persistent O."""
    if i >= len(layers):  # transport node
        lo, hi = _transport_cost(outcome, read_timeout_s, connect_timeout_s)
        if outcome.kind == "success":
            sig = "success"
        elif outcome.kind == "status":
            sig = ("response", outcome.status)
        else:
            sig = "exception"
        return ExecResult(1, 1, lo, hi, sig, lo, hi)

    layer = layers[i]
    inner = _exec(layers, i + 1, outcome, method, read_timeout_s,
                  warnings, connect_timeout_s)
    tc_min = inner.transport_cost_min
    tc_max = inner.transport_cost_max
    wg = inner.wait_gap_max

    if layer.predicate.unknown:
        warnings.append(
            Warning("UNKNOWN_PREDICATE", "critical",
                    f"layer {layer.name}: custom predicate not modelable"))
        return ExecResult(1, INF, 0.0, INF, "exception", tc_min, tc_max, wg)

    if layer.kind == "status_check":
        # A non-retrying application boundary, e.g. Response.raise_for_status.
        # It changes the signal seen by outer retries, not the send count.
        if (isinstance(inner.signal, tuple) and inner.signal[0] == "response"
                and 400 <= inner.signal[1] < 600):
            inner.signal = "exception"
            warnings.append(Warning("APPLICATION_STATUS_CHECK", "info",
                                    "application converts HTTP error response to exception"))
        return inner

    if layer.kind == "budget":
        if inner.budget_scoped:
            warnings.append(Warning(
                "SHADOWED_BUDGET", "warn",
                f"layer {layer.name}: an inner budget layer already owns "
                "the request scope; this layer is inert under the "
                "deployed innermost-wins semantics"))
            return ExecResult(inner.attempts_min, inner.attempts_max,
                              inner.elapsed_min_s, inner.elapsed_max_s,
                              inner.signal, tc_min, tc_max, wg,
                              budget_scoped=True)
        a_max = min(inner.attempts_max, float(layer.budget_attempts or INF))
        if layer.deadline_s is not None:
            # no NEW attempt after deadline: bound by min-cost transport cycle
            floor = max(tc_min, 1e-4)
            a_max = min(a_max, math.ceil(layer.deadline_s / floor))
            # refusal fires at the first post-deadline send attempt;
            # elapsed overshoots the deadline by at most one inter-send
            # gap (one wait + one in-flight attempt)
            e_max = min(inner.elapsed_max_s,
                        layer.deadline_s + tc_max + wg)
            warnings.append(Warning(
                "DEADLINE_GATES_ONLY", "info",
                "hard deadline blocks NEW attempts only; in-flight I/O and "
                f"scheduled waits may overshoot by <= "
                f"{tc_max + wg:.3f}s"))
        else:
            e_max = inner.elapsed_max_s
        if a_max != INF:
            # the attempt cap also bounds elapsed: n sends with at most
            # one scheduled wait between each pair
            e_max = min(e_max,
                        a_max * tc_max + max(a_max - 1.0, 0.0) * wg)
        a_min = min(inner.attempts_min, a_max)
        e_min = min(inner.elapsed_min_s, e_max)
        return ExecResult(a_min, a_max, e_min, e_max, inner.signal,
                          tc_min, tc_max, wg, budget_scoped=True)

    # ---- retry-capable layer: decide what inner returns ----------------
    sig = inner.signal
    if layer.kind == "urllib3":
        eligible = (sig == "exception" and _u3_eligible(layer, outcome, method)) or \
                   (isinstance(sig, tuple) and sig[0] == "response" and
                    _u3_eligible(layer, outcome, method))
        if not eligible:
            # outcome passes through unmodified
            new_sig = sig
            return ExecResult(inner.attempts_min, inner.attempts_max,
                              inner.elapsed_min_s, inner.elapsed_max_s,
                              new_sig, tc_min, tc_max, wg)
        n_max = _u3_type_cap(layer, outcome)
        if n_max == INF:
            warnings.append(Warning(
                "NO_ATTEMPT_CAP", "critical",
                f"layer {layer.name}: no attempt cap for outcome "
                f"{outcome.kind}"))
        n_min = n_max  # persistent outcome: deterministic exhaustion
        # urllib3: the retry loop retries the TRANSPORT, so inner is the
        # transport itself; attempts multiply by n
        att_min = inner.attempts_min * n_min
        att_max = inner.attempts_max * n_max
        waits_max = (sum(_u3_wait(layer, outcome, k)
                         for k in range(1, int(n_max))) if n_max != INF else INF)
        e_min = inner.elapsed_min_s * n_min
        e_max = (inner.elapsed_max_s * n_max + waits_max) if n_max != INF else INF
        if outcome.retry_after_s is not None and layer.respect_retry_after:
            warnings.append(Warning(
                "RETRY_AFTER_DOMINATES", "warn",
                f"layer {layer.name}: Retry-After "
                f"{outcome.retry_after_s}s overrides backoff (cap "
                f"{layer.retry_after_max_s}s)"))
        if sig == "exception":
            new_sig = "exception"
        else:
            # exhausted status: raise or return last response
            new_sig = "exception" if layer.raise_on_status else sig
        wg2 = max(wg, _max_layer_wait(
            lambda k: _u3_wait(layer, outcome, k), n_max))
        return ExecResult(att_min, att_max, e_min, e_max, new_sig,
                          tc_min, tc_max, wg2)

    if layer.kind == "tenacity":
        # eligibility of inner's SIGNAL (not the raw outcome)
        if sig == "exception":
            eligible = (layer.predicate.on_connect or
                        layer.predicate.on_read or
                        layer.predicate.on_other)
        elif isinstance(sig, tuple) and sig[0] == "response":
            eligible = sig[1] in layer.predicate.on_statuses
        else:
            eligible = False
        if not eligible:
            return ExecResult(inner.attempts_min, inner.attempts_max,
                              inner.elapsed_min_s, inner.elapsed_max_s,
                              sig, tc_min, tc_max, wg)
        n_max = float(layer.max_attempts) if layer.max_attempts else INF
        if layer.soft_deadline_s is not None:
            # stop checked BETWEEN attempts: invocations bounded by delay /
            # min cycle (inner exec + wait), and overshoots by <= one exec
            cycle_min = inner.elapsed_min_s + max(
                layer.backoff.delay_for(1), 0.0)
            cycle_min = max(cycle_min, 1e-4)
            n_deadline = 1 + math.ceil(layer.soft_deadline_s / cycle_min)
            n_max = min(n_max, float(n_deadline))
            if layer.max_attempts is None:
                warnings.append(Warning(
                    "NO_ATTEMPT_CAP", "warn",
                    f"layer {layer.name}: stop_after_delay without attempt "
                    "cap; bound depends on cost model A3"))
            warnings.append(Warning(
                "SOFT_DEADLINE_OVERSHOOT", "info",
                "stop_after_delay is checked between attempts; the last "
                "inner execution may finish past the delay"))
        if n_max == INF:
            warnings.append(Warning(
                "NO_ATTEMPT_CAP", "critical",
                f"layer {layer.name}: unbounded attempts"))
        # A time stop may fire after fewer calls than the upper count derived
        # from a minimum cost. Do not use that upper count as a lower bound.
        n_min = 1.0 if layer.soft_deadline_s is not None else n_max
        att_min = inner.attempts_min * n_min
        att_max = inner.attempts_max * n_max
        k = int(n_max) if n_max != INF else 0
        wait_max = sum(layer.backoff.delay_for(j) for j in range(1, k))
        e_min = inner.elapsed_min_s * n_min
        e_max = inner.elapsed_max_s * n_max + wait_max if n_max != INF else INF
        if layer.soft_deadline_s is not None and e_max != INF:
            e_max = min(e_max, layer.soft_deadline_s
                        + inner.elapsed_max_s
                        + layer.backoff.delay_for(k))
        # With reraise=True an exception is re-raised; exhausting a result
        # predicate still raises RetryError. An enclosing retry sees an
        # exception in both cases, not the last rejected response.
        new_sig = "exception"
        wg2 = max(wg, _max_layer_wait(layer.backoff.delay_for, n_max))
        return ExecResult(att_min, att_max, e_min, e_max, new_sig,
                          tc_min, tc_max, wg2)

    if layer.kind == "httpx":
        # survey semantics (httpcore 1.x): retries CONNECT-phase failures
        # only; status/read/other outcomes pass through. Retries are NOT
        # backoff-free: httpcore sleeps exponential_backoff(0.5) between
        # connect attempts — 0s, 0.5s, 1s, 2s... (RETRIES_BACKOFF_FACTOR).
        if outcome.kind != "conn_err":
            return ExecResult(inner.attempts_min, inner.attempts_max,
                              inner.elapsed_min_s, inner.elapsed_max_s,
                              sig, tc_min, tc_max, wg)
        n_max = float(layer.max_attempts) if layer.max_attempts else INF
        waits_max = 0.0
        if n_max != INF:
            waits_max = sum(layer.backoff.delay_for(k)
                            for k in range(1, int(n_max)))
        e_max = (inner.elapsed_max_s * n_max + waits_max
                 if n_max != INF else INF)
        wg2 = max(wg, _max_layer_wait(layer.backoff.delay_for, n_max))
        return ExecResult(
            inner.attempts_min * n_max, inner.attempts_max * n_max,
            inner.elapsed_min_s * n_max, e_max,
            "exception", tc_min, tc_max, wg2)

    warnings.append(Warning("UNKNOWN_LAYER_KIND", "critical",
                            f"layer {layer.name}: kind {layer.kind}"))
    return ExecResult(1, INF, 0.0, INF, "exception", tc_min, tc_max, wg)


def analyze(policy: Policy, outcome: Outcome, contract: Contract,
            read_timeout_s: float | None = None,
            connect_timeout_s: float | None = None) -> AnalysisResult:
    warnings: list[Warning] = []

    errors = scope_errors(policy)
    if errors:
        return AnalysisResult(policy.name, outcome.kind, "UNKNOWN", None, None,
                              None, None, warnings=[Warning(
                                  "UNSUPPORTED_BUDGET_SCOPE", "critical", errors[0])])

    if any(
        l.kind == "urllib3"
        and outcome.kind in ("status", "read_err")
        and l.predicate.methods is not None
        and policy.method.upper() not in l.predicate.methods
        for l in policy.layers
    ):
        warnings.append(Warning(
            "METHOD_NOT_ELIGIBLE", "info",
            f"method {policy.method} outside urllib3 allowed_methods; "
            "inner layer will not retry (attempts collapse)"))

    res = _exec(policy.layers, 0, outcome, policy.method,
                read_timeout_s, warnings, connect_timeout_s)

    assumptions = [
        "A1 per-invocation layer state (no cross-op sharing)",
        "A2 persistent outcome per cell (heterogeneous mixes via simulator)",
        "A3 loopback attempt-cost model (floors/ceilings documented)",
        "A4 Windows timer granularity ~15.6ms adds timing uncertainty",
    ]

    # completed effects are bounded by reaching attempts: every attempt
    # that is not a connect-phase failure can reach the server and apply
    # an effect; connect-phase attempts never do
    effects_max = (None if res.attempts_max == INF else
                   (0.0 if outcome.kind == "conn_err" else res.attempts_max))

    if any(w.code == "UNKNOWN_PREDICATE" or w.code == "UNKNOWN_LAYER_KIND"
           for w in warnings):
        verdict = "UNKNOWN"
    elif res.attempts_max == INF:
        verdict = "UNBOUNDED"
    elif (contract.max_transport_attempts is not None
          and res.attempts_min > contract.max_transport_attempts):
        verdict = "WILL_BREACH"
    elif (contract.max_transport_attempts is not None
          and res.attempts_max > contract.max_transport_attempts):
        verdict = "POSSIBLE_BREACH"
    elif (contract.max_effects is not None and effects_max is not None
          and effects_max > contract.max_effects):
        verdict = "POSSIBLE_BREACH"
    elif (contract.max_wall_s is not None
          and res.elapsed_max_s != INF
          and res.elapsed_max_s > contract.max_wall_s):
        verdict = "POSSIBLE_BREACH"
    else:
        verdict = "GUARANTEED"

    repairs = _suggest_repairs(policy, outcome, contract, res,
                               read_timeout_s)
    for s in repairs:
        # verify each suggestion by materializing the repaired policy and
        # re-running the bound — a resulting_bound is only credible when
        # recomputed on the repaired stack, not asserted by construction
        try:
            rr = _exec(apply_suggestion(policy, s).layers, 0, outcome,
                       policy.method, read_timeout_s, [],
                       connect_timeout_s)
            s["verified_attempts_max"] = (
                None if rr.attempts_max == INF else int(rr.attempts_max))
            s["verified_meets_contract"] = (
                contract.max_transport_attempts is not None
                and rr.attempts_max != INF
                and rr.attempts_max <= contract.max_transport_attempts)
        except Exception:
            s["verified_attempts_max"] = None
            s["verified_meets_contract"] = None

    return AnalysisResult(
        policy_name=policy.name,
        outcome=outcome.kind,
        verdict=verdict,
        attempts_min=None if res.attempts_min == INF else res.attempts_min,
        attempts_max=None if res.attempts_max == INF else res.attempts_max,
        elapsed_min_s=None if res.elapsed_min_s == INF else res.elapsed_min_s,
        elapsed_max_s=None if res.elapsed_max_s == INF else res.elapsed_max_s,
        effects_max=None if effects_max == INF else effects_max,
        warnings=warnings,
        assumptions=assumptions,
        repairs=repairs,
    )


def apply_suggestion(policy: Policy, suggestion: dict) -> Policy:
    """Materialize a repair suggestion into a modified Policy (IR level).

    Used to re-analyze repaired stacks so suggestion bounds are verified
    rather than asserted (repair verification, protocol RQ3).
    """
    layers = list(policy.layers)
    if suggestion["kind"] in ("cap_layer", "add_attempt_cap",
                              "remove_outer_retry"):
        # identity by position: layer names may repeat in a stack, so a
        # name match alone could edit a different (or several) layers
        idx = suggestion.get("layer_index")
        if idx is None:
            idx = next(i for i, l in enumerate(layers)
                       if l.name == suggestion["layer"])
        elif layers[idx].name != suggestion.get("layer"):
            raise ValueError("suggestion layer_index/name mismatch")
        new_max = (1 if suggestion["kind"] == "remove_outer_retry"
                   else suggestion["set_max_attempts"])
        l = layers[idx]
        layers[idx] = Layer(**{**l.__dict__, "max_attempts": new_max})
    elif suggestion["kind"] == "cap_shared_budget":
        idx = suggestion["layer_index"]
        if layers[idx].kind != "budget":
            raise ValueError("cap_shared_budget must target a budget layer")
        l = layers[idx]
        cap = suggestion["budget_attempts"]
        layers[idx] = Layer(**{**l.__dict__, "max_attempts": cap,
                               "budget_attempts": cap})
    elif suggestion["kind"] == "add_shared_budget":
        layers.insert(0, Layer(
            name="shared_budget", kind="budget",
            max_attempts=suggestion["budget_attempts"],
            budget_attempts=suggestion["budget_attempts"]))
    return Policy(layers=tuple(layers), method=policy.method,
                  name=f"{policy.name}+{suggestion['kind']}")


def _suggest_repairs(policy: Policy, outcome: Outcome, contract: Contract,
                     res: ExecResult, read_timeout_s: float | None) -> list:
    """Minimal-edit suggestions toward the attempt contract."""
    if (contract.max_transport_attempts is None
            or res.attempts_max <= contract.max_transport_attempts):
        return []
    target = contract.max_transport_attempts
    layers = list(policy.layers)
    retry_idx = [i for i, l in enumerate(layers)
                 if l.kind in ("urllib3", "tenacity")]
    others_prod = 1
    per_cap = {}
    for i in retry_idx:
        n = _u3_type_cap(layers[i], outcome) if layers[i].kind == "urllib3" \
            else float(layers[i].max_attempts or INF)
        per_cap[i] = n
        others_prod *= n
    suggestions = []
    finite_prod = 1
    for i in retry_idx:
        if per_cap[i] != INF:
            finite_prod *= per_cap[i]
    for i in retry_idx:
        if per_cap[i] == INF:
            # uncapped layer (e.g. soft deadline): suggest adding a cap
            prod_rest = finite_prod
            new_cap = int(target // prod_rest) if prod_rest else target
            if new_cap >= 1:
                suggestions.append({
                    "kind": "add_attempt_cap",
                    "layer": layers[i].name,
                    "layer_index": i,
                    "set_max_attempts": new_cap,
                    "edits": 1,
                    "resulting_bound": int(prod_rest * new_cap),
                })
            continue
        prod_rest = finite_prod / per_cap[i] if per_cap[i] else 1
        # other uncapped layers stay uncapped -> cap this layer cannot
        # bound the product alone; only suggest when it can
        if any(per_cap[j] == INF for j in retry_idx if j != i):
            continue
        new_cap = int(target // prod_rest) if prod_rest else target
        if new_cap >= 1:
            suggestions.append({
                "kind": "cap_layer",
                "layer": layers[i].name,
                "layer_index": i,
                "set_max_attempts": new_cap,
                "edits": 1,
                "resulting_bound": int(prod_rest * new_cap),
            })
    # single-layer option: outermost retry layer runs once; what remains
    # is the product of the INNER retry caps, not their minimum
    if len(retry_idx) >= 2:
        outer_i = retry_idx[0]
        inner_caps = [per_cap[j] for j in retry_idx if j != outer_i]
        if all(c != INF for c in inner_caps):
            inner_bound = 1.0
            for c in inner_caps:
                inner_bound *= c
            suggestions.append({
                "kind": "remove_outer_retry",
                "layer": layers[outer_i].name,
                "layer_index": outer_i,
                "set_outer_max_attempts": 1,
                "edits": 1,
                "resulting_bound": int(inner_bound),
            })
    # shared-budget repair: when the stack already contains a budget
    # layer, inserting another is inert under the deployed semantics —
    # the innermost budget wins (adapters last-wins; simulator inner
    # scope overwrites ctx.budget_remaining), so the suggestion must cap
    # the effective existing layer instead of adding a shadowed one
    budget_idx = [i for i, l in enumerate(layers) if l.kind == "budget"]
    if budget_idx:
        i = budget_idx[-1]  # innermost = effective scope
        capped = min(int(layers[i].budget_attempts or INF), target)
        suggestions.append({
            "kind": "cap_shared_budget",
            "layer": layers[i].name,
            "layer_index": i,
            "budget_attempts": capped,
            "edits": 1,
            "resulting_bound": capped,
            "note": "tighten the effective existing budget layer",
        })
    else:
        suggestions.append({
            "kind": "add_shared_budget",
            "budget_attempts": target,
            "edits": 1,
            "resulting_bound": target,
            "note": "existing best-practice request-scoped budget",
        })
    suggestions.sort(key=lambda s: (s["edits"], s["resulting_bound"]))
    return suggestions
