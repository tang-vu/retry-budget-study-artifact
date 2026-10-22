"""Executable model of the same layer semantics, on a virtual clock.

Used two ways:
1. scenario replay: run the cell's deterministic outcome sequence;
2. exhaustive bounded exploration: DFS over the executed-path tree of
   outcome sequences (alphabet of outcome classes, depth-bounded),
   checking the analyzer's bound is not under-approximated and measuring
   tightness where a path attains it.

Simulation evidence is model evidence only — reported separately from
live timing evidence. The cost model is documented (assumption A3).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .analyzer import COST_CEIL_S, COST_FLOOR_S, _transport_cost
from .ir import Layer, Outcome, Policy


@dataclass
class SimCtx:
    outcome_fn: object  # (attempt_index:int) -> Outcome
    read_timeout_s: float | None = None
    connect_timeout_s: float | None = None
    attempt_index: int = 0
    elapsed: float = 0.0
    attempts: int = 0
    effects: int = 0
    budget_remaining: float | None = None
    deadline: float | None = None
    trace: list = field(default_factory=list)


@dataclass
class SimResult:
    signal: object
    attempts: int
    effects: int
    elapsed_s: float
    trace: list


def _signal(o: Outcome):
    if o.kind == "success":
        return "success"
    if o.kind == "status":
        return ("response", o.status)
    return "exception"


def _attempt(ctx: SimCtx) -> tuple:
    """Consume one transport attempt; returns (signal, Outcome)."""
    o = ctx.outcome_fn(ctx.attempt_index)
    ctx.attempt_index += 1
    ctx.attempts += 1
    lo, hi = _transport_cost(o, ctx.read_timeout_s, ctx.connect_timeout_s)
    # deterministic model cost: the ceiling (safe for elapsed bounds)
    ctx.elapsed += hi
    reached = o.kind != "conn_err"
    if reached:
        ctx.effects += 1
    ctx.trace.append({
        "attempt": ctx.attempts, "outcome": o.kind,
        "status": o.status, "elapsed_s": round(ctx.elapsed, 6),
    })
    return _signal(o), o


def _u3_retryable(layer: Layer, o: Outcome, method: str) -> bool:
    p = layer.predicate
    if o.kind in ("status", "read_err") and p.methods is not None and method.upper() not in p.methods:
        return False
    if o.kind == "conn_err":
        return p.on_connect
    if o.kind == "read_err":
        return p.on_read
    if o.kind == "other_err":
        return p.on_other
    if o.kind == "status":
        if o.status in p.on_statuses:
            return True
        return bool(
            layer.respect_retry_after
            and o.retry_after_s is not None
            and o.status in p.ra_statuses
        )
    return False


def _ctr_key(o: Outcome) -> int:
    return {"conn_err": 0, "read_err": 1, "status": 2, "other_err": 3}[o.kind]


def _sim(layers: tuple, i: int, ctx: SimCtx, method: str) -> tuple:
    """Execute one invocation of layers[i:]; returns (signal, outcome)."""
    if i >= len(layers):
        if ctx.budget_remaining is not None or ctx.deadline is not None:
            if ctx.deadline is not None and ctx.elapsed >= ctx.deadline:
                return ("terminal_exception", Outcome("budget"))
            if (ctx.budget_remaining is not None
                    and ctx.budget_remaining <= 0):
                return ("terminal_exception", Outcome("budget"))
            if ctx.budget_remaining is not None:
                ctx.budget_remaining -= 1
        return _attempt(ctx)

    layer = layers[i]
    if layer.kind == "budget":
        # scopes the budget for this call chain: applied at transport node
        prev_b, prev_d = ctx.budget_remaining, ctx.deadline
        if layer.budget_attempts is not None:
            ctx.budget_remaining = layer.budget_attempts
        if layer.deadline_s is not None:
            ctx.deadline = ctx.elapsed + layer.deadline_s
        sig = _sim(layers, i + 1, ctx, method)
        ctx.budget_remaining, ctx.deadline = prev_b, prev_d
        return sig

    if layer.kind == "status_check":
        sig, o = _sim(layers, i + 1, ctx, method)
        if isinstance(sig, tuple) and sig[0] == "response" and 400 <= sig[1] < 600:
            return ("exception", o)
        return (sig, o)

    if layer.kind == "urllib3":
        total = None if layer.max_attempts is None else layer.max_attempts - 1
        ctrs = list(layer.type_counters) if layer.type_counters else None
        retries_used = 0
        while True:
            sig, o = _sim(layers, i + 1, ctx, method)
            if sig == "success" or sig == "terminal_exception":
                return (sig, o)
            if not _u3_retryable(layer, o, method):
                return (sig, o)
            # counter check (total takes precedence)
            exhausted = False
            if total is not None:
                if total - retries_used <= 0:
                    exhausted = True
            if not exhausted and ctrs is not None:
                k = _ctr_key(o)
                if ctrs[k] is not None and ctrs[k] <= 0:
                    exhausted = True
                elif ctrs[k] is not None:
                    ctrs[k] -= 1
            if exhausted:
                if o.kind == "status" and not layer.raise_on_status:
                    return (("response", o.status), o)
                return ("exception", o)
            retries_used += 1
            ra = o.retry_after_s if layer.respect_retry_after else None
            ctx.elapsed += layer.backoff.delay_for(
                retries_used,
                retry_after_s=ra,
                ra_max_s=layer.retry_after_max_s,
            )

    if layer.kind == "tenacity":
        attempt = 0
        t0 = ctx.elapsed
        while True:
            attempt += 1
            sig, o = _sim(layers, i + 1, ctx, method)
            if sig == "success" or sig == "terminal_exception":
                return (sig, o)
            # eligibility on the inner SIGNAL
            if sig == "exception":
                eligible = (layer.predicate.on_connect
                            or layer.predicate.on_read
                            or layer.predicate.on_other)
            elif isinstance(sig, tuple) and sig[0] == "response":
                eligible = sig[1] in layer.predicate.on_statuses
            else:
                eligible = False
            if not eligible:
                return (sig, o)
            # stop checks happen between attempts (after failure)
            if layer.max_attempts is not None and attempt >= layer.max_attempts:
                return (sig, o)
            if (layer.soft_deadline_s is not None
                    and ctx.elapsed - t0 >= layer.soft_deadline_s):
                return (sig, o)
            ctx.elapsed += layer.backoff.delay_for(attempt)

    if layer.kind == "httpx":
        # survey semantics (httpcore 1.x): connect-phase retries only;
        # each failed connect sleeps exponential_backoff(0.5) — 0, 0.5,
        # 1, 2... — before the next attempt (RETRIES_BACKOFF_FACTOR).
        n_done = 0
        while True:
            n_done += 1
            sig, o = _sim(layers, i + 1, ctx, method)
            if sig == "success" or sig == "terminal_exception":
                return (sig, o)
            if o.kind != "conn_err":
                return (sig, o)
            if layer.max_attempts is not None \
                    and n_done >= layer.max_attempts:
                return ("exception", o)
            ctx.elapsed += layer.backoff.delay_for(n_done)

    raise ValueError(f"unknown layer kind {layer.kind}")


def run(policy: Policy, outcome_fn, read_timeout_s: float | None = None,
        connect_timeout_s: float | None = None) -> SimResult:
    ctx = SimCtx(outcome_fn=outcome_fn, read_timeout_s=read_timeout_s,
                 connect_timeout_s=connect_timeout_s)
    sig, _o = _sim(policy.layers, 0, ctx, policy.method)
    return SimResult(sig, ctx.attempts, ctx.effects, ctx.elapsed, ctx.trace)


def scenario_fn(outcomes: list, default: Outcome | None = None):
    """outcome_fn from an explicit sequence; index beyond -> default."""
    d = default or Outcome("success")

    def fn(k: int) -> Outcome:
        return outcomes[k] if k < len(outcomes) else d

    return fn


def explore(policy: Policy, alphabet: list, read_timeout_s: float | None = None,
            connect_timeout_s: float | None = None,
            max_depth: int = 18) -> dict:
    """DFS over the executed-path tree.

    For each node we run the policy on prefix + infinite success padding:
    if the run terminates inside the prefix the path is complete; if it
    consumes into the padding, the node is non-terminal and we branch.
    Returns {path_tuple: SimResult-lite} plus maxima.
    """
    results: dict = {}
    success = Outcome("success")

    def fn_for(seq):
        return lambda k: seq[k] if k < len(seq) else success

    def dfs(prefix: list):
        seq = list(prefix)
        res = run(policy, fn_for(seq), read_timeout_s, connect_timeout_s)
        if res.attempts <= len(prefix):
            results[tuple(seq[: res.attempts])] = res
            return
        # non-terminal at position len(prefix): branch over alphabet
        for o in alphabet:
            child = prefix + [o]
            if len(child) > max_depth:
                results[tuple(child)] = ("DEPTH_LIMIT",)
                continue
            if o.kind == "success":
                r = run(policy, fn_for(child), read_timeout_s,
                        connect_timeout_s)
                results[tuple(child)] = r
            else:
                dfs(child)

    dfs([])
    max_attempts = 0
    max_elapsed = 0.0
    for path, r in results.items():
        if isinstance(r, SimResult):
            max_attempts = max(max_attempts, r.attempts)
            max_elapsed = max(max_elapsed, r.elapsed_s)
    return {
        "paths": {",".join(str(p) for p in k): (
            {"attempts": r.attempts, "elapsed_s": r.elapsed_s,
             "effects": r.effects, "signal": str(r.signal)}
            if isinstance(r, SimResult) else {"depth_limit": True}
        ) for k, r in results.items()},
        "max_attempts": max_attempts,
        "max_elapsed_s": max_elapsed,
        "n_paths": len(results),
    }
