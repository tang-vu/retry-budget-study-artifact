"""Pilot runner: analyzer -> simulator -> live replay per matrix cell.

Resource discipline:
- one worker, sequential cells, sequential reps
- global physical-attempt cap (default 3000, from permissions.yaml)
- shared-resource lock at manifests/.pilot.lock (O_EXCL, pid-stamped)
- all traffic targets loopback servers authored in this repo
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import sys
import time
from dataclasses import asdict

from . import __version__, analyzer, simulator
from .adapters import execute_op
from .fault_server import DeadPort, FaultServer
from .ir import (
    BackoffSpec,
    Layer,
    Outcome,
    Policy,
    RetryPredicate,
    TimeoutSpec,
    U3_IDEMPOTENT,
)
from .ledger import ServerLedger, new_op_id
from .quota import PhysicalAttemptQuota, PhysicalQuotaExhausted

MAX_ATTEMPTS_DEFAULT = 3000


# ---------------------------------------------------------------- spec -> IR
def layer_from_spec(spec: dict, timeouts: dict | None = None) -> Layer:
    kind = spec["kind"]
    if kind == "status_check":
        return Layer(name="status_check", kind="status_check", max_attempts=1)
    if kind == "urllib3":
        am = spec.get("allowed_methods", "default")
        methods = U3_IDEMPOTENT if am == "default" else am
        counters = (
            spec.get("connect"), spec.get("read"),
            spec.get("status"), spec.get("other"))
        total = spec.get("total", 3)
        bf = spec.get("backoff_factor", 0.0)
        t = timeouts or {}
        return Layer(
            name="urllib3", kind="urllib3",
            max_attempts=None if total is None else total + 1,
            predicate=RetryPredicate(
                on_connect=True if counters[0] is None else counters[0] > 0,
                on_read=True if counters[1] is None else counters[1] > 0,
                on_other=True if counters[3] is None else counters[3] > 0,
                on_statuses=frozenset(spec.get("status_forcelist", [])),
                methods=methods,
            ),
            backoff=BackoffSpec(
                kind="urllib3_exp" if bf else "none",
                base_s=bf, max_s=120.0 if bf else None),
            timeout=TimeoutSpec(
                connect_s=spec.get("connect_timeout_s",
                                   t.get("connect_timeout_s")),
                read_s=spec.get("read_timeout_s",
                                t.get("read_timeout_s"))),
            respect_retry_after=spec.get("respect_retry_after", True),
            retry_after_max_s=21600.0,
            raise_on_status=spec.get("raise_on_status", True),
            type_counters=counters,
        )
    if kind == "tenacity":
        return Layer(
            name="tenacity", kind="tenacity",
            max_attempts=spec.get("stop_attempts"),
            predicate=RetryPredicate(
                on_connect=spec.get("on_exception", True),
                on_read=spec.get("on_exception", True),
                on_other=spec.get("on_exception", True),
                on_statuses=frozenset(spec.get("on_statuses", [])),
                methods=None,
                unknown=spec.get("unknown_predicate", False)),
            backoff=BackoffSpec(
                kind={"fixed": "fixed", "exponential": "tenacity_exp",
                      "none": "none"}.get(spec.get("wait_kind", "fixed")),
                base_s=spec.get("wait_s", 0.0),
                max_s=spec.get("wait_max_s")),
            soft_deadline_s=spec.get("stop_delay_s"),
        )
    if kind == "budget":
        return Layer(
            name="budget", kind="budget",
            max_attempts=spec.get("max_attempts"),
            budget_attempts=spec.get("max_attempts"),
            deadline_s=spec.get("deadline_s"))
    if kind == "httpx":
        return Layer(name="httpx", kind="httpx",
                     max_attempts=1 + spec.get("retries", 0),
                     predicate=RetryPredicate(
                         on_connect=True, on_read=False, on_other=False),
                     # httpcore exponential_backoff(RETRIES_BACKOFF_FACTOR
                     # =0.5): inter-retry sleeps 0, 0.5, 1, 2...
                     backoff=BackoffSpec(kind="httpcore_exp", base_s=0.5))
    if kind == "custom":
        return Layer(name="custom", kind="custom", max_attempts=None,
                     predicate=RetryPredicate(unknown=True))
    raise ValueError(f"unknown layer spec kind {kind}")


def policy_from_spec(name: str, spec: dict, method: str,
                     timeouts: dict | None = None) -> Policy:
    layers = tuple(layer_from_spec(l, timeouts) for l in spec["layers"])
    return Policy(layers=layers, method=method, name=name)


def outcomes_from_spec(spec: dict) -> list:
    return [Outcome(
        o["kind"],
        status=o.get("status", 0),
        retry_after_s=o.get("retry_after_s"),
        delay_s=o.get("delay_s", 0.0)) for o in spec["outcomes"]]


def _outcome_for_action(action: dict, read_timeout_s) -> Outcome | None:
    """Client-observed Outcome for one server-side scenario action."""
    if action.get("drop"):
        return Outcome("read_err", delay_s=action.get("delay_s", 0.0))
    delay = action.get("delay_s", 0.0)
    inner = action.get("then", action)
    if delay and read_timeout_s is not None and delay > read_timeout_s:
        return Outcome("read_err", delay_s=delay)
    status = inner.get("respond")
    if status is None:
        return None
    if 200 <= status < 300:
        return Outcome("success")
    ra = inner.get("headers", {}).get("Retry-After")
    return Outcome("status", status=status,
                   retry_after_s=float(ra) if ra is not None else None)


def _same_outcome(want: Outcome, got: dict) -> bool:
    return (want.kind == got.get("kind")
            and want.status == got.get("status", 0)
            and want.retry_after_s == got.get("retry_after_s")
            and want.delay_s == got.get("delay_s", 0.0))


def scenario_consistency_errors(failure: dict, read_timeout_s) -> list:
    """Check the dual failure-scenario encoding stays consistent.

    The live server replays scenario["plan"] then applies
    scenario["default"] to every further receipt; the simulator replays
    failure["outcomes"] with outcomes[-1] as the persistent tail. The
    tail must therefore equal the default's client-observed outcome —
    verify it instead of trusting the convention silently.
    Returns human-readable mismatches ([] = consistent).
    """
    errs = []
    outs = failure.get("outcomes", [])
    if failure.get("scenario_type") == "dead_port":
        if not outs or outs[0].get("kind") != "conn_err":
            errs.append("dead_port scenario must map to conn_err outcome")
        return errs
    sc = failure.get("scenario", {})
    plan = sc.get("plan", [])
    default = sc.get("default", {"respond": 200, "body": "ok"})
    if len(outs) != len(plan) and len(outs) != len(plan) + 1:
        errs.append(
            f"len(outcomes)={len(outs)} != len(plan)={len(plan)}(+1)")
    for i, act in enumerate(plan[:len(outs)]):
        want = _outcome_for_action(act, read_timeout_s)
        if want and not _same_outcome(want, outs[i]):
            errs.append(
                f"plan[{i}] -> {want.kind}/{want.status} but "
                f"outcomes[{i}]={outs[i].get('kind')}/{outs[i].get('status')}")
    tail = _outcome_for_action(default, read_timeout_s)
    if tail and outs and not _same_outcome(tail, outs[-1]):
        errs.append(
            f"scenario.default -> {tail.kind}/{tail.status} but "
            f"outcomes[-1]={outs[-1].get('kind')}/{outs[-1].get('status')}")
    return errs


# ---------------------------------------------------------------- live exec
def run_live(cell: dict, policy: Policy, failure: dict,
             cell_dir: str, rep: int, timeouts: dict, quota=None) -> dict:
    """One live rep: real stack vs fault server (or dead port)."""
    srv_ledger = ServerLedger()
    dead_port = failure.get("scenario_type") == "dead_port"
    httpx_only = policy.layers[-1].kind == "httpx"

    if dead_port:
        base_url = DeadPort().url
        results = _exec_reps(policy, base_url, cell, rep, httpx_only,
                             timeouts, quota)
        return {"ops": results, "server_ledger": srv_ledger.to_dict(),
                "events": results[0].pop("_events", None), "effect_state": {}}

    with FaultServer(failure["scenario"], srv_ledger) as srv:
        results = _exec_reps(policy, srv.url, cell, rep, httpx_only,
                             timeouts, quota)
        events = results[0].pop("_events", None)
    return {"ops": results, "server_ledger": srv_ledger.to_dict(),
            "effect_state": srv.effect_state.snapshot(),
            "events": events, "server_completion": {
                "complete": srv.drain_complete,
                "pending_handlers": srv.completion.pending,
                "drain_timeout_s": srv.drain_timeout_s}}


def _exec_reps(policy, base_url, cell, rep, httpx_only, timeouts, quota=None):
    op_id = new_op_id(f"{cell['id']}-r{rep}")
    if httpx_only:
        res = _run_httpx(policy, base_url, cell["id"], op_id, timeouts, quota)
        return [{"op": asdict(res), "op_id": op_id}]
    res, led = execute_op(policy, base_url, cell["id"], op_id,
                          body=b"x" if policy.method == "POST" else None, quota=quota)
    rec = {"op": asdict(res), "op_id": op_id, "_events": led.to_json()}
    return [rec]


def _run_httpx(policy: Policy, base_url: str, cell_id: str, op_id: str,
               timeouts: dict, quota=None):
    """Survey adapter: httpx transport retries (connect-phase only)."""
    import httpx

    from .adapters import OpResult

    retries = policy.layers[-1].max_attempts - 1
    client = httpx.Client(
        transport=httpx.HTTPTransport(retries=retries),
        timeout=httpx.Timeout(
            timeouts["connect_timeout_s"], read=timeouts["read_timeout_s"]))
    # client-side attempt counting: httpcore's "trace" request extension
    # emits "connection.connect_tcp.started" once per physical connect
    # attempt (httpcore/_trace.py, _sync/connection.py). Server receipts
    # remain the independent ground truth for requests that arrived.
    n_connect = [0]

    def _count_connects(name, _info):
        if name == "connection.connect_tcp.started":
            if quota is not None:
                quota.consume()
            n_connect[0] += 1

    t0 = time.monotonic()
    outcome, status, err = "exception", None, None
    try:
        r = client.get(base_url + "/op",
                       headers={"X-Op-Id": op_id, "X-Cell-Id": cell_id},
                       extensions={"trace": _count_connects})
        status = r.status_code
        outcome = "success" if status < 400 else "http_error"
    except httpx.HTTPError as e:
        err = type(e).__name__
    except PhysicalQuotaExhausted as e:
        outcome, err = "quota_exhausted", str(e)
    finally:
        client.close()
    wall = time.monotonic() - t0
    # -1 stays as NOT_MEASURED only if the trace extension never fired
    # (e.g. an httpcore without trace support) — never silently guessed.
    attempts = n_connect[0] if n_connect[0] > 0 else -1
    return OpResult(op_id=op_id, outcome=outcome, status=status,
                    transport_attempts=attempts, wrapper_invocations=1,
                    wall_s=wall, error=err)


# ------------------------------------------------------------------ driver
def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _acquire_lock(path: str) -> int:
    """Shared-resource lock: exclusive create with pid + timestamp."""
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.write(fd, json.dumps({
        "pid": os.getpid(), "t": time.time(),
        "host": platform.node()}).encode())
    return fd


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", default="config/pilot_matrix.json")
    ap.add_argument("--out", default=None, help="raw run dir")
    ap.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS_DEFAULT)
    ap.add_argument("--lock", default="manifests/.pilot.lock")
    ap.add_argument("--reserve-attempts", type=int, default=None)
    args = ap.parse_args(argv)

    matrix = json.load(open(args.matrix, encoding="utf-8"))
    defaults = matrix["defaults"]
    timeouts = {"connect_timeout_s": defaults["connect_timeout_s"],
                "read_timeout_s": defaults["read_timeout_s"]}

    run_id = time.strftime("run_%Y%m%d_%H%M%S")
    out_dir = args.out or os.path.join("raw", run_id)
    os.makedirs(out_dir, exist_ok=False)

    try:
        lock_fd = _acquire_lock(args.lock)
    except FileExistsError:
        print(f"FATAL: lock held at {args.lock}; another pilot may be "
              "running or a stale lock remains", file=sys.stderr)
        return 3

    # Permissions cap is cumulative, not reset for each invocation. The CLI
    # can lower it but cannot increase it. Persist each charge before I/O.
    try:
        perms = open('config/permissions.yaml', encoding='utf-8').read()
        hard_cap = int(re.search(r'max_physical_attempts:\s*(\d+)', perms).group(1))
        history = json.load(open('manifests/run_index.json', encoding='utf-8'))
        historical = sum(r['attempts_used'] for r in history)
        quota = PhysicalAttemptQuota('manifests/physical_quota.json', historical,
                                     min(args.max_attempts, hard_cap))
        quota.reserve(args.reserve_attempts if args.reserve_attempts is not None
                      else quota.cap - quota.initial)
    except BaseException:
        os.close(lock_fd)
        os.unlink(args.lock)
        raise
    attempt_budget = {"used": 0, "cap": quota.cap - quota.initial}
    manifest = {
        "run_id": run_id, "matrix_sha256": _sha256_file(args.matrix),
        "matrix_version": matrix["matrix_version"],
        "orchestrator": __version__,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "clock_info": {
            k: vars(time.get_clock_info(k))
            for k in ("monotonic", "perf_counter")
        },
        "attempt_cap": args.max_attempts,
        "cumulative_cap": quota.cap, "previous_attempts_charged": quota.initial,
        "measurement_revision": "physical-entry-completion-v1",
        "quota_persistence_in_timing": False,
        "reserved_attempts": quota.reserved_until - quota.initial,
        "started_wall": time.time(),
        "cells": [], "exit_status": "RUNNING",
    }
    t_start = time.monotonic()
    try:
        for cell in matrix["cells"]:
            cell_dir = os.path.join(out_dir, cell["id"])
            os.makedirs(cell_dir)
            failure = matrix["failures"][cell["failure"]]
            spec = matrix["policies"][cell["policy"]]
            method = cell.get("method", "GET")
            if cell.get("mutant") == "unknown_predicate":
                spec = json.loads(json.dumps(spec))
                for l in spec["layers"]:
                    if l["kind"] == "tenacity":
                        l["unknown_predicate"] = True
            if cell.get("mutant") == "unknown_layer_kind":
                spec = json.loads(json.dumps(spec))
                spec["layers"][0] = {"kind": "custom"}
            policy = policy_from_spec(cell["policy"], spec, method, timeouts)
            if cell.get("overrides", {}).get("budget_deadline_s"):
                d = cell["overrides"]["budget_deadline_s"]
                policy = Policy(
                    layers=tuple(
                        Layer(**{**l.__dict__, "deadline_s": d})
                        if l.kind == "budget" else l
                        for l in policy.layers),
                    method=method, name=policy.name)

            contract = analyzer.Contract.from_dict(
                cell.get("contract", defaults["contract"]))

            # --- analyzer pass (persistent representative outcome) ------
            outcomes = outcomes_from_spec(failure)
            sc_errs = scenario_consistency_errors(
                failure, timeouts["read_timeout_s"])
            primary = outcomes[0]
            an = analyzer.analyze(
                policy, primary, contract,
                read_timeout_s=timeouts["read_timeout_s"],
                connect_timeout_s=timeouts["connect_timeout_s"])
            analysis = an.to_dict()
            # mixed-cumulative bound for per-type-counter policies
            mixed_max = None
            for l in policy.layers:
                if l.kind == "urllib3" and l.type_counters:
                    s = sum(c for c in l.type_counters if c is not None)
                    if any(c is not None for c in l.type_counters):
                        mixed_max = 1 + s
            analysis["attempts_max_mixed_counters"] = mixed_max
            analysis["scenario_consistency"] = sc_errs or "OK"
            if sc_errs:
                analysis["warnings"] = analysis.get("warnings", []) + [
                    f"SCENARIO_INCONSISTENT: {e}" for e in sc_errs]

            # --- simulator pass ----------------------------------------
            sim = None
            explore_res = None
            httpx_only = policy.layers[-1].kind == "httpx"
            if not httpx_only and an.verdict != "UNKNOWN" and not sc_errs:
                # the live server applies scenario["default"] (or dead-port
                # refusal) to every receipt beyond the plan; by matrix
                # convention the LAST listed outcome is that persistent
                # tail, so the simulator must repeat it — not silently
                # succeed — to replay the same failure trace as live
                fn = simulator.scenario_fn(outcomes, default=outcomes[-1])
                sr = simulator.run(
                    policy, fn,
                    read_timeout_s=timeouts["read_timeout_s"],
                    connect_timeout_s=timeouts["connect_timeout_s"])
                sim = {"attempts": sr.attempts, "effects": sr.effects,
                       "elapsed_s": sr.elapsed_s, "signal": str(sr.signal)}
                alphabet = list({o.kind for o in outcomes})
                aobjs = {o.kind: o for o in outcomes}
                alpha = [aobjs[k] for k in alphabet if k != "success"]
                alpha.append(Outcome("success"))
                if len(alpha) > 1:
                    explore_res = simulator.explore(
                        policy, alpha,
                        read_timeout_s=timeouts["read_timeout_s"],
                        connect_timeout_s=timeouts["connect_timeout_s"],
                        max_depth=12)

            # --- live reps ----------------------------------------------
            reps = cell.get("reps", defaults["reps"])
            rep_results = []
            if cell["split"] != "mutant":
                for rep in range(reps):
                    if quota.used >= quota.cap:
                        rep_results.append({"skipped": "attempt cap reached"})
                        break
                    rep_dir = os.path.join(cell_dir, f"rep{rep}")
                    os.makedirs(rep_dir)
                    live = run_live(cell, policy, failure, rep_dir, rep,
                                    timeouts, quota)
                    for oprec in live["ops"]:
                        att = oprec["op"]["transport_attempts"]
                        if att > 0:
                            attempt_budget["used"] += att
                    with open(os.path.join(rep_dir, "events.jsonl"), "w",
                              encoding="utf-8") as f:
                        f.write(live["events"] or "")
                    with open(os.path.join(rep_dir, "server_ledger.json"),
                              "w", encoding="utf-8") as f:
                        json.dump(live["server_ledger"], f, indent=2)
                    with open(os.path.join(rep_dir, "ops.json"), "w",
                              encoding="utf-8") as f:
                        json.dump(live["ops"], f, indent=2)
                    with open(os.path.join(rep_dir, "completion.json"), "w",
                              encoding="utf-8") as f:
                        json.dump(live.get("server_completion", {
                            "complete": True, "reason": "dead_port_no_server"}),
                            f, indent=2)
                    with open(os.path.join(rep_dir, "effect_state.json"), "w",
                              encoding="utf-8") as f:
                        json.dump(live["effect_state"], f, indent=2)
                    rep_results.append({"rep": rep, "dir": rep_dir})

            with open(os.path.join(cell_dir, "analyzer.json"), "w",
                      encoding="utf-8") as f:
                json.dump(analysis, f, indent=2)
            if sim:
                with open(os.path.join(cell_dir, "sim.json"), "w",
                          encoding="utf-8") as f:
                    json.dump(sim, f, indent=2)
            if explore_res:
                with open(os.path.join(cell_dir, "explore.json"), "w",
                          encoding="utf-8") as f:
                    json.dump(explore_res, f, indent=2)

            manifest["cells"].append({
                "id": cell["id"], "split": cell["split"],
                "policy": cell["policy"], "failure": cell["failure"],
                "method": method, "contract": cell.get("contract",
                                                       defaults["contract"]),
                "verdict": analysis["verdict"],
                "attempts_used": attempt_budget["used"],
            })

        manifest["exit_status"] = (
            "CAP_REACHED" if quota.used >= quota.cap
            else "OK")
    except Exception as e:  # keep evidence; record failure in manifest
        manifest["exit_status"] = f"ERROR: {type(e).__name__}: {e}"
        raise
    finally:
        quota.settle()
        manifest["attempts_used"] = quota.charged
        manifest["cumulative_attempts_charged"] = quota.used
        manifest["elapsed_s"] = round(time.monotonic() - t_start, 3)
        manifest["finished_wall"] = time.time()
        with open(os.path.join(out_dir, "run_manifest.json"), "w",
                  encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        idx_path = "manifests/run_index.json"
        idx = []
        if os.path.exists(idx_path):
            idx = json.load(open(idx_path, encoding="utf-8"))
        idx.append({"run_id": run_id, "dir": out_dir,
                    "exit_status": manifest["exit_status"],
                    "attempts_used": quota.charged})
        with open(idx_path, "w", encoding="utf-8") as f:
            json.dump(idx, f, indent=2)
        os.close(lock_fd)
        os.unlink(args.lock)

    print(json.dumps({"run_id": run_id,
                      "attempts_used": attempt_budget["used"],
                      "exit": manifest["exit_status"]}))
    return 0 if manifest["exit_status"] == "OK" else 2


if __name__ == "__main__":
    sys.exit(main())
