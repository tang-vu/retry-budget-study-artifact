"""Derived analysis: raw/<run_id> -> derived/ tables.

This module is the ONLY writer of derived/. It reconciles client-side
attempt accounting with server receipts, applies the contract oracle
(protocol/contract.md), and reports simulation and live evidence
separately — never merged.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys


def _load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _cell_dirs(run_dir):
    return sorted(
        d for d in os.listdir(run_dir)
        if os.path.isdir(os.path.join(run_dir, d)))


def _breaches(op, server_counts, contract, events_lines):
    """Oracle rules from protocol/contract.md. Returns list of breach dicts."""
    out = []
    att = op["transport_attempts"]
    if (contract.get("max_transport_attempts") is not None
            and att > contract["max_transport_attempts"]):
        out.append({"rule": "attempt_budget", "value": att,
                    "limit": contract["max_transport_attempts"]})
    me = contract.get("max_effects")
    if (me is not None and server_counts["effects"] is not None
            and server_counts["effects"] > me):
        out.append({"rule": "effect_budget", "value": server_counts["effects"],
                    "limit": me})
    if (contract.get("max_wall_s") is not None
            and op["wall_s"] > contract["max_wall_s"]):
        out.append({"rule": "wall_budget", "value": round(op["wall_s"], 4),
                    "limit": contract["max_wall_s"]})
    # deadline new-attempt check: attempt_start events after deadline
    dl = contract.get("deadline_s")
    if dl is not None and events_lines:
        physical = any(e.get("kind") == "physical_attempt_start"
                       and e.get("op_id") == op.get("op_id") for e in events_lines)
        starts = [e for e in events_lines
                  if e.get("kind") in (("physical_attempt_start",) if physical
                                        else ("attempt_start", "retry_scheduled"))
                  and e.get("op_id") == op.get("op_id")]
        if starts:
            budgets = [e for e in events_lines if e.get("kind") == "budget_created"
                       and e.get("op_id") == op.get("op_id")
                       and e.get("data", {}).get("deadline_mono") is not None]
            deadline = (budgets[0]["data"]["deadline_mono"] if budgets
                        else starts[0]["t_mono"] + dl)
            late = [e for e in starts if e["t_mono"] > deadline]
            if late:
                out.append({"rule": "deadline_new_attempt",
                            "value": len(late), "limit": dl})
    return out


def _parse_events(text):
    if not text:
        return []
    return [json.loads(l) for l in text.splitlines() if l.strip()]


# baseline comparison classes (protocol/analysis_plan.md step 5)
CONFIG_CLASS = {
    "p_none": "A_native",
    "p_u3": "B_single_layer",
    "p_ten": "B_single_layer",
    "p_budget": "C_shared_budget",
    "p_deadline": "C_shared_budget",
    "p_repaired": "D_analyzer_repaired",
    "p_u3_ten": "X_naive_composition",
    "p_u3_ten_rosf": "X_naive_composition",
    "p_delay_only": "X_soft_deadline",
    "p_u3_nora": "B_single_layer",
    "p_httpx": "S_survey",
}


def _repairs_summary(an):
    """Compact description of analyzer repair suggestions."""
    if not an or not an.get("repairs"):
        return ""
    parts = []
    for r in an["repairs"]:
        vb = r.get("verified_attempts_max")
        vc = r.get("verified_meets_contract")
        vtag = "" if vb is None else (
            f", verified={vb}" + ("" if vc else "/FAIL"))
        if r["kind"] == "cap_layer":
            parts.append(f"cap {r['layer']}->{r['set_max_attempts']}"
                         f" (edits={r['edits']}, bound={r['resulting_bound']}{vtag})")
        elif r["kind"] == "remove_outer_retry":
            parts.append(f"remove_outer_retry "
                         f"(edits={r['edits']}, bound={r['resulting_bound']}{vtag})")
        elif r["kind"] == "add_shared_budget":
            parts.append(f"add_shared_budget {r['budget_attempts']}"
                         f" (edits={r['edits']}{vtag})")
        elif r["kind"] == "cap_shared_budget":
            parts.append(f"cap_shared_budget {r['layer']}->{r['budget_attempts']}"
                         f" (edits={r['edits']}, bound={r['resulting_bound']}{vtag})")
    return " | ".join(parts)


def _repair_replay_rows(run_dir, matrix, cell_meta):
    """Replay each analyzer repair suggestion through the simulator —
    an independent executable model — on the cell's own scenario.

    The analyzer's verified_attempts_max recomputes a bound with the
    same machinery that produced the suggestion; the simulator replays
    the materialized repaired policy step by step, so agreement is
    evidence the suggestion is not an analyzer artifact. Model-level
    evidence only (virtual clock) — not live timing evidence.
    """
    from . import analyzer, runner, simulator
    defaults = matrix["defaults"]
    timeouts = {"read_timeout_s": defaults["read_timeout_s"],
                "connect_timeout_s": defaults["connect_timeout_s"]}
    rows = []
    for cid in _cell_dirs(run_dir):
        an_p = os.path.join(run_dir, cid, "analyzer.json")
        meta = cell_meta.get(cid, {})
        if not os.path.exists(an_p) or meta.get("split") == "mutant":
            continue
        an = _load(an_p)
        if not an.get("repairs"):
            continue
        failure = matrix["failures"][meta["failure"]]
        policy = runner.policy_from_spec(
            meta["policy"], matrix["policies"][meta["policy"]],
            meta.get("method", "GET"), timeouts)
        outcomes = runner.outcomes_from_spec(failure)
        fn = simulator.scenario_fn(outcomes, default=outcomes[-1])
        contract = meta.get("contract", defaults["contract"])
        cap = contract.get("max_transport_attempts")
        for r in an["repairs"]:
            row = {"cell": cid, "policy": meta.get("policy"),
                   "failure": meta.get("failure"),
                   "method": meta.get("method"),
                   "kind": r.get("kind"), "layer": r.get("layer"),
                   "layer_index": r.get("layer_index"),
                   "edits": r.get("edits"),
                   "analyzer_bound": r.get("resulting_bound"),
                   "verified_attempts_max": r.get("verified_attempts_max")}
            try:
                rp = analyzer.apply_suggestion(policy, r)
                sr = simulator.run(
                    rp, fn,
                    read_timeout_s=timeouts["read_timeout_s"],
                    connect_timeout_s=timeouts["connect_timeout_s"])
                row["sim_attempts"] = sr.attempts
                row["sim_within_attempt_contract"] = (
                    None if cap is None else bool(sr.attempts <= cap))
                row["sim_matches_bound"] = (
                    sr.attempts <= r["resulting_bound"]
                    if r.get("resulting_bound") is not None else None)
            except Exception as e:
                row["sim_attempts"] = None
                row["sim_within_attempt_contract"] = None
                row["sim_matches_bound"] = None
                row["sim_error"] = repr(e)[:120]
            rows.append(row)
    return rows


def analyze_run(run_dir: str, derived_dir: str,
                matrix_path: str | None = None) -> dict:
    manifest = _load(os.path.join(run_dir, "run_manifest.json"))
    os.makedirs(derived_dir, exist_ok=True)
    cell_meta = {c["id"]: c for c in manifest["cells"]}

    summary_rows, bounds_rows, sound_rows, rq3_rows = [], [], [], []
    metrics = {
        "run_id": manifest["run_id"],
        "attempt_cap": manifest["attempt_cap"],
        "attempts_used": manifest["attempts_used"],
        "exit_status": manifest["exit_status"],
        "cells": {}, "n_soundness_violations": 0,
        "n_risk_not_realized": 0, "n_breaches": 0,
        # predicted-bound misses are distinct from declared-budget
        # breaches: a cell can stay inside the contract while exceeding
        # the analyzer's bound (a model error), or vice versa
        "n_bound_misses": 0, "bound_miss_cells": [],
    }

    for cid in _cell_dirs(run_dir):
        cdir = os.path.join(run_dir, cid)
        meta = cell_meta.get(cid, {})
        contract = meta.get("contract", {})
        an = _load(os.path.join(cdir, "analyzer.json")) \
            if os.path.exists(os.path.join(cdir, "analyzer.json")) else None
        sim = _load(os.path.join(cdir, "sim.json")) \
            if os.path.exists(os.path.join(cdir, "sim.json")) else None
        exp = _load(os.path.join(cdir, "explore.json")) \
            if os.path.exists(os.path.join(cdir, "explore.json")) else None

        rep_rows = []
        effects_unresolved = False
        all_breaches = []
        walls, atts = [], []
        for rep in sorted(os.listdir(cdir)):
            rdir = os.path.join(cdir, rep)
            if not rep.startswith("rep") or not os.path.isdir(rdir):
                continue
            ops = _load(os.path.join(rdir, "ops.json"))
            sled = _load(os.path.join(rdir, "server_ledger.json"))
            completion_path = os.path.join(rdir, "completion.json")
            completed = (os.path.exists(completion_path)
                         and _load(completion_path).get("complete") is True)
            effect_path = os.path.join(rdir, "effect_state.json")
            effect_state = _load(effect_path) if os.path.exists(effect_path) else None
            completed = completed and effect_state is not None
            effects_unresolved |= not completed
            ev = []
            evp = os.path.join(rdir, "events.jsonl")
            if os.path.exists(evp):
                ev = _parse_events(open(evp, encoding="utf-8").read())
            for oprec in ops:
                op = oprec["op"]
                op_id = oprec["op_id"]
                scounts = {"receipts": 0, "effects": 0}
                for oid, recs in sled.items():
                    if oid == op_id:
                        scounts["receipts"] = len(recs)
                        scounts["effects"] = sum(
                            1 for r in recs if r.get("effect") == "apply")
                observed_effects = scounts["effects"]
                if effect_state is not None:
                    matched_effect_state = (effect_state.get(op_id, 0) == observed_effects)
                    if not matched_effect_state:
                        effects_unresolved = True
                        completed = False
                if not completed:
                    scounts["effects"] = None
                br = _breaches(op, scounts, contract, ev)
                all_breaches += [{**b, "rep": rep} for b in br]
                att = op["transport_attempts"]
                if att > 0:
                    atts.append(att)
                walls.append(op["wall_s"])
                rep_rows.append({
                    "cell": cid, "rep": rep, "op_id": op_id,
                    "outcome": op["outcome"], "status": op["status"],
                    "attempts": att, "receipts": scounts["receipts"],
                    "effects": scounts["effects"],
                    "effects_observed": observed_effects,
                    "effects_complete": completed,
                    "wall_s": round(op["wall_s"], 4),
                    "invocations": op["wrapper_invocations"],
                    "error": op["error"],
                    "breaches": ";".join(b["rule"] for b in br) or "none",
                })

        max_att = max(atts) if atts else 0
        verdict = an["verdict"] if an else "NOT_RUN"
        any_breach = len(all_breaches) > 0
        soundness = ""
        risk = ""
        unresolved = (not rep_rows or any(r['outcome'] == 'quota_exhausted'
                                          for r in rep_rows)
                      or (effects_unresolved and contract.get('max_effects') is not None))
        if meta.get("split") == "eval":
            if verdict == "GUARANTEED" and any_breach:
                soundness = "VIOLATION"
                metrics["n_soundness_violations"] += 1
            elif unresolved:
                soundness = "UNRESOLVED"
            elif verdict in ("POSSIBLE_BREACH", "WILL_BREACH") \
                    and not any_breach:
                risk = "risk_not_realized"
                metrics["n_risk_not_realized"] += 1
        metrics["n_breaches"] += len(all_breaches)
        metrics["cells"][cid] = {
            "split": meta.get("split"), "verdict": verdict,
            "breaches": len(all_breaches),
            "effects_complete": not effects_unresolved,
            "unresolved": unresolved,
        }

        # model verdict: the simulator's scenario-exact path vs contract —
        # reported alongside (never merged with) the persistent bound
        model_verdict = "n/a"
        if sim is not None:
            if (contract.get("max_transport_attempts") is not None
                    and sim["attempts"] > contract["max_transport_attempts"]):
                model_verdict = "breach_attempts"
            elif (contract.get("max_wall_s") is not None
                    and sim["elapsed_s"] > contract["max_wall_s"]):
                model_verdict = "breach_wall"
            else:
                model_verdict = "within"

        summary_rows.append({
            "cell": cid, "split": meta.get("split"),
            "policy": meta.get("policy"), "failure": meta.get("failure"),
            "method": meta.get("method"), "verdict": verdict,
            "model_verdict": model_verdict,
            "n_reps": len(rep_rows),
            "attempts_max": max_att,
            "wall_p50_s": round(statistics.median(walls), 4) if walls else None,
            "wall_max_s": round(max(walls), 4) if walls else None,
            "success_rate": round(
                sum(1 for r in rep_rows if r["outcome"] == "success")
                / len(rep_rows), 3) if rep_rows else None,
            "n_breaches": len(all_breaches),
            "breach_rules": ";".join(sorted({b["rule"] for b in all_breaches}))
                or "none",
            "soundness": soundness, "risk": risk,
        })
        live_wall_max = round(max(walls), 4) if walls else None
        att_ok = wall_ok = None
        if an is not None and rep_rows:
            if an["attempts_max"] is not None and atts:
                att_ok = max_att <= an["attempts_max"]
            if (an["elapsed_max_s"] is not None
                    and live_wall_max is not None):
                wall_ok = live_wall_max <= an["elapsed_max_s"]
        if att_ok is False or wall_ok is False:
            metrics["n_bound_misses"] += 1
            metrics["bound_miss_cells"].append(cid)
        bounds_rows.append({
            "cell": cid,
            "analyzer_min": an["attempts_min"] if an else None,
            "analyzer_max": an["attempts_max"] if an else None,
            "analyzer_elapsed_max_s": an["elapsed_max_s"] if an else None,
            "analyzer_mixed_counters": an.get("attempts_max_mixed_counters")
                if an else None,
            "sim_attempts": sim["attempts"] if sim else None,
            "sim_elapsed_s": sim["elapsed_s"] if sim else None,
            "explore_max_attempts": exp["max_attempts"] if exp else None,
            "live_attempts_max": max_att,
            "live_wall_max_s": live_wall_max,
            "live_within_att_bound": att_ok if an else None,
            "live_within_wall_bound": wall_ok if an else None,
        })
        sound_rows.append({
            "cell": cid, "split": meta.get("split"), "verdict": verdict,
            "any_breach": any_breach, "soundness": soundness or "ok",
            "risk": risk, "breach_rules": ";".join(
                sorted({b["rule"] for b in all_breaches})) or "none",
        })
        if meta.get("split") != "mutant":
            succ = ([r["outcome"] == "success" for r in rep_rows]
                    if rep_rows else [])
            rq3_rows.append({
                "cell": cid,
                "config_class": CONFIG_CLASS.get(meta.get("policy"), "?"),
                "policy": meta.get("policy"),
                "failure": meta.get("failure"),
                "method": meta.get("method"),
                "verdict": verdict,
                "attempts_max": max_att,
                "success_rate": round(sum(succ) / len(succ), 3)
                    if succ else None,
                "wall_p50_s": round(statistics.median(walls), 4)
                    if walls else None,
                "n_breaches": len(all_breaches),
                "analyzer_repairs": _repairs_summary(an),
            })

    def wcsv(name, rows):
        if not rows:
            return
        with open(os.path.join(derived_dir, name), "w", newline="",
                  encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    wcsv("cell_summary.csv", summary_rows)
    wcsv("rq1_bounds.csv", bounds_rows)
    wcsv("rq2_soundness.csv", sound_rows)
    wcsv("rq3_repairs.csv", rq3_rows)
    if matrix_path:
        wcsv("rq3_repair_replay.csv",
             _repair_replay_rows(run_dir, _load(matrix_path), cell_meta))
    rep_path = os.path.join(derived_dir, "reps.csv")
    with open(rep_path, "w", newline="", encoding="utf-8") as f:
        allreps = [r for cid in _cell_dirs(run_dir) for r in
                   _rep_rows(os.path.join(run_dir, cid))]
        if allreps:
            w = csv.DictWriter(f, fieldnames=list(allreps[0].keys()))
            w.writeheader()
            w.writerows(allreps)

    with open(os.path.join(derived_dir, "metrics.json"), "w",
              encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    return metrics


def _rep_rows(cdir):
    rows = []
    for rep in sorted(os.listdir(cdir)):
        rdir = os.path.join(cdir, rep)
        if not rep.startswith("rep") or not os.path.isdir(rdir):
            continue
        ops_p = os.path.join(rdir, "ops.json")
        if not os.path.exists(ops_p):
            continue
        ops = _load(ops_p)
        sled = _load(os.path.join(rdir, "server_ledger.json"))
        for oprec in ops:
            op = oprec["op"]
            recs = sled.get(oprec["op_id"], [])
            cp = os.path.join(rdir, "completion.json")
            ep = os.path.join(rdir, "effect_state.json")
            complete = (os.path.exists(cp) and _load(cp).get('complete') is True
                        and os.path.exists(ep))
            observed_effects = sum(1 for r in recs if r.get('effect') == 'apply')
            state_effects = _load(ep).get(oprec['op_id'], 0) if os.path.exists(ep) else None
            complete = complete and state_effects == observed_effects
            rows.append({
                "cell": os.path.basename(cdir), "rep": rep,
                "op_id": oprec["op_id"], "outcome": op["outcome"],
                "attempts": op["transport_attempts"],
                "receipts": len(recs),
                "effects": state_effects if complete else None,
                "effects_observed": observed_effects,
                "effects_complete": complete,
                "wall_s": round(op["wall_s"], 4),
            })
    return rows


def audit_main_measurements(run_dir, derived_dir):
    """Reconcile the separate measurement channels without sending traffic."""
    manifest = _load(os.path.join(run_dir, 'run_manifest.json'))
    meta = {c['id']: c for c in manifest['cells']}
    rows = []
    for cid in _cell_dirs(run_dir):
        for rep in sorted(os.listdir(os.path.join(run_dir, cid))):
            rd = os.path.join(run_dir, cid, rep)
            if not rep.startswith('rep') or not os.path.isdir(rd):
                continue
            completion = _load(os.path.join(rd, 'completion.json'))
            state = _load(os.path.join(rd, 'effect_state.json'))
            receipts = _load(os.path.join(rd, 'server_ledger.json'))
            events = _parse_events(open(os.path.join(rd, 'events.jsonl'), encoding='utf-8').read())
            for rec in _load(os.path.join(rd, 'ops.json')):
                op = rec['op']; oid = rec['op_id']
                physical = [e for e in events if e['op_id']==oid
                            and e['kind']=='physical_attempt_start']
                ledger_effects = sum(r.get('effect')=='apply' for r in receipts.get(oid, []))
                rows.append({'cell':cid,'family':meta[cid]['failure'],
                             'policy':meta[cid]['policy'],'rep':rep,'op_id':oid,
                             'success':op['outcome']=='success',
                             'attempts':op['transport_attempts'],
                             'physical_events':len(physical),
                             'attempts_match':len(physical)==op['transport_attempts'],
                             'receipts':len(receipts.get(oid, [])),
                             'effects_state':state.get(oid,0),
                             'effects_ledger':ledger_effects,
                             'effects_match':state.get(oid,0)==ledger_effects,
                             'completion':completion['complete'],
                             'wall_s':op['wall_s']})
    report = {'run_id':manifest['run_id'],'operations':len(rows),
              'attempts':sum(r['attempts'] for r in rows),
              'quota_charged':manifest['attempts_used'],
              'physical_counts_match':all(r['attempts_match'] for r in rows),
              'all_complete':all(r['completion'] for r in rows),
              'effect_counts_match':all(r['effects_match'] for r in rows),
              'incomplete_operations':sum(not r['completion'] for r in rows),
              'arms':{}}
    for arm in sorted({r['policy'] for r in rows}):
        rs=[r for r in rows if r['policy']==arm]
        report['arms'][arm]={'operations':len(rs),'successes':sum(r['success'] for r in rs),
                            'attempts':sum(r['attempts'] for r in rs),
                            'effects':sum(r['effects_state'] for r in rs)
                            if all(r['completion'] and r['effects_match'] for r in rs) else None}
    os.makedirs(derived_dir,exist_ok=True)
    with open(os.path.join(derived_dir,'measurement_audit.json'),'w',encoding='utf-8') as f:
        json.dump(report,f,indent=2)
    with open(os.path.join(derived_dir,'measurement_audit.csv'),'w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    return report


def write_status_boundary_comparison(main_raw, flat_raw, dest):
    """Generate manuscript tables from retained evidence, without traffic."""
    from pathlib import Path
    from collections import defaultdict
    destination = Path(dest)
    destination.mkdir(parents=True, exist_ok=False)
    rows = []
    audits = []
    for name, folder in [('main', main_raw), ('flat', flat_raw)]:
        audit = audit_main_measurements(str(folder), str(destination/name))
        if not (audit['physical_counts_match'] and audit['all_complete']
                and audit['effect_counts_match'] and audit['attempts']==audit['quota_charged']):
            raise ValueError(f'{name}: measurement channels do not reconcile')
        audits.append(audit)
        meta = {c['id']:c for c in _load(os.path.join(folder,'run_manifest.json'))['cells']}
        with (destination/name/'measurement_audit.csv').open(newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                row['method'] = meta[row['cell']]['method']
                rows.append(row)
    groups = defaultdict(list)
    for row in rows:
        groups[(row['method'],row['family'],row['policy'])].append(row)
    table = []
    for method in ('GET','POST'):
        for family in ('persistent','recover_2','recover_4','drop_status'):
            record = {'method':method,'family':family}
            for arm in ('unchecked','checked','single','shared','flat'):
                rr = groups[(method,family,arm)]
                values={(int(r['attempts']),r['success']=='True',int(r['effects_state'])) for r in rr}
                if len(rr)!=2 or len(values)!=1:
                    raise ValueError(f'Unstable/missing repeated condition: {method}/{family}/{arm}')
                attempts,success,effects=next(iter(values))
                record[arm+'_attempts']=attempts
                record[arm+'_success']=success
                record[arm+'_effects']=effects
            table.append(record)
    arms=[]
    for arm in ('unchecked','checked','single','shared','flat'):
        rr=[r for r in rows if r['policy']==arm]
        arms.append({'arm':arm,'operations':len(rr),
                     'attempts':sum(int(r['attempts']) for r in rr),
                     'successes':sum(r['success']=='True' for r in rr),
                     'effects':sum(int(r['effects_state']) for r in rr),
                     'attempt_budget_breaches':sum(int(r['attempts'])>4 for r in rr)})
    for filename, records in [('condition_comparison.csv',table),('arm_comparison.csv',arms)]:
        with (destination/filename).open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=list(records[0]));writer.writeheader();writer.writerows(records)
    markdown=['| Method / failure | Unchecked | Checked | Single | Shared cap 4 | Flat cap 4 |',
              '|---|---|---|---|---|---|']
    for row in table:
        cells=[f"{row[a+'_attempts']} / {'yes' if row[a+'_success'] else 'no'} / {row[a+'_effects']}"
               for a in ('unchecked','checked','single','shared','flat')]
        markdown.append('| '+row['method']+' / '+row['family']+' | '+' | '.join(cells)+' |')
    (destination/'condition_comparison.md').write_text('\n'.join(markdown)+'\n',encoding='utf-8')
    summary={'operations':len(rows),'condition_pairs':8,'arms':arms,
             'physical_attempts':sum(a['attempts'] for a in audits),
             'all_measurement_channels_reconcile':True,
             'all_condition_repetitions_agree':True,
             'post_inspection_arm':'flat',
             'causal_latency_or_population_claim':False}
    (destination/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--derived", default="derived")
    ap.add_argument("--matrix", default=None,
                    help="pilot matrix JSON; enables repair sim-replay table")
    args = ap.parse_args(argv)
    m = analyze_run(args.raw, args.derived, matrix_path=args.matrix)
    if os.path.exists(os.path.join(args.raw, 'run_manifest.json')) and \
            _load(os.path.join(args.raw, 'run_manifest.json')).get('measurement_revision'):
        audit_main_measurements(args.raw,args.derived)
    print(json.dumps({"derived": args.derived,
                      "attempts_used": m["attempts_used"],
                      "soundness_violations": m["n_soundness_violations"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
