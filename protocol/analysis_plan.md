# Analysis plan (pre-registered)

## Units

- Observation unit = one logical operation (op). Reps per cell are NOT
  independent evidence for population claims; they are stability checks.
- Reporting unit = policy family × failure family cell, and root-cause
  groups (composition_product, soft_deadline, method_gating,
  retry_after_dominated, connect_phase, response_loss).
- No cloud-cost or real-world damage inference from loopback numbers.

## Procedures

1. `doctor` preflight: env, pinned versions, repo privacy, caps, lock.
2. Analyzer pass over every matrix cell: emit IR, bounds, verdict,
   warnings, suggested repair (minimal edit toward contract).
3. Simulator pass: (a) per-cell scenario replay on the executable model;
   (b) exhaustive bounded exploration (outcome sequences over
   {conn_err, read_err, status_503, success}, truncated at termination,
   depth ≤ 9) to check analyzer bounds are not under-approximated and to
   measure tightness where the bound is attained.
4. Live replay: each cell × reps against the fault server; events +
   server ledger + wall timing persisted to `raw/<run_id>/`.
5. Derived tables via `analysis.py` only:
   - `cell_summary.csv` — per cell×rep: predicted bound, sim max,
     live attempts/receipts/effects, wall s, breaches by rule.
   - `rq1_bounds.csv` — analyzer vs sim vs live attempts per cell
     (sim and live reported separately, never merged).
   - `rq2_soundness.csv` — per cell: verdict, observed breach,
     risk_not_realized, soundness violation flags.
   - `rq3_repairs.csv` — native vs single-layer vs shared-budget vs
     analyzer-repaired: attempts, success rate, wall p50/p95, edits made.
   - `metrics.json` — totals incl. physical attempt count (must be ≤3000),
     calibration/eval split, uncertainty notes.
6. Report synthesis (human-readable; numbers only from derived/).

## Calibration/evaluation split

- `split=cal` cells (baseline families) estimate the attempt-cost model
  constants (connect-fail cost, immediate-status cost, per-attempt
  overhead ε) used by elapsed bounds. They are excluded from RQ metrics.
- `split=eval` cells are analyzed with the frozen cost model.
- `split=control` known-correct cells must show exactly predicted
  behavior (1 attempt for p_none, receipts==attempts for served paths).
- `split=mutant` cells feed mis-stated configs to the analyzer; expected
  output is a warning/UNKNOWN — they validate warnings, not findings.

## Decision rules

- GO (for pilot→main-study): analyzer covers ≥90% of eval cells without
  UNKNOWN; zero soundness violations; shared-budget and repaired configs
  hold the contract where predicted; repair suggestions are minimal
  (≤2 changed parameters) in the majority of repaired cells.
- REVISE: if the only demonstrated fact is that layered retries
  multiply requests, or soundness violations cannot be explained.
- Anything unresolved → listed in reports/pilot.md + human_review.md.
