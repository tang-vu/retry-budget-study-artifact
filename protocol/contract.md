# Contract & oracle — retry-budget-study

## Event vocabulary (every event has an id and a ledger)

| Term | Definition | Ground truth |
|---|---|---|
| logical operation (op) | one caller-visible unit of work | `OperationLedger.op_id` |
| wrapper invocation | one call of the inner stack by an outer retry layer | client events `invoke` |
| transport attempt | one HTTP request send at the lowest layer | client events `attempt` (urllib3 increment + initial send; socket connect counter as cross-check) |
| server receipt | request fully received (headers+body read) at fault server | `ServerLedger.receipts` |
| completed effect | receipt whose scenario marks `effect=apply` | `ServerLedger` |

Rules: a client timeout never implies absence of a receipt/effect;
connect-phase failures produce attempts with zero receipts (legitimately
"server saw nothing").

## Budget contract (per matrix cell)

```json
{
  "max_transport_attempts": 5,      // per logical op
  "max_effects": null,              // null = unbounded allowed; n = at most n completed effects
  "max_wall_s": 3.0,                // op wall time budget
  "deadline_s": null,               // if set: no NEW transport attempt may start after op_start + deadline_s
  "deadline_is_hard": false         // true => in-flight attempts may still complete past deadline
}
```

Verdicts per cell:
- `GUARANTEED` — analyzer bound ≤ contract (attempts and, when computable,
  elapsed).
- `POSSIBLE_BREACH` — analyzer bound > contract but a modeled path exists
  within contract (bound not tight, or failure-pattern dependent).
- `WILL_BREACH` — analyzer bound > contract and even the best modeled
  path exceeds it (e.g. every failure is retried and minimum attempts
  already exceed the cap).
- `UNBOUNDED` — no finite bound derivable (e.g. stop_after_delay without
  attempt cap and no positive per-cycle lower bound).
- `UNKNOWN` — configuration uses constructs outside the IR.

## Oracle rules (checked on live evidence)

1. attempt accounting: `transport_attempts = 1 + successful_retry_decisions`
   per layer invocation chain; reconciled with server receipts
   (`receipts ≤ attempts`; equality not required — connect-phase attempts
   may never reach the server).
2. attempt-budget breach: `transport_attempts > max_transport_attempts`.
3. deadline breach (new-attempt semantics): any `attempt_start` with
   `t > op_start + deadline_s`. In-flight completion past deadline is
   recorded separately (`inflight_overshoot_s`) and is NOT a breach under
   the contract — reported, never hidden.
4. effect breach: `completed_effects > max_effects` when `max_effects`
   set (non-idempotent hazard on POST).
5. wall breach: `op_wall_s > max_wall_s`.
6. false-alarm accounting: `POSSIBLE_BREACH`/`WILL_BREACH` verdict with
   zero observed breaches across reps ⇒ `risk_not_realized` (counted);
   `GUARANTEED` verdict with any observed breach ⇒ soundness bug
   (must be 0, investigated, never silently absorbed).
7. mutants (deliberately mis-stated policies fed to the analyzer) live in
   the matrix with `split=mutant` and are never counted in findings.

## Assumptions the analyzer must declare

- A1 per-invocation state: each layer's counters reset per invocation
  (true for urllib3 `Retry` cloning and tenacity per-call state).
- A2 outcome persistence: within a cell, every transport attempt yields
  the scenario's outcome class (or the documented k-failures-then-success
  sequence); heterogeneous mixes are handled by the simulator's
  exhaustive exploration, not by closed-form bounds.
- A3 attempt-cost model: per-outcome latency ranges used for
  elapsed/deadline bounds are assumptions (loopback-measured constants +
  configured timeouts), recorded per run.
- A4 scheduling: Windows timer granularity (~15.6 ms) adds error to
  sub-100 ms waits; recorded as measurement uncertainty.
