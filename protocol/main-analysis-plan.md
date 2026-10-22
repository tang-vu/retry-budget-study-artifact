# Prospective controlled execution, revision 1

Registered before new traffic on 22 September 2026. This is a bounded synthetic
controlled extension, not independently sampled natural workloads, blinded
hold-out data or a claim of Q2 readiness. The families and policy mechanisms
are informed by the pilot. Development smoke results are excluded.

Matrix: `config/main_matrix.json`, generated deterministically by
`scripts/prepare_main_matrix.py` (no random seed is used). Six scenario families
cross three policy arms, two execution repetitions: 18 cells, 36 operations.
The repetitions describe execution variability, not 36 independent workloads.
Methods are POST with explicit allowed_methods=None; effects mutate a separate
per-operation counter. The POST/connect condition also tests method semantics.

Scenarios: recovery after two/four status failures, mixed drop/status recovery,
Retry-After recovery, delayed effects after read timeout, and connect refusal.
Arms: single retry layer, two-layer composition, the same composition with a
shared four-attempt budget and 1.5-second deadline. Uniform declared attempt
contract is four; effects at most one. These are not equal-cost arms: cost is
an outcome reported alongside recovery, not suppressed from comparison.

Outcomes: observed physical transport entries, server receipts, completed
counter mutations, operation success, and late physical entries against the
recorded absolute budget deadline. Preserve every failed/incomplete operation.
Drain is bounded at five seconds. Incomplete drain or receipt/counter mismatch
makes exact effects unresolved. Persisted quota charges can exceed actual sends
after a crash; charges are never interpreted as successful receipts.

Report per-family paired tables and descriptive differences only. No population
confidence intervals or significance tests on six constructed families. Quota
is durably reserved before measurement, then charged in memory at each transport
entry; unused reservation is settled only after execution ends. A crash retains
the full reservation. Timing remains instrumented and is not an application
latency benchmark. Effects are loopback
counter mutations, not external business transactions. The state counter is
separate from receipt bookkeeping but shares the scenario executor.

Resource reservation: at most 156 transport entries for the full matrix and at
most six for the separate development smoke; existing recorded usage is 2258
of 3000. The durable cumulative gate runs before each transport entry, including
connect failures. One worker and the existing exclusive study lock apply.

Freeze code and matrix in Git before the smoke. If smoke exposes a harness
failure, amend the implementation before freezing the final confirmation commit.
Keep all failed smoke evidence and quota charges; do not tune confirmation
cases using their observed outcomes.

Pre-confirmation amendment: the development smoke used four attempts and
completed successfully, but per-attempt fsync produced 4.87 seconds of observer
overhead in its operation. Replace per-attempt persistence with pre-run durable
reservation before confirmation; no confirmation outcome has been inspected.
Recorded cumulative usage is now 2262; reserve at most 156 for confirmation.

Post-execution amendment: v1 completed 36 operations with 124 transport entries
(cumulative 2386). Eight operations retained one handler beyond the five-second
drain despite recorded responses; exact effects remain UNRESOLVED for those
operations. Bound server idle/incomplete-request socket reads at two seconds,
without interrupting scenario delays or counter mutations. Rerun the fixed
matrix as a **measurement replication after inspected outcomes**, not a new
untouched confirmation set. Keep both runs and report the change. No scenario,
policy or outcome-selection change is permitted. Reserve at most 156 more
attempts, within the unchanged 3000 ceiling.
