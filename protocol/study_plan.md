# Study plan — When Retry Limits Compose (exploratory pilot)

## Title

When Retry Limits Compose: Verifying End-to-End Attempt and Deadline
Budgets in Layered Clients.

## Context

Layered HTTP clients are common: an application-level retry wrapper
(e.g. `tenacity`) sits above a retrying transport (`urllib3.Retry` inside
`requests`). Each layer owns an attempt limit, a wait policy, and failure
eligibility rules — but the *end-to-end* budget the caller intended is a
property of the composition, not of any single layer. Known phenomena
(retry multiplication, per-request budgets, "retry at one layer") are NOT
claimed as contributions here; the study asks whether composed behavior
can be *predicted, bounded, and repaired* from configuration.

## Research questions

- **RQ1 (behavior).** For a closed set of real, pinned client stacks
  (requests/urllib3, tenacity; httpx surveyed), what attempt counts,
  server receipts, completed effects, and wall-time do composed
  configurations produce under deterministic loopback failures?
- **RQ2 (predictability).** Can a small static analyzer over a policy IR
  produce sound conditional bounds (max attempts, max elapsed) for these
  stacks under explicit assumptions — verified first by exhaustive
  state-machine exploration, then by live replay?
- **RQ3 (repair).** Do analyzer-guided minimal configuration changes and
  the existing best-practice shared budget keep a declared contract
  (attempt + deadline bounds), and at what success/latency trade-off?

## Scope

- Stacks: `requests`+`urllib3` (primary), `tenacity` wrapper, httpx
  (semantics survey only). Versions pinned in
  `manifests/tool_manifest.json`.
- Sequential composition of layers with per-invocation state only.
  Anything not modelable in the IR is reported as UNKNOWN, never guessed.
- Failure model: deterministic loopback scenarios — connect refusal
  (dead port), retriable status, delayed response past read timeout,
  response loss after effect, Retry-After, transient-then-success.
- ~24 policy/failure cells + known-correct controls + mutants. ≤3000
  physical transport attempts total, replay included. One worker.

## Claims discipline

- No claim that retry multiplication, per-request budgets, or
  single-layer retry are new. Value to demonstrate: analysis coverage,
  warning accuracy (no missed breaches, few unrealized risks), and
  repair quality.
- Simulation (virtual clock) reported separately from live execution.
- "Server did not receive request" is never inferred from client
  timeout; server receipts are ground truth for receipt, client-side
  instrumentation is ground truth for connect-phase attempts.
- Differing retry counts are not called bugs when the contract allows.

## Outputs

Runnable analyzer/server/adapters/tests; raw evidence (immutable, hashed
into manifests); derived tables; `reports/pilot.md`,
`claim_ledger.csv`, `human_review.md`, `go_no_go.md`, `journal_fit.md`;
English exploratory `paper/`. GO requires differentiated analysis +
credible verification + repair value; multiplication-only ⇒ REVISE.
