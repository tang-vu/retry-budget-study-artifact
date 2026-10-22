# When Retry Limits Compose: Replication Artifact

Code and synthetic observations supporting *When Retry Limits Compose:
Signal-Aware Analysis and Measurement of Layered HTTP Clients*.

Tang Minh Vu, Independent Researcher, Ninh Binh, Viet Nam.
Contact: tangminhvu2212@gmail.com. ORCID: https://orcid.org/0009-0009-4685-4427.

Version 1.0.0: [release and downloadable archive](https://github.com/tang-vu/retry-budget-study-artifact/releases/tag/v1.0.0).
The companion manuscript is distributed separately. This repository does not
indicate journal acceptance or independent human verification.

## Reproduce without study traffic

The artifact was checked with Python 3.14.8 on Windows. Install the exact
versions in `requirements-emse-lock.txt` in an isolated environment:

```text
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements-emse-lock.txt
.venv\Scripts\python scripts/validate_artifact.py --out validation-output
```

On Linux/macOS use `.venv/bin/python`. These platforms have not been tested
for this release. Choose a fresh output directory. Dependency installation
requires network access; the validation and analysis send no study requests.

The validator checks the file inventory and 31 frozen sources, reproduces
the manuscript's boundary tables, runs 60 pure tests, and reruns the
1,536-case library-state comparison, 576-case application-boundary comparison
and 256-case nested-controller diagnostic. It checks the newly generated
results and verifies that the historical quota counter is unchanged.

Individual commands are also available:

```text
.venv\Scripts\python scripts/reproduce_emse.py --out reproduced
.venv\Scripts\python scripts/verify.py --report verification.json
.venv\Scripts\python scripts/validate_library_states.py --out state-recheck
.venv\Scripts\python scripts/validate_status_boundary.py --out boundary-recheck
.venv\Scripts\python scripts/validate_nested_signals.py --out nested-recheck
```

## Evidence and expected results

The status-boundary extension and matched-cap flat comparator cover 80
operations and 252 transport entries. Checked, shared-cap and flat-cap arms
use 70, 52 and 56 entries and succeed in 10, 8 and 8 operations out of 16.
Tighter caps can prevent recovery. Agreement with conditional attempt bounds
is restricted to the authored configuration domains.

- `src/`, `tests/`, `scripts/`: analysis, adapters and reproduction checks.
- `config/`: recorded configurations and execution restrictions.
- `raw/`: retained synthetic observations, including earlier measurement limitations.
- `retained-derived/`: comparison inputs and retained derived results.
- `protocol/`: contemporaneous study plans and amendments.
- `manifests/`: version records, freezes, evidence hashes and historical quota state.
- `frozen-sources/`: exact source snapshots associated with the recorded execution hashes.
- `reports/`: machine-check results and counterexamples. Earlier reports retain their original scope and dates.
- `SHA256SUMS.json`: hashes of the release files, excluding this inventory itself.

The original execution workspace was private. Administrative restrictions in
its recorded configuration describe that environment; they are retained as
historical evidence. The public artifact is a separate curated snapshot.
`.gitattributes` preserves file bytes across Git checkouts.

## Scope and limitations

Failures and numeric configurations are authored. Measurements use one host.
Airflow documentation motivates an integration ordering; Airflow is not run.
The matched flat comparator and nested-controller diagnostic were designed
after inspecting earlier results. They are not independent field samples.
Five boundary cells exceed the generic elapsed-cost estimate, and historical
delayed-effect uncertainty remains unresolved. No universal repair superiority,
production prevalence or general soundness claim is made.

Live replay is unnecessary for reproduction. It is distinct from the offline
checks above and is constrained by the recorded quota and loopback gates.
Do not reset historical counters or run unaccounted requests.

See `CITATION.cff` for citation metadata and `THIRD_PARTY_NOTICES.md` for rights
and dependency information. No archival DOI has been assigned.
