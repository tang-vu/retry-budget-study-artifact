"""Offline differential check against real urllib3 and tenacity state machines.

No network, sleeps, transport invocations, or simulator calls. This is an
implementation-level check, not independently sampled field validation.
"""
from pathlib import Path
import csv
import argparse
import hashlib
import importlib.metadata
import itertools
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from retry_budget_study import analyzer, ir
from urllib3.util.retry import Retry
from urllib3.exceptions import NewConnectionError, ProtocolError, MaxRetryError
from urllib3.response import HTTPResponse
from tenacity import Retrying, retry_if_exception_type, retry_if_result, stop_after_attempt, wait_none


def actual(case):
    count = 0
    kind, method, total, pertype, raise_status, outer, on_exception, on_result = case
    kwargs = {'total': total, 'allowed_methods': Retry.DEFAULT_ALLOWED_METHODS,
              'status_forcelist': [503], 'raise_on_status': raise_status,
              'respect_retry_after_header': True}
    kwargs[{'conn_err': 'connect', 'read_err': 'read', 'other_err': 'other', 'status': 'status'}[kind]] = pertype

    def inner():
        nonlocal count
        state = Retry(**kwargs)
        for _ in range(30):
            count += 1
            if kind == 'status':
                response = HTTPResponse(status=503, body=b'')
                if not state.is_retry(method, 503, has_retry_after=False):
                    return response
                try:
                    state = state.increment(method, '/offline', response=response)
                except MaxRetryError:
                    if raise_status:
                        raise
                    return response
            else:
                error = (NewConnectionError(None, 'offline') if kind == 'conn_err' else
                         ProtocolError('offline') if kind == 'read_err' else RuntimeError('offline'))
                state = state.increment(method, '/offline', error=error)
        raise AssertionError('Bounded state machine failed to terminate')

    predicate = retry_if_result(lambda r: on_result and r.status == 503)
    if on_exception:
        predicate = predicate | retry_if_exception_type(Exception)
    try:
        result = Retrying(stop=stop_after_attempt(outer), wait=wait_none(),
                          sleep=lambda _: None, retry=predicate, reraise=True)(inner)
        signal = 'response'
    except Exception:
        signal = 'exception'
    return count, signal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=ROOT/'derived/library-state-validation-20260925')
    dest = parser.parse_args().out
    dest.mkdir(parents=True, exist_ok=True)
    if (dest / 'comparison.csv').exists():
        raise SystemExit('Existing evidence: choose a fresh output directory for reruns')
    cases = list(itertools.product(('conn_err', 'read_err', 'other_err', 'status'),
        ('GET', 'POST'), (0, 1, 3, None), (0, 1, 2), (False, True),
        (1, 3), (False, True), (False, True)))
    hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in (Path(__file__), ROOT/'src/retry_budget_study/analyzer.py', ROOT/'src/retry_budget_study/ir.py')}
    frozen = {'design': 'Full Cartesian matrix; persistent outcomes, finite per-type limits; attempt bounds only.',
              'cases': cases, 'source_sha256': hashes,
              'versions': {n: importlib.metadata.version(n) for n in ('urllib3', 'tenacity')},
              'transport_attempts': 0, 'independent_field_sample': False}
    (dest/'freeze.json').write_text(json.dumps(frozen, indent=2), encoding='utf-8')
    rows = []
    for i, case in enumerate(cases):
        kind, method, total, pertype, raises, outer, exc, result = case
        kwargs = {dict(conn_err='connect', read_err='read', other_err='other', status='status')[kind]: pertype}
        policy = ir.Policy(name=f'library-state-{i:04}', method=method, layers=(
            ir.tenacity_layer(stop_attempts=outer, on_exception=exc, on_statuses=(503,) if result else ()),
            ir.u3_layer(total=total, status_forcelist=(503,), raise_on_status=raises, **kwargs)))
        prediction = analyzer.analyze(policy, ir.Outcome(kind, status=503 if kind=='status' else 0),
                                      analyzer.Contract(max_transport_attempts=100))
        attempts, signal = actual(case)
        row = dict(case_id=policy.name, kind=kind, method=method, total=total, per_type=pertype,
                   raise_on_status=raises, outer_attempts=outer, on_exception=exc, on_result=result,
                   predicted_min=prediction.attempts_min, predicted_max=prediction.attempts_max,
                   observed=attempts, terminal_signal=signal,
                   within_bound=prediction.attempts_min <= attempts <= prediction.attempts_max)
        rows.append(row)
    with (dest/'comparison.csv').open('w', newline='', encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    summary = {'cases':len(rows),'within_bound':sum(r['within_bound'] for r in rows),
               'mismatches':[r for r in rows if not r['within_bound']], 'transport_attempts':0,
               'limitations':'Real library state transitions, with authored orchestration and persistent outcomes. No network, deadlines, waits, effects, transient sequences or field-sample validation.'}
    (dest/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps(summary,indent=2))
    return 0 if not summary['mismatches'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
