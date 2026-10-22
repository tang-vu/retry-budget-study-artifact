"""Offline comparator using actual Retry, Retrying and Response.raise_for_status.

No socket connections, sleeps or simulator calls. Design is authored and chosen
with knowledge of the model; this is not a field sample or timing experiment.
"""
from pathlib import Path
import argparse
import csv
import hashlib
import importlib.metadata
import itertools
import json
import sys
from unittest.mock import patch
import requests
from urllib3.util.retry import Retry
from urllib3.response import HTTPResponse
from urllib3.exceptions import MaxRetryError
from tenacity import Retrying, retry_if_result, retry_if_exception_type, stop_after_attempt, wait_none

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from retry_budget_study import analyzer, ir


def actual(case):
    method, total, raises, check, outer, exc, result_retry, status = case
    calls = 0
    def inner():
        nonlocal calls
        state = Retry(total=total, status_forcelist=[503], raise_on_status=raises)
        while True:
            calls += 1
            response = requests.Response()
            response.status_code = status
            if not state.is_retry(method, status, has_retry_after=False):
                break
            try:
                state = state.increment(method, '/offline', response=HTTPResponse(status=status))
            except MaxRetryError:
                if raises:
                    raise
                break
        if check:
            response.raise_for_status()
        return response
    predicate = retry_if_result(lambda r: result_retry and r.status_code == 503)
    if exc:
        predicate = predicate | retry_if_exception_type(Exception)
    try:
        response = Retrying(stop=stop_after_attempt(outer), wait=wait_none(),
                            sleep=lambda _: None, retry=predicate, reraise=True)(inner)
        signal = f'response-{response.status_code}'
    except Exception as error:
        signal = type(error).__name__
    return calls, signal


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--out', type=Path, required=True)
    dest = ap.parse_args().out
    dest.mkdir(parents=True, exist_ok=False)
    cases = list(itertools.product(('GET', 'POST'), (0, 1, 3), (False, True),
                                  (False, True), (1, 3), (False, True),
                                  (False, True), (200, 404, 503)))
    source_files = [Path(__file__), ROOT/'src/retry_budget_study/analyzer.py',
                    ROOT/'src/retry_budget_study/ir.py']
    freeze = {'design': 'full Cartesian matrix; persistent HTTP status only',
              'cases': cases, 'transport_attempts': 0, 'independent_field_sample': False,
              'source_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                for p in source_files},
              'versions': {n: importlib.metadata.version(n) for n in ('requests','urllib3','tenacity')},
              'python': sys.version}
    (dest/'freeze.json').write_text(json.dumps(freeze, indent=2), encoding='utf-8')
    rows = []
    with patch('socket.socket.connect', side_effect=AssertionError('offline comparator forbids network')):
        for number, case in enumerate(cases):
            method, total, raises, check, outer, exc, result_retry, status = case
            layers = [ir.tenacity_layer(stop_attempts=outer, wait_s=0, on_exception=exc,
                                       on_statuses=(503,) if result_retry else ())]
            if check:
                layers.append(ir.Layer('status_check', 'status_check', 1))
            layers.append(ir.u3_layer(total=total, status_forcelist=(503,), raise_on_status=raises))
            policy = ir.Policy(tuple(layers), method=method, name=f'boundary-{number:04}')
            outcome = ir.Outcome('success') if status == 200 else ir.Outcome('status', status=status)
            prediction = analyzer.analyze(policy, outcome, analyzer.Contract(max_transport_attempts=100))
            observed, signal = actual(case)
            rows.append(dict(case_id=policy.name, method=method, total=total,
                             raise_on_status=raises, check_response=check, outer_attempts=outer,
                             on_exception=exc, on_result=result_retry, status=status,
                             predicted_min=prediction.attempts_min, predicted_max=prediction.attempts_max,
                             observed=observed, terminal_signal=signal,
                             within_bound=prediction.attempts_min <= observed <= prediction.attempts_max))
    with (dest/'comparison.csv').open('w', newline='', encoding='utf-8') as f:
        writer=csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    summary = {'cases': len(rows), 'within_bound': sum(r['within_bound'] for r in rows),
               'mismatches': [r for r in rows if not r['within_bound']], 'transport_attempts': 0}
    (dest/'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(summary, indent=2))
    return 0 if not summary['mismatches'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
