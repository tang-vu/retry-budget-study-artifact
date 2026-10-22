"""Run pure checks by default; --live charges harness traffic to study quota."""
from pathlib import Path
import argparse
import hashlib
import io
import json
import os
import platform
import sys
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from retry_budget_study import adapters
from retry_budget_study.quota import PhysicalAttemptQuota
from retry_budget_study.runner import _acquire_lock


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--live', action='store_true')
    ap.add_argument('--report', type=Path)
    args = ap.parse_args()
    os.chdir(ROOT)
    suite = unittest.TestSuite()
    for file in sorted((ROOT/'tests').glob('test_*.py')):
        if file.name == 'test_harness.py' and not args.live:
            continue
        suite.addTests(unittest.defaultTestLoader.discover(str(ROOT/'tests'), pattern=file.name))
    quota = None
    fd = None
    capture = io.StringIO()
    started = time.time()
    outcome = None
    try:
        if args.live:
            fd = _acquire_lock('manifests/.pilot.lock')
            history = json.loads((ROOT/'manifests/run_index.json').read_text())
            quota = PhysicalAttemptQuota(ROOT/'manifests/physical_quota.json',
                                         sum(r['attempts_used'] for r in history), 3000)
            quota.reserve(64)
            original = adapters.execute_op
            def charged(*a, **kw):
                kw['quota'] = quota
                return original(*a, **kw)
            with patch.object(adapters, 'execute_op', side_effect=charged):
                outcome = unittest.TextTestRunner(stream=capture, verbosity=2).run(suite)
        else:
            with patch('socket.socket.connect', side_effect=AssertionError('pure verification forbids network')):
                outcome = unittest.TextTestRunner(stream=capture, verbosity=2).run(suite)
    finally:
        if quota is not None:
            quota.settle()
            history.append({'run_id': f'quota-tests-{time.strftime("%Y%m%dT%H%M%S")}',
                            'dir': None, 'exit_status': 'OK' if outcome and outcome.wasSuccessful() else 'ERROR',
                            'attempts_used': quota.charged, 'purpose': 'harness regression; not study sample'})
            (ROOT/'manifests/run_index.json').write_text(json.dumps(history, indent=2), encoding='utf-8')
        if fd is not None:
            os.close(fd)
            (ROOT/'manifests/.pilot.lock').unlink()
    report = {'tests': outcome.testsRun, 'failures': len(outcome.failures),
              'errors': len(outcome.errors), 'passed': outcome.wasSuccessful(),
              'physical_attempts': quota.charged if quota else 0,
              'cumulative_used': quota.used if quota else None,
              'python': sys.version, 'platform': platform.platform(),
              'started_wall': started, 'finished_wall': time.time(),
              'output': capture.getvalue()}
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(capture.getvalue())
    print(json.dumps({k:v for k,v in report.items() if k != 'output'}, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
