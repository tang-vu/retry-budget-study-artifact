"""Verify a replication artifact and reproduce its offline results."""
from pathlib import Path
import argparse
import csv
import hashlib
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def checked_summary(path, want):
    """Check fresh comparison output, including the recorded negative case."""
    result = json.loads(path.read_text(encoding='utf-8'))
    if not (result['cases'] == result['within_bound'] == want
            and not result['mismatches'] and result['transport_attempts'] == 0):
        raise ValueError(f'Fresh comparison failed: {path}')
    return want


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    destination = parser.parse_args().out.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    inventory = json.loads((ROOT / 'SHA256SUMS.json').read_text(encoding='utf-8'))
    for relative, want in inventory.items():
        path = (ROOT / relative).resolve()
        assert path.is_relative_to(ROOT), relative
        assert sha(path.read_bytes()) == want, relative
    snapshots = 0
    for name, freeze in [('boundary', 'status-boundary-freeze-20261022.json'),
                         ('flat', 'status-boundary-flat-freeze-20261022.json')]:
        record = json.loads((ROOT / 'manifests' / freeze).read_text(encoding='utf-8'))
        for relative, want in record['source_sha256'].items():
            path = ROOT / 'frozen-sources' / name / relative.replace('\\', '/')
            assert sha(path.read_bytes()) == want, relative
            snapshots += 1
    diagnostic = ROOT / 'retained-derived/nested-signal-review-20261022'
    record = json.loads((diagnostic / 'freeze.json').read_text(encoding='utf-8'))
    for relative, want in record['source_sha256'].items():
        path = diagnostic / 'source-snapshot' / relative.replace('\\', '/')
        assert sha(path.read_bytes()) == want, relative
        snapshots += 1
    quota_path = ROOT / 'manifests/physical_quota.json'
    quota_before = quota_path.read_bytes()
    commands = [
        ('reproduction', ['scripts/reproduce_emse.py', '--out', str(destination / 'reproduced')]),
        ('pure-tests', ['scripts/verify.py', '--report', str(destination / 'pure-tests.json')]),
        ('library-states', ['scripts/validate_library_states.py', '--out', str(destination / 'library-states')]),
        ('status-boundary', ['scripts/validate_status_boundary.py', '--out', str(destination / 'status-boundary')]),
        ('nested-signals', ['scripts/validate_nested_signals.py', '--out', str(destination / 'nested-signals')]),
    ]
    for label, arguments in commands:
        result = subprocess.run([sys.executable, *arguments], cwd=ROOT, capture_output=True,
                                text=True, encoding='utf-8', errors='replace', timeout=180)
        log = destination / (label + '.log')
        log.write_text(result.stdout + result.stderr, encoding='utf-8')
        if result.returncode:
            raise RuntimeError(f'{label} failed; inspect {log}')
    for name in ('condition_comparison.csv', 'arm_comparison.csv'):
        def rows(path):
            with path.open(newline='', encoding='utf-8') as stream:
                return list(csv.DictReader(stream))
        assert rows(destination / 'reproduced/comparison' / name) == rows(
            ROOT / 'retained-derived/status-boundary-comparison-20261022' / name)
    comparisons = {}
    for name, want in [('library-states', 1536), ('status-boundary', 576), ('nested-signals', 256)]:
        comparisons[name] = checked_summary(destination / name / 'summary.json', want)
    tests = json.loads((destination / 'pure-tests.json').read_text(encoding='utf-8'))
    assert tests['passed'] and tests['physical_attempts'] == 0
    assert quota_path.read_bytes() == quota_before
    report = {'passed': True, 'members_verified': len(inventory),
              'frozen_sources_verified': snapshots, 'table_csvs_match': True,
              'pure_tests': tests['tests'], 'offline_comparisons_rerun': comparisons,
              'quota_unchanged': True, 'study_transport_entries_generated': 0}
    (destination / 'validation.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
