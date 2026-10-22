"""Freeze an authored status-boundary extension before loopback execution."""
from pathlib import Path
import copy
import hashlib
import importlib.metadata
import json
import platform
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    target = ROOT/'config/status_boundary_20261022.json'
    freeze = ROOT/'manifests/status-boundary-freeze-20261022.json'
    if target.exists() or freeze.exists():
        raise SystemExit('Frozen design already exists; preserve it and use a new version.')
    transport = {'kind':'urllib3', 'total':3, 'status_forcelist':[503],
                 'backoff_factor':0, 'raise_on_status':False, 'allowed_methods':'default'}
    outer = {'kind':'tenacity', 'stop_attempts':3, 'wait_kind':'none', 'on_exception':True}
    checked = [outer, {'kind':'status_check'}, transport]
    policies = {'unchecked': {'layers':[outer, transport]},
                'checked': {'layers':checked},
                'single': {'layers':[{'kind':'status_check'}, transport]},
                'shared': {'layers':[{'kind':'budget','max_attempts':4}, *checked]}}
    failures = {'persistent': {'scenario': {'default':{'respond':503}},
                               'outcomes':[{'kind':'status','status':503}]}}
    for count in (2, 4):
        failures[f'recover_{count}'] = {
            'scenario': {'plan':[{'respond':503} for _ in range(count)],
                         'default':{'respond':200, 'effect':'apply'}},
            'outcomes':[{'kind':'status','status':503} for _ in range(count)] + [{'kind':'success'}]}
    failures['drop_status'] = {
        'scenario': {'plan':[{'drop':True,'effect':'apply'}, {'respond':503}],
                     'default':{'respond':200,'effect':'apply'}},
        'outcomes':[{'kind':'read_err'}, {'kind':'status','status':503}, {'kind':'success'}]}
    cells = [{'id':f'boundary_{method.lower()}_{family}_{arm}', 'method':method,
              'split':'eval', 'failure':family, 'policy':arm}
             for method in ('GET','POST') for family in failures for arm in policies]
    matrix = {'matrix_version':'status-boundary-20261022-v1',
              'defaults':{'connect_timeout_s':0.15,'read_timeout_s':0.08,'reps':2,
                          'contract':{'max_transport_attempts':4}},
              'policies':policies, 'failures':failures, 'cells':cells}
    target.write_text(json.dumps(matrix, indent=2), encoding='utf-8')
    sources = list((ROOT/'src/retry_budget_study').glob('*.py')) + [Path(__file__)]
    design = {'type':'pre-execution design freeze; authored, source-informed, not preregistered field sample',
              'source': {'url':'https://airflow.apache.org/docs/apache-airflow-providers-http/6.0.0/_modules/airflow/providers/http/hooks/http.html',
                         'version':'apache-airflow-providers-http 6.0.0',
                         'consulted':'2026-10-22',
                         'mapping':'optional adapter + application raise_for_status + outer tenacity retry',
                         'deviations':'Airflow code is not executed. Outer limit 3, inner total 3, zero waits, faults and method matrix are authored; exception class is requests.HTTPError, not AirflowException.'},
              'git_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
              'matrix_sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
              'source_sha256': {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
              'python': sys.version, 'platform':platform.platform(),
              'versions':{n:importlib.metadata.version(n) for n in ('requests','urllib3','tenacity','httpx','httpcore','charset_normalizer','idna','certifi','anyio','h11')},
              'operations':len(cells)*2, 'reservation_attempts':256,
              'analysis':'Compare attempts, caller success and drained effects by method/family/arm; repetitions are stability checks. No causal timing, field prevalence or superiority claim. Keep all cells including negative results.'}
    freeze.write_text(json.dumps(design, indent=2), encoding='utf-8')
    print(json.dumps({'cells':len(cells),'operations':len(cells)*2,'reserve':256}))


if __name__ == '__main__':
    main()
