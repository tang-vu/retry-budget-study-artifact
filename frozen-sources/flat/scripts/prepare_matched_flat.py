"""Freeze a post-inspection comparator on every status-boundary condition."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
matrix_path = ROOT/'config/status_boundary_flat_20261022.json'
freeze_path = ROOT/'manifests/status-boundary-flat-freeze-20261022.json'
if matrix_path.exists() or freeze_path.exists():
    raise SystemExit('Existing freeze; do not overwrite.')
base = json.loads((ROOT/'config/status_boundary_20261022.json').read_text())
base['matrix_version'] = 'status-boundary-flat-20261022-v1'
base['policies'] = {'flat': {'layers': [
    {'kind':'tenacity','stop_attempts':4,'wait_kind':'none','on_exception':True},
    {'kind':'status_check'},
    {'kind':'urllib3','total':0,'status_forcelist':[503],'raise_on_status':False}]}}
base['cells'] = [{'id':f'flat_{m.lower()}_{f}', 'method':m, 'split':'eval',
                  'policy':'flat','failure':f}
                 for m in ('GET','POST') for f in base['failures']]
matrix_path.write_text(json.dumps(base,indent=2),encoding='utf-8')
record = {'type':'post-inspection comparator, frozen before its own execution',
          'selection':'all eight unchanged method/failure conditions; no outcome-dependent subset',
          'purpose':'same four-attempt contract; one retry layer; compare recovery as well as count',
          'previous_status_boundary_run':'raw/status_boundary_20261022',
          'observations_already_inspected':True, 'reservation_attempts':64,
          'python':sys.version, 'operations':16,
          'git_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
          'matrix_sha256':hashlib.sha256(matrix_path.read_bytes()).hexdigest(),
          'source_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in list((ROOT/'src/retry_budget_study').glob('*.py'))+[Path(__file__)]}}
freeze_path.write_text(json.dumps(record,indent=2),encoding='utf-8')
print(json.dumps({'cells':8,'operations':16,'reservation_attempts':64}))
