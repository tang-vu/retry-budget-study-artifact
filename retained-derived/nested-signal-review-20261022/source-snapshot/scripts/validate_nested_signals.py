"""Post-inspection check of nested Tenacity result exhaustion; no network."""
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
from tenacity import Retrying, stop_after_attempt, wait_none

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from retry_budget_study import adapters, analyzer, ir, simulator


def real_execution(outer,inner,status,check):
    count=0
    def transport():
        nonlocal count
        count+=1
        response=requests.Response()
        response.status_code=status
        if check:
            response.raise_for_status()
        return response
    def controller(layer,call):
        return Retrying(stop=stop_after_attempt(layer.max_attempts),wait=wait_none(),
                        retry=adapters._tenacity_pred(layer),reraise=True,
                        sleep=lambda _:None)(call)
    try:
        response=controller(outer,lambda:controller(inner,transport))
        signal='success' if response.status_code==200 else f'response-{response.status_code}'
    except Exception:
        signal='exception'
    return count,signal


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True)
    dest=parser.parse_args().out
    dest.mkdir(parents=True,exist_ok=False)
    cases=list(itertools.product((1,3),(1,2),(False,True),(False,True),
                                 (False,True),(False,True),(False,True),(200,503)))
    files=[Path(__file__)]+[ROOT/'src/retry_budget_study'/name for name in
                           ('adapters.py','analyzer.py','ir.py','simulator.py')]
    freeze={'design':'post-inspection full factorial diagnostic prompted by a discovered counterexample',
            'cases':cases,'transport_attempts':0,'independent_field_sample':False,
            'source_sha256':{p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
            'versions':{name:importlib.metadata.version(name) for name in ('requests','tenacity')},
            'python':sys.version}
    (dest/'freeze.json').write_text(json.dumps(freeze,indent=2),encoding='utf-8')
    rows=[]
    with patch('socket.socket.connect',side_effect=AssertionError('offline check forbids network')):
        for number,(oc,ic,oe,orr,ie,irr,check,status) in enumerate(cases):
            outer=ir.tenacity_layer(stop_attempts=oc,wait_s=0,on_exception=oe,on_statuses=(503,) if orr else ())
            inner=ir.tenacity_layer(stop_attempts=ic,wait_s=0,on_exception=ie,on_statuses=(503,) if irr else ())
            layers=(outer,inner)+( (ir.Layer('check','status_check',1),) if check else () )
            policy=ir.Policy(layers,name=f'nested-{number:03}')
            outcome=ir.Outcome('success') if status==200 else ir.Outcome('status',status=status)
            bound=analyzer.analyze(policy,outcome,analyzer.Contract(max_transport_attempts=100))
            modeled=simulator.run(policy,lambda _:outcome)
            observed,signal=real_execution(outer,inner,status,check)
            modeled_signal=(f'response-{modeled.signal[1]}' if isinstance(modeled.signal,tuple) else modeled.signal)
            within=bound.attempts_min<=observed<=bound.attempts_max
            rows.append(dict(case_id=policy.name,outer_cap=oc,inner_cap=ic,outer_exception=oe,
                             outer_result=orr,inner_exception=ie,inner_result=irr,
                             check_response=check,status=status,predicted_min=bound.attempts_min,
                             predicted_max=bound.attempts_max,observed=observed,
                             simulated=modeled.attempts,terminal_signal=signal,
                             simulated_signal=modeled_signal,within_bound=within,
                             simulator_agrees=(modeled.attempts==observed and modeled_signal==signal)))
    with (dest/'comparison.csv').open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    summary={'cases':len(rows),'within_bound':sum(r['within_bound'] for r in rows),
             'simulator_agrees':sum(r['simulator_agrees'] for r in rows),
             'mismatches':[r for r in rows if not r['within_bound'] or not r['simulator_agrees']],
             'transport_attempts':0,'post_inspection':True}
    (dest/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps(summary,indent=2))
    return 0 if not summary['mismatches'] else 1


if __name__=='__main__':
    raise SystemExit(main())
