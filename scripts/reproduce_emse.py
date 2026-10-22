"""Rebuild the manuscript's evidence tables with all socket connections disabled."""
from pathlib import Path
import argparse
import json
import sys
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from retry_budget_study.analysis import analyze_run, audit_main_measurements, write_status_boundary_comparison


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--out',type=Path,required=True)
    destination=ap.parse_args().out.resolve()
    destination.mkdir(parents=True,exist_ok=False)
    reports={}
    with patch('socket.socket.connect',side_effect=AssertionError('offline reproduction forbids network')):
        for name,run,matrix in [
            ('pilot','run_20260921_225755','pilot_matrix.json'),
            ('previous-pilot','run_20260921_postfix','pilot_matrix.json'),
            ('controlled-initial','main_controlled_20260922','main_matrix.json'),
            ('controlled-replication','main_replication_20260922','main_matrix.json'),
            ('boundary','status_boundary_20261022','status_boundary_20261022.json'),
            ('flat','status_boundary_flat_20261022','status_boundary_flat_20261022.json')]:
            raw=ROOT/'raw'/run
            out=destination/name
            out.mkdir()
            metrics=analyze_run(str(raw),str(out),str(ROOT/'config'/matrix))
            reports[name]={'attempts_used':metrics['attempts_used'],
                           'elapsed_bound_miss_cells':metrics.get('n_bound_misses'),
                           'measured_cells':len(metrics.get('cells',{}))}
            if name not in ('pilot','previous-pilot'):
                audit=audit_main_measurements(str(raw),str(out))
                reports[name].update({k:audit[k] for k in ('operations','attempts','all_complete','physical_counts_match','effect_counts_match')})
        reports['comparison']=write_status_boundary_comparison(
            ROOT/'raw/status_boundary_20261022', ROOT/'raw/status_boundary_flat_20261022',
            destination/'comparison')
    (destination/'reproduction.json').write_text(json.dumps(reports,indent=2),encoding='utf-8')
    print(json.dumps(reports,indent=2))


if __name__=='__main__':
    main()
