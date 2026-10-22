"""Regression checks against real library control and wait objects; no traffic."""
from types import SimpleNamespace
from pathlib import Path
from contextlib import redirect_stdout
import importlib.util
import io
import json
import tempfile
import unittest
from unittest.mock import patch

from tenacity import Retrying, RetryError, retry_if_result, stop_after_attempt, wait_none
from retry_budget_study import adapters, analyzer, ir, simulator


def script(name):
    path=Path(__file__).resolve().parents[1]/'scripts'/f'{name}.py'
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReviewSemanticsTests(unittest.TestCase):
    def test_result_exhaustion_is_an_exception_to_outer_controller(self):
        outer=ir.tenacity_layer(stop_attempts=3,wait_s=0)
        inner=ir.tenacity_layer(stop_attempts=2,wait_s=0,on_exception=False,on_statuses=(503,))
        calls=[]
        def f():
            calls.append(1)
            return SimpleNamespace(status_code=503)
        r=Retrying(stop=stop_after_attempt(2),wait=wait_none(),
                   retry=retry_if_result(lambda x:x.status_code==503),reraise=True)
        o=Retrying(stop=stop_after_attempt(3),wait=wait_none(),
                   retry=adapters._tenacity_pred(outer),reraise=True)
        with self.assertRaises(RetryError):
            o(lambda:r(f))
        policy=ir.Policy((outer,inner))
        outcome=ir.Outcome('status',status=503)
        bound=analyzer.analyze(policy,outcome,analyzer.Contract(max_transport_attempts=4))
        modeled=simulator.run(policy,lambda _:outcome)
        self.assertEqual((len(calls),bound.attempts_max,modeled.attempts),(6,6,6))
        self.assertEqual((bound.verdict,modeled.signal),('WILL_BREACH','exception'))

    def test_ineligible_result_does_not_raise_or_retry(self):
        policy=ir.Policy((ir.tenacity_layer(stop_attempts=3,wait_s=0,on_exception=False),))
        result=simulator.run(policy,lambda _:ir.Outcome('status',status=503))
        self.assertEqual((result.attempts,result.signal),(1,('response',503)))

    def test_soft_stop_upper_count_is_not_a_lower_bound(self):
        policy=ir.Policy((ir.tenacity_layer(stop_delay_s=0.1,wait_s=0),
                          ir.u3_layer(total=3,status_forcelist=(503,))))
        outcome=ir.Outcome('status',status=503)
        result=simulator.run(policy,lambda _:outcome)
        bound=analyzer.analyze(policy,outcome,analyzer.Contract(max_transport_attempts=4))
        self.assertEqual(result.attempts,8)
        self.assertLessEqual(bound.attempts_min,result.attempts)
        self.assertGreaterEqual(bound.attempts_max,result.attempts)
        self.assertEqual(bound.verdict,'POSSIBLE_BREACH')

    def test_unbounded_urllib3_status_does_not_convert_infinity_to_int(self):
        policy=ir.Policy((ir.u3_layer(total=None,status_forcelist=(503,)),))
        result=analyzer.analyze(policy,ir.Outcome('status',status=503),analyzer.Contract())
        self.assertEqual(result.verdict,'UNBOUNDED')
        self.assertIsNone(result.attempts_max)

    def test_exponential_wait_matches_library_with_default_maximum(self):
        layer=ir.tenacity_layer(stop_attempts=4,wait_kind='exponential',wait_s=0.1)
        actual=adapters._tenacity_wait(layer)
        self.assertEqual([layer.backoff.delay_for(k) for k in range(1,5)],
                         [actual(SimpleNamespace(attempt_number=k)) for k in range(1,5)])

    def test_exponential_wait_matches_library_with_cap(self):
        layer=ir.tenacity_layer(stop_attempts=4,wait_kind='exponential',wait_s=0.1,wait_max_s=0.25)
        actual=adapters._tenacity_wait(layer)
        self.assertEqual([layer.backoff.delay_for(k) for k in range(1,5)],
                         [actual(SimpleNamespace(attempt_number=k)) for k in range(1,5)])

    def test_budget_inside_retry_is_rejected_before_any_transport(self):
        policy=ir.Policy((ir.tenacity_layer(stop_attempts=3),ir.budget_layer(2),ir.u3_layer()))
        outcome=ir.Outcome('status',status=503)
        self.assertEqual(analyzer.analyze(policy,outcome,analyzer.Contract()).verdict,'UNKNOWN')
        with self.assertRaisesRegex(ValueError,'outermost'):
            simulator.run(policy,lambda _:outcome)
        with self.assertRaisesRegex(ValueError,'outermost'):
            adapters.execute_op(policy,'http://127.0.0.1:1','invalid','invalid')

    def test_archive_checker_rejects_fresh_comparison_mismatches(self):
        module=script('validate_emse_archive')
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'summary.json'
            path.write_text(json.dumps({'cases':1536,'within_bound':1535,
                                        'mismatches':[{'case':'bad'}],'transport_attempts':0}))
            with self.assertRaisesRegex(ValueError,'Fresh comparison failed'):
                module.checked_summary(path,1536)

    def test_library_comparator_returns_failure_for_bad_counts(self):
        module=script('validate_library_states')
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(module,'actual',return_value=(0,'exception')), \
                 patch('sys.argv',['check','--out',folder]), redirect_stdout(io.StringIO()):
                self.assertEqual(module.main(),1)
