import unittest

from retry_budget_study.ledger import ServerLedger
from retry_budget_study.analysis import _breaches


class MeasurementAuditTests(unittest.TestCase):
    def test_post_connect_retry_matches_library_state_machine(self):
        from urllib3.util import Retry
        from urllib3.exceptions import ConnectTimeoutError
        from retry_budget_study import analyzer, ir, simulator
        retry = Retry(total=3)
        for _ in range(3):
            retry = retry.increment(method='POST', error=ConnectTimeoutError())
        self.assertEqual(retry.total, 0)
        policy = ir.Policy(method='POST', layers=(ir.u3_layer(total=3),))
        failure = ir.Outcome('conn_err')
        bound = analyzer.analyze(policy, failure, analyzer.Contract(max_transport_attempts=5))
        self.assertEqual(bound.attempts_max, 4)
        self.assertEqual(simulator.run(policy, lambda _: failure).attempts, 4)

    def test_out_of_order_completions_keep_request_identity(self):
        ledger = ServerLedger()
        first = ledger.record_receipt('op', {})
        second = ledger.record_receipt('op', {})
        ledger.update_receipt('op', second, effect='none')
        ledger.update_receipt('op', first, effect='apply')
        self.assertEqual([r['effect'] for r in ledger.op('op')], ['apply', 'none'])
        snapshot = ledger.to_dict()
        ledger.update_receipt('op', first, effect='none')
        self.assertEqual(snapshot['op'][0]['effect'], 'apply')

    def test_internal_retry_after_deadline_is_not_omitted(self):
        op = {'op_id': 'op', 'transport_attempts': 2, 'wall_s': 2}
        events = [dict(op_id='op', kind='attempt_start', t_mono=10),
                  dict(op_id='op', kind='retry_scheduled', t_mono=12),
                  dict(op_id='unrelated', kind='attempt_start', t_mono=99)]
        breaches = _breaches(op, {'effects': 0}, {'deadline_s': 1}, events)
        self.assertEqual(breaches, [{'rule': 'deadline_new_attempt', 'value': 1, 'limit': 1}])
