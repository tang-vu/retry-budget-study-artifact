import tempfile
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from retry_budget_study.adapters import RequestsTransport
from retry_budget_study.ir import u3_layer
from retry_budget_study.ledger import OperationLedger
from retry_budget_study.quota import PhysicalAttemptQuota, PhysicalQuotaExhausted
from retry_budget_study.analysis import _breaches, _rep_rows


class PhysicalObservationTests(unittest.TestCase):
    def test_export_preserves_incomplete_effects_as_unknown(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'rep0';p.mkdir()
            op={'op_id':'op','outcome':'success','transport_attempts':1,'wall_s':.1}
            (p/'ops.json').write_text(json.dumps([{'op_id':'op','op':op}]))
            (p/'server_ledger.json').write_text('{"op":[{"effect":"apply"}]}')
            (p/'effect_state.json').write_text('{"op":1}')
            (p/'completion.json').write_text('{"complete":false}')
            row=_rep_rows(d)[0]
            self.assertIsNone(row['effects'])
            self.assertEqual(row['effects_observed'],1)
            self.assertFalse(row['effects_complete'])

    def test_actual_entry_counted_without_retry_decision(self):
        with tempfile.TemporaryDirectory() as d:
            quota = PhysicalAttemptQuota(Path(d)/'quota.json', 2, 3)
            ledger = OperationLedger('op')
            transport = RequestsTransport(u3_layer(total=2), 'http://127.0.0.1:1',
                                          'pure', ledger, quota=quota)
            pool = transport.session.adapters['http://'].poolmanager.connection_from_url(
                'http://127.0.0.1:1')
            with patch('urllib3.connectionpool.HTTPConnectionPool._make_request',
                       side_effect=ConnectionError('synthetic; no network')) as parent:
                with self.assertRaises(ConnectionError):
                    pool._make_request(None, 'GET', '/')
                with self.assertRaises(PhysicalQuotaExhausted):
                    pool._make_request(None, 'GET', '/')
                self.assertEqual(parent.call_count, 1)
            self.assertEqual(ledger.count('physical_attempt_start'), 1)
            self.assertEqual(ledger.count('retry_scheduled'), 0)
            transport.session.close()

    def test_deadline_uses_budget_origin_and_physical_entry(self):
        events = [dict(op_id='op', kind='budget_created', t_mono=5,
                       data={'deadline_mono': 6}),
                  dict(op_id='op', kind='retry_scheduled', t_mono=5.9),
                  dict(op_id='op', kind='physical_attempt_start', t_mono=6.1)]
        breaches = _breaches({'op_id':'op','transport_attempts':1,'wall_s':.1},
                            {'effects':0}, {'deadline_s':1}, events)
        self.assertEqual(breaches[0]['rule'], 'deadline_new_attempt')

    def test_missing_effect_count_is_not_zero(self):
        self.assertEqual(_breaches({'transport_attempts':1,'wall_s':1},
                                  {'effects':None}, {'max_effects':0}, []), [])
