import json
import tempfile
import unittest
from pathlib import Path
from retry_budget_study.quota import PhysicalAttemptQuota, PhysicalQuotaExhausted


class QuotaTests(unittest.TestCase):
    def test_reservation_charges_crash_but_settles_known_unused_units(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'quota.json'
            q = PhysicalAttemptQuota(p, 10, 20)
            q.reserve(6)
            q.consume()
            self.assertEqual(PhysicalAttemptQuota(p, 10, 20).used, 16)
            self.assertEqual(q.used, 11)
            q.settle()
            self.assertEqual(PhysicalAttemptQuota(p, 10, 20).used, 11)

    def test_cumulative_cap_survives_restart_and_stale_history(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'quota.json'
            q = PhysicalAttemptQuota(p, 2258, 2260)
            q.consume()
            q2 = PhysicalAttemptQuota(p, 2258, 2260)
            self.assertEqual(q2.used, 2259)
            q2.consume()
            with self.assertRaises(PhysicalQuotaExhausted):
                q2.consume()
            self.assertEqual(json.loads(p.read_text())['used'], 2260)

    def test_history_cannot_be_reset_by_low_checkpoint(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'quota.json'
            p.write_text('{"used": 2}')
            q = PhysicalAttemptQuota(p, 10, 11)
            q.consume()
            self.assertEqual(q.used, 11)
            with self.assertRaises(PhysicalQuotaExhausted):
                PhysicalAttemptQuota(p, 12, 11)
