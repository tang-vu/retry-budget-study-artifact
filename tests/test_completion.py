"""Pure synchronization tests: no network traffic or quota consumption."""
import threading
import unittest

from retry_budget_study.fault_server import CompletionTracker, EffectState
from retry_budget_study.ledger import ServerLedger


class CompletionTests(unittest.TestCase):
    def test_counter_state_is_not_derived_from_receipt_labels(self):
        state = EffectState()
        ledger = ServerLedger()
        idx = ledger.record_receipt('op', {})
        state.apply('op')
        ledger.update_receipt('op', idx, effect='none')
        self.assertEqual(state.snapshot(), {'op': 1})
        self.assertEqual(ledger.counts('op')['effects'], 0)
        snapshot = state.snapshot()
        state.apply('op')
        self.assertEqual(snapshot, {'op': 1})

    def test_idle_and_pending_are_distinct(self):
        tracker = CompletionTracker()
        self.assertTrue(tracker.drain(0))
        tracker.enter()
        self.assertFalse(tracker.drain(0))
        tracker.leave()
        self.assertTrue(tracker.drain(0))

    def test_wait_observes_delayed_completion(self):
        tracker = CompletionTracker()
        tracker.enter()
        release = threading.Event()
        worker = threading.Thread(target=lambda: (release.wait(), tracker.leave()))
        worker.start()
        self.assertFalse(tracker.drain(0))
        release.set()
        self.assertTrue(tracker.drain(1))
        worker.join(1)
        self.assertFalse(worker.is_alive())

    def test_one_completion_does_not_clear_other_handlers(self):
        tracker = CompletionTracker()
        tracker.enter()
        tracker.enter()
        tracker.leave()
        self.assertFalse(tracker.drain(0))
        tracker.leave()
        self.assertTrue(tracker.drain(0))
        with self.assertRaises(RuntimeError):
            tracker.leave()
        with self.assertRaises(ValueError):
            tracker.drain(-1)
