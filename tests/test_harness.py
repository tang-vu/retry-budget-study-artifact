"""Live loopback harness tests: fault server + adapter + ledgers."""

import unittest

from retry_budget_study import adapters, ir
from retry_budget_study.fault_server import FaultServer
from retry_budget_study.ledger import ServerLedger, new_op_id


def _run(policy, scenario, cell_id="t-cell"):
    """Execute one logical op against a fresh fault server."""
    sl = ServerLedger()
    with FaultServer(scenario, sl) as srv:
        op_id = new_op_id("t")
        res, ol = adapters.execute_op(policy, srv.url, cell_id, op_id)
    return res, ol, sl.op(op_id)


class TestFaultServer(unittest.TestCase):
    def test_receipt_ledger_records_effect(self):
        res, _ol, recs = _run(
            ir.Policy(layers=(ir.u3_layer(total=0),), name="t"),
            {"plan": [{"respond": 200, "effect": "apply"}]},
        )
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["effect"], "apply")
        self.assertTrue(recs[0]["responded"])

    def test_receipts_match_attempts_status(self):
        pol = ir.Policy(
            layers=(ir.u3_layer(total=3, status_forcelist=(503,)),),
            name="t")
        res, _ol, recs = _run(pol, {"default": {"respond": 503}})
        self.assertEqual(res.transport_attempts, 4)
        self.assertEqual(len(recs), 4)
        self.assertEqual(res.outcome, "exception")  # RetryError on exhaust

    def test_drop_then_success(self):
        pol = ir.Policy(
            layers=(ir.u3_layer(total=2, read_timeout_s=0.3),), name="t")
        res, _ol, recs = _run(
            pol,
            {"plan": [{"drop": True, "effect": "apply"}],
             "default": {"respond": 200}},
        )
        self.assertEqual(res.outcome, "success")
        self.assertEqual(len(recs), 2)
        # receipt 1 reached the server and applied its effect even though
        # the client saw a read error — the core RQ distinction
        self.assertEqual(recs[0]["effect"], "apply")
        self.assertFalse(recs[0]["responded"])


class TestComposedLive(unittest.TestCase):
    def test_application_status_check_routes_returned_errors(self):
        for method, want in [('GET', 12), ('POST', 3)]:
            policy = ir.Policy(layers=(
                ir.tenacity_layer(stop_attempts=3, wait_s=0),
                ir.Layer('status_check', 'status_check', 1),
                ir.u3_layer(total=3, status_forcelist=(503,), raise_on_status=False),
            ), method=method, name='checked')
            result, _, receipts = _run(policy, {'default': {'respond': 503}})
            self.assertEqual(result.transport_attempts, want)
            self.assertEqual(len(receipts), want)
            self.assertEqual(result.outcome, 'exception')

    def test_product_attempts(self):
        pol = ir.Policy(layers=(
            ir.tenacity_layer(stop_attempts=3, wait_s=0.0),
            ir.u3_layer(total=3, status_forcelist=(503,)),
        ), name="c")
        res, _ol, recs = _run(pol, {"default": {"respond": 503}})
        self.assertEqual(res.transport_attempts, 12)
        self.assertEqual(res.wrapper_invocations, 6)
        self.assertEqual(len(recs), 12)

    def test_post_gating_live(self):
        pol = ir.Policy(layers=(
            ir.tenacity_layer(stop_attempts=3, wait_s=0.0),
            ir.u3_layer(total=3, status_forcelist=(503,)),
        ), method="POST", name="p")
        res, _ol, recs = _run(pol, {"default": {"respond": 503}})
        # POST outside urllib3 allowed_methods: response (not exception)
        # propagates; tenacity result predicate does not match
        self.assertEqual(res.transport_attempts, 1)
        self.assertEqual(res.status, 503)
        self.assertEqual(len(recs), 1)


if __name__ == "__main__":
    unittest.main()
