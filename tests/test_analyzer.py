"""Analyzer unit tests: bounds, propagation semantics, verdicts."""

import unittest

from retry_budget_study import analyzer, ir
from retry_budget_study.analyzer import Contract


def _an(pol, outcome, contract=None, **kw):
    return analyzer.analyze(
        pol, outcome, contract or Contract(max_transport_attempts=5), **kw)


class TestCompositionBounds(unittest.TestCase):
    def test_product_bound(self):
        pol = ir.Policy(name="c", layers=(
            ir.tenacity_layer(stop_attempts=3, wait_s=0.01),
            ir.u3_layer(total=3, status_forcelist=(503,))))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertEqual(r.attempts_max, 12.0)  # 3 x (3+1)
        self.assertEqual(r.verdict, "WILL_BREACH")

    def test_single_layer(self):
        pol = ir.Policy(name="s", layers=(
            ir.u3_layer(total=3, status_forcelist=(503,)),))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertEqual(r.attempts_max, 4.0)
        self.assertEqual(r.verdict, "GUARANTEED")

    def test_method_gating_collapses_bound(self):
        # POST outside default allowed_methods: urllib3 will not retry,
        # and tenacity-on-exception sees a response, not an exception
        pol = ir.Policy(name="p", method="POST", layers=(
            ir.tenacity_layer(stop_attempts=3),
            ir.u3_layer(total=3, status_forcelist=(503,))))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertEqual(r.attempts_max, 1.0)
        self.assertIn("METHOD_NOT_ELIGIBLE", [w.code for w in r.warnings])

    def test_raise_on_status_false_routes_to_result_predicate(self):
        # raise_on_status=False -> outer sees response(status); only a
        # result predicate can retry it
        pol = ir.Policy(name="r", layers=(
            ir.tenacity_layer(stop_attempts=3, on_statuses=(503,)),
            ir.u3_layer(total=3, status_forcelist=(503,),
                        raise_on_status=False)))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertEqual(r.attempts_max, 12.0)

    def test_response_path_without_result_pred_no_retry(self):
        pol = ir.Policy(name="r2", layers=(
            ir.tenacity_layer(stop_attempts=3),  # exception pred only
            ir.u3_layer(total=3, status_forcelist=(503,),
                        raise_on_status=False)))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertEqual(r.attempts_max, 4.0)

    def test_soft_deadline_bounds_by_cost_model(self):
        pol = ir.Policy(name="d", layers=(
            ir.tenacity_layer(stop_delay_s=0.3, wait_s=0.02,
                              on_statuses=(503,)),
            ir.u3_layer(total=0)))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertIsNotNone(r.attempts_max)
        self.assertIn("NO_ATTEMPT_CAP", [w.code for w in r.warnings])

    def test_unknown_predicate(self):
        pol = ir.Policy(name="u", layers=(
            ir.tenacity_layer(stop_attempts=2),
            ir.Layer(name="x", kind="urllib3", max_attempts=2,
                     predicate=ir.RetryPredicate(unknown=True))))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertEqual(r.verdict, "UNKNOWN")

    def test_effects_bound(self):
        pol = ir.Policy(name="e", method="POST", layers=(
            ir.tenacity_layer(stop_attempts=3),
            ir.u3_layer(total=3, read_timeout_s=0.25)))
        r = _an(pol, ir.Outcome("read_err"),
                Contract(max_transport_attempts=5, max_effects=1))
        # urllib3 skips POST read retries -> 3 attempts, all reaching
        self.assertEqual(r.attempts_max, 3.0)
        self.assertEqual(r.effects_max, 3.0)
        self.assertEqual(r.verdict, "POSSIBLE_BREACH")

    def test_repair_suggestions(self):
        pol = ir.Policy(name="c", layers=(
            ir.tenacity_layer(stop_attempts=3),
            ir.u3_layer(total=3, status_forcelist=(503,))))
        r = _an(pol, ir.Outcome("status", status=503))
        kinds = {s["kind"] for s in r.repairs}
        self.assertIn("cap_layer", kinds)
        self.assertIn("add_shared_budget", kinds)

    def test_remove_outer_retry_bound_is_inner_product(self):
        # tenacity(3) x urllib3(total=3 -> 4 attempts): removing the
        # OUTER retry leaves the inner urllib3 cap (4), not min(3, 4)
        pol = ir.Policy(name="c", layers=(
            ir.tenacity_layer(stop_attempts=3),
            ir.u3_layer(total=3, status_forcelist=(503,))))
        r = _an(pol, ir.Outcome("status", status=503))
        sug = [s for s in r.repairs if s["kind"] == "remove_outer_retry"]
        self.assertEqual(len(sug), 1)
        self.assertEqual(sug[0]["resulting_bound"], 4)
        # repair verification must recompute the same bound on the
        # materialized policy rather than assert it
        self.assertEqual(sug[0]["verified_attempts_max"], 4)
        self.assertTrue(sug[0]["verified_meets_contract"])

    def test_httpx_connect_retries_have_backoff(self):
        # httpcore retries connect-phase failures with
        # exponential_backoff(0.5): inter-retry sleeps 0, 0.5, 1, 2...
        pol = ir.Policy(name="h", layers=(
            ir.Layer(name="httpx", kind="httpx", max_attempts=3,
                     predicate=ir.RetryPredicate(
                         on_connect=True, on_read=False, on_other=False),
                     backoff=ir.BackoffSpec(kind="httpcore_exp",
                                            base_s=0.5)),))
        r = _an(pol, ir.Outcome("conn_err"),
                Contract(max_transport_attempts=99, max_wall_s=99),
                connect_timeout_s=0.5)
        self.assertEqual(r.attempts_max, 3.0)
        # 3 attempts at <=0.55s ceiling each + inter-retry sleeps 0 + 0.5
        self.assertGreaterEqual(r.elapsed_max_s, 3 * 0.55 + 0.5)

    def test_httpx_non_connect_outcomes_not_retried(self):
        pol = ir.Policy(name="h", layers=(
            ir.Layer(name="httpx", kind="httpx", max_attempts=3,
                     predicate=ir.RetryPredicate(
                         on_connect=True, on_read=False, on_other=False),
                     backoff=ir.BackoffSpec(kind="httpcore_exp",
                                            base_s=0.5)),))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertEqual(r.attempts_max, 1.0)


class TestBudgetLayer(unittest.TestCase):
    def test_budget_clamps_product(self):
        pol = ir.Policy(name="b", layers=(
            ir.budget_layer(max_attempts=5),
            ir.tenacity_layer(stop_attempts=3),
            ir.u3_layer(total=3, status_forcelist=(503,))))
        r = _an(pol, ir.Outcome("status", status=503))
        self.assertEqual(r.attempts_max, 5.0)
        self.assertEqual(r.verdict, "GUARANTEED")

    def test_deadline_gates_new_attempts(self):
        pol = ir.Policy(name="bd", layers=(
            ir.budget_layer(max_attempts=10, deadline_s=0.5),
            ir.u3_layer(total=3, read_timeout_s=0.25)))
        r = _an(pol, ir.Outcome("read_err"), read_timeout_s=0.25)
        self.assertIn("DEADLINE_GATES_ONLY", [w.code for w in r.warnings])
        self.assertLessEqual(r.attempts_max, 10)


if __name__ == "__main__":
    unittest.main()
