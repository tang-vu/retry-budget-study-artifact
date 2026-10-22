"""Remediation regression tests: layer-index repair identity, dual
scenario-encoding consistency, and simulator replay of repairs."""

import json
import os
import unittest

from retry_budget_study import analyzer, ir, runner, simulator
from retry_budget_study.analyzer import Contract

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MATRIX = json.load(open(os.path.join(ROOT, "config", "pilot_matrix.json"),
                        encoding="utf-8"))
RT = MATRIX["defaults"]["read_timeout_s"]


class TestLayerIdentity(unittest.TestCase):
    def _stack_same_names(self):
        # two layers sharing a name — a name-matched edit would hit both
        l1 = ir.tenacity_layer(stop_attempts=2, wait_s=0.0)
        l2 = ir.tenacity_layer(stop_attempts=3, wait_s=0.0)
        return ir.Policy(name="dup", layers=(l1, l2), method="GET")

    def test_apply_suggestion_uses_layer_index(self):
        pol = self._stack_same_names()
        sug = {"kind": "cap_layer", "layer": pol.layers[0].name,
               "layer_index": 1, "set_max_attempts": 1}
        rp = analyzer.apply_suggestion(pol, sug)
        self.assertEqual(rp.layers[0].max_attempts, 2)   # untouched
        self.assertEqual(rp.layers[1].max_attempts, 1)   # edited

    def test_apply_suggestion_index_name_mismatch_raises(self):
        pol = self._stack_same_names()
        sug = {"kind": "cap_layer", "layer": "no_such_layer",
               "layer_index": 0, "set_max_attempts": 1}
        with self.assertRaises(ValueError):
            analyzer.apply_suggestion(pol, sug)

    def test_suggestions_carry_layer_index(self):
        pol = ir.Policy(name="c", layers=(
            ir.tenacity_layer(stop_attempts=3, wait_s=0.0),
            ir.u3_layer(total=3, status_forcelist=(503,))))
        res = analyzer.analyze(
            pol, ir.Outcome("status", status=503),
            Contract(max_transport_attempts=5))
        for s in res.repairs:
            if s["kind"] in ("cap_layer", "add_attempt_cap",
                             "remove_outer_retry"):
                self.assertIn("layer_index", s)
                self.assertEqual(
                    pol.layers[s["layer_index"]].name, s["layer"])


class TestScenarioConsistency(unittest.TestCase):
    def test_matrix_failures_consistent(self):
        bad = {fid: runner.scenario_consistency_errors(f, RT)
               for fid, f in MATRIX["failures"].items()}
        self.assertEqual({k: v for k, v in bad.items() if v}, {})

    def test_default_tail_drift_detected(self):
        f = json.loads(json.dumps(MATRIX["failures"]["f_flaky2"]))
        # server would persist 503s while the simulator tail stays success
        f["scenario"]["default"] = {"respond": 503, "effect": "apply"}
        errs = runner.scenario_consistency_errors(f, RT)
        self.assertTrue(any("default" in e for e in errs))

    def test_plan_outcome_drift_detected(self):
        f = json.loads(json.dumps(MATRIX["failures"]["f_flaky2"]))
        f["outcomes"][0]["kind"] = "success"
        errs = runner.scenario_consistency_errors(f, RT)
        self.assertTrue(any("plan[0]" in e for e in errs))


class TestNestedBudget(unittest.TestCase):
    def test_inner_budget_wins_not_min(self):
        # [budget(5), budget(10), retry...]: deployed semantics is
        # innermost-wins (adapters last-wins, sim overwrite) -> bound
        # must be 10, and the outer layer must be flagged inert.
        pol = ir.Policy(name="nb", layers=(
            ir.Layer(name="b_out", kind="budget", max_attempts=5,
                     budget_attempts=5),
            ir.Layer(name="b_in", kind="budget", max_attempts=10,
                     budget_attempts=10),
            ir.tenacity_layer(stop_attempts=3, wait_s=0.0),
            ir.u3_layer(total=3, status_forcelist=(503,))))
        res = analyzer.analyze(
            pol, ir.Outcome("status", status=503),
            Contract(max_transport_attempts=5))
        self.assertEqual(res.attempts_max, 10)
        self.assertTrue(any(w.code == "SHADOWED_BUDGET"
                            for w in res.warnings))

    def test_add_shared_budget_not_suggested_over_budget_layer(self):
        # on an already-budgeted stack the repair must cap the effective
        # layer, not insert a shadowed one (sim replay caught sim=10 vs
        # analyzer bound=5 for the old add_shared_budget on p_deadline)
        spec = MATRIX["policies"]["p_deadline"]
        pol = runner.policy_from_spec("p_deadline", spec, "GET",
                                      {"read_timeout_s": RT,
                                       "connect_timeout_s": 0.5})
        res = analyzer.analyze(
            pol, ir.Outcome("status", status=503),
            Contract(max_transport_attempts=5))
        kinds = [s["kind"] for s in res.repairs]
        self.assertIn("cap_shared_budget", kinds)
        self.assertNotIn("add_shared_budget", kinds)
        sug = next(s for s in res.repairs if s["kind"] == "cap_shared_budget")
        rp = analyzer.apply_suggestion(pol, sug)
        self.assertEqual(rp.layers[sug["layer_index"]].budget_attempts, 5)
        # and it replays under the contract in the independent model
        failure = MATRIX["failures"]["f_status503"]
        outcomes = runner.outcomes_from_spec(failure)
        sr = simulator.run(
            rp, simulator.scenario_fn(outcomes, default=outcomes[-1]),
            read_timeout_s=RT, connect_timeout_s=0.5)
        self.assertLessEqual(sr.attempts, 5)


class TestRepairReplay(unittest.TestCase):
    def test_cap_layer_repair_replays_under_contract(self):
        # ev_u3ten_503: naive 12-attempt composition; cap urllib3->1 must
        # replay at <= contract cap (5) on the same persistent-503 scenario
        failure = MATRIX["failures"]["f_status503"]
        pol = runner.policy_from_spec(
            "p_u3_ten", MATRIX["policies"]["p_u3_ten"], "GET",
            {"read_timeout_s": RT,
             "connect_timeout_s": MATRIX["defaults"]["connect_timeout_s"]})
        outcomes = runner.outcomes_from_spec(failure)
        fn = simulator.scenario_fn(outcomes, default=outcomes[-1])
        sug = {"kind": "cap_layer", "layer": "urllib3", "layer_index": 1,
               "set_max_attempts": 1}
        rp = analyzer.apply_suggestion(pol, sug)
        sr = simulator.run(rp, fn, read_timeout_s=RT,
                           connect_timeout_s=0.5)
        self.assertLessEqual(sr.attempts, 5)
        self.assertEqual(sr.attempts, 3)


if __name__ == "__main__":
    unittest.main()
