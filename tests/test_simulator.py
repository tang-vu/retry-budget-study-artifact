"""Simulator unit tests: scenario replay + bounded exploration."""

import unittest

from retry_budget_study import analyzer, ir, simulator
from retry_budget_study.analyzer import Contract


def _pol_product():
    return ir.Policy(name="c", layers=(
        ir.tenacity_layer(stop_attempts=3, wait_s=0.01),
        ir.u3_layer(total=3, status_forcelist=(503,))))


class TestReplay(unittest.TestCase):
    def test_persistent_status_product(self):
        pol = _pol_product()
        seq = [ir.Outcome("status", status=503)] * 20
        r = simulator.run(pol, simulator.scenario_fn(seq))
        self.assertEqual(r.attempts, 12)

    def test_transient_inner_absorbs(self):
        pol = _pol_product()
        seq = [ir.Outcome("status", status=503)] * 2 + [ir.Outcome("success")]
        r = simulator.run(pol, simulator.scenario_fn(seq))
        self.assertEqual(r.attempts, 3)  # inner succeeds on attempt 3

    def test_retry_after_wait(self):
        pol = ir.Policy(name="ra", layers=(
            ir.u3_layer(total=3, status_forcelist=(503,),
                        respect_retry_after=True),))
        seq = [ir.Outcome("status", status=503, retry_after_s=1.0)] * 2 \
            + [ir.Outcome("success")]
        r = simulator.run(pol, simulator.scenario_fn(seq))
        self.assertEqual(r.attempts, 3)
        self.assertGreaterEqual(r.elapsed_s, 2.0)

    def test_retry_after_ignored(self):
        pol = ir.Policy(name="nora", layers=(
            ir.u3_layer(total=3, status_forcelist=(503,),
                        respect_retry_after=False),))
        seq = [ir.Outcome("status", status=503, retry_after_s=1.0)] * 2 \
            + [ir.Outcome("success")]
        r = simulator.run(pol, simulator.scenario_fn(seq))
        self.assertLess(r.elapsed_s, 0.5)

    def test_budget_deadline_gates(self):
        pol = ir.Policy(name="bd", layers=(
            ir.budget_layer(max_attempts=10, deadline_s=0.5),
            ir.u3_layer(total=3, read_timeout_s=0.25)))
        # server holds 0.4s; read timeout 0.25 cuts it -> ~0.27s/attempt
        seq = [ir.Outcome("read_err", delay_s=0.4)] * 10
        r = simulator.run(pol, simulator.scenario_fn(seq),
                        read_timeout_s=0.25)
        # sim costs attempts at the ceiling (~0.27): sends at ~0, ~0.27;
        # the 3rd send would start at ~0.54 >= deadline -> gated
        self.assertEqual(r.attempts, 2)

    def test_scenario_default_repeats_last_outcome(self):
        # matrix convention: the LAST listed outcome is the persistent
        # tail that the live server's scenario["default"] produces;
        # the runner passes it explicitly so sim and live replay the
        # same failure trace (persistent 503 -> full 12-attempt product)
        pol = _pol_product()
        seq = [ir.Outcome("status", status=503)]
        r = simulator.run(pol, simulator.scenario_fn(seq, default=seq[-1]))
        self.assertEqual(r.attempts, 12)

    def test_httpx_retries_connect_to_cap_with_backoff(self):
        # httpcore retries connect-phase failures with
        # exponential_backoff(0.5): sleeps 0, 0.5, 1... between attempts
        pol = ir.Policy(name="h", layers=(
            ir.Layer(name="httpx", kind="httpx", max_attempts=3,
                     predicate=ir.RetryPredicate(
                         on_connect=True, on_read=False, on_other=False),
                     backoff=ir.BackoffSpec(kind="httpcore_exp",
                                            base_s=0.5)),))
        seq = [ir.Outcome("conn_err")] * 10
        r = simulator.run(pol, simulator.scenario_fn(seq),
                        connect_timeout_s=0.5)
        # retries=2 -> 3 physical attempts, inter-retry sleeps 0 + 0.5
        self.assertEqual(r.attempts, 3)
        self.assertGreaterEqual(r.elapsed_s, 3 * 0.55 + 0.5)

    def test_httpx_status_not_retried(self):
        pol = ir.Policy(name="h", layers=(
            ir.Layer(name="httpx", kind="httpx", max_attempts=3,
                     predicate=ir.RetryPredicate(
                         on_connect=True, on_read=False, on_other=False),
                     backoff=ir.BackoffSpec(kind="httpcore_exp",
                                            base_s=0.5)),))
        seq = [ir.Outcome("status", status=503)] * 5
        r = simulator.run(pol, simulator.scenario_fn(seq))
        self.assertEqual(r.attempts, 1)


class TestExplore(unittest.TestCase):
    def test_explore_respects_bound(self):
        pol = _pol_product()
        alpha = [ir.Outcome("status", status=503),
                 ir.Outcome("read_err"), ir.Outcome("success")]
        res = simulator.explore(pol, alpha, max_depth=16)
        self.assertEqual(res["max_attempts"], 12)

    def test_mixed_counters_exceed_single_class(self):
        # per-type counters: alternating failure classes consume
        # independent counters -> mixed max > single-class bound
        pol = ir.Policy(name="m", layers=(
            ir.u3_layer(total=None, connect=1, read=2, status=1,
                        status_forcelist=(503,)),))
        alpha = [ir.Outcome("conn_err"), ir.Outcome("read_err"),
                 ir.Outcome("status", status=503), ir.Outcome("success")]
        res = simulator.explore(pol, alpha, max_depth=8)
        # interleaving drains counters jointly: 2 conn + 3 read + 2 status
        # slots, run ends when any counter exhausts -> max 5
        self.assertEqual(res["max_attempts"], 5)
        single = analyzer.analyze(pol, ir.Outcome("read_err"),
                                  Contract(max_transport_attempts=99))
        self.assertEqual(single.attempts_max, 3.0)


if __name__ == "__main__":
    unittest.main()
