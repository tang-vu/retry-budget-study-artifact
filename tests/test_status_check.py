"""Application-boundary semantics; no transport requests."""
import unittest
from retry_budget_study import analyzer, ir, simulator
from retry_budget_study.runner import policy_from_spec


class StatusCheckTests(unittest.TestCase):
    def policy(self, method, checked=True):
        layers = [ir.tenacity_layer(stop_attempts=3, wait_s=0),
                  ir.Layer('status_check', 'status_check', 1)] if checked else [
                      ir.tenacity_layer(stop_attempts=3, wait_s=0)]
        layers.append(ir.u3_layer(total=3, status_forcelist=(503,), raise_on_status=False))
        return ir.Policy(tuple(layers), method=method)

    def test_check_changes_exception_routing_for_both_methods(self):
        for method, plain, checked in [('GET', 4, 12), ('POST', 1, 3)]:
            for enabled, want in [(False, plain), (True, checked)]:
                with self.subTest(method=method, checked=enabled):
                    policy = self.policy(method, enabled)
                    bound = analyzer.analyze(policy, ir.Outcome('status', status=503),
                                             analyzer.Contract(max_transport_attempts=4))
                    self.assertEqual(bound.attempts_max, want)
                    result = simulator.run(policy, lambda _: ir.Outcome('status', status=503))
                    self.assertEqual(result.attempts, want)

    def test_success_is_not_changed(self):
        policy = self.policy('GET')
        result = simulator.run(policy, lambda _: ir.Outcome('success'))
        self.assertEqual((result.attempts, result.signal), (1, 'success'))

    def test_transient_recovery_and_shared_cap_remain_distinct(self):
        p = self.policy('GET')
        seq = [ir.Outcome('status', status=503)] * 4 + [ir.Outcome('success')]
        result = simulator.run(p, simulator.scenario_fn(seq))
        capped = ir.Policy((ir.budget_layer(4),) + p.layers, method='GET')
        limited = simulator.run(capped, simulator.scenario_fn(seq))
        self.assertEqual((result.attempts, result.signal), (5, 'success'))
        self.assertEqual(limited.attempts, 4)
        self.assertNotEqual(limited.signal, 'success')

    def test_matrix_translation_preserves_boundary(self):
        p = policy_from_spec('checked', {'layers': [
            {'kind': 'tenacity', 'stop_attempts': 3},
            {'kind': 'status_check'}, {'kind': 'urllib3', 'total': 3}]}, 'GET')
        self.assertEqual(p.layers[1].kind, 'status_check')
