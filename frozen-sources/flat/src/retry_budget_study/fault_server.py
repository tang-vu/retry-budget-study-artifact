"""Deterministic loopback fault server.

All requests go to 127.0.0.1 only. A per-cell *scenario* maps each
receipt index (counted per X-Op-Id) to a deterministic action:

- {"respond": 200, "body": "ok"}                       immediate status
- {"respond": 503, "headers": {"Retry-After": "1"}}    status + headers
- {"delay_s": 0.4, "then": {...}}                      hold, then act
- {"drop": true}                                     read fully, apply
                                                     effect if marked,
                                                     close w/o response
- {"effect": "apply"|"none"}                         completed-effect flag

Connect-refusal scenarios are served by DeadPort (a bound-then-closed
socket), producing genuine ECONNREFUSED with zero receipts.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .ledger import ServerLedger

OP_HEADER = "X-Op-Id"
CELL_HEADER = "X-Cell-Id"


def resolve_action(scenario: dict, receipt_idx: int) -> dict:
    """Deterministic action for a receipt index.

    scenario["plan"] is a list; entries beyond the plan use
    scenario["default"] (default: respond 200).
    """
    plan = scenario.get("plan", [])
    if receipt_idx <= len(plan):
        return plan[receipt_idx - 1]
    return scenario.get("default", {"respond": 200, "body": "ok"})


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive; pool reuse is exercised

    def setup(self):
        super().setup()
        # Bound idle keep-alive reads as well as incomplete request bodies.
        # This does not interrupt a scenario's Python delay/effect handler.
        self.request.settimeout(2.0)

    # silence default logging
    def log_message(self, *args):  # noqa: D102
        pass

    def _read_body(self) -> bytes:
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def _serve(self, method: str):
        server = self.server.fault_server  # type: ignore[attr-defined]
        op_id = self.headers.get(OP_HEADER, "op-unknown")
        cell_id = self.headers.get(CELL_HEADER, "cell-unknown")
        body = self._read_body()  # full receipt before acting
        idx = server.ledger.record_receipt(
            op_id,
            {
                "cell_id": cell_id,
                "method": method,
                "path": self.path,
                "body_len": len(body),
                "t_mono": time.monotonic(),
            },
        )
        action = resolve_action(server.scenario, idx)
        server.ledger.update_receipt(op_id, idx, action=action)

        delay = float(action.get("delay_s", 0.0))
        if delay:
            # deterministic hold: this IS the scenario, not a racy sleep
            time.sleep(delay)

        inner = action.get("then", action)
        effect = action.get("effect", inner.get("effect", "none"))
        effect = "apply" if effect == "apply" else "none"
        if effect == "apply":
            server.effect_state.apply(op_id)
        server.ledger.update_receipt(op_id, idx, effect=effect)
        if inner.get("drop"):
            # response loss after receipt: close without responding
            server.ledger.update_receipt(op_id, idx, responded=False)
            self.close_connection = True
            return

        status = int(inner.get("respond", 200))
        payload = inner.get("body", "ok").encode()
        try:
            self.send_response(status)
            for k, v in inner.get("headers", {}).items():
                self.send_header(k, str(v))
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if method != "HEAD":
                self.wfile.write(payload)
            server.ledger.update_receipt(op_id, idx, responded=True, status=status)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            # client gave up mid-delay (read timeout) before we could
            # respond — that is the scenario, not a harness error
            server.ledger.update_receipt(op_id, idx, responded=False)

    do_GET = lambda self: self._serve("GET")
    do_POST = lambda self: self._serve("POST")
    do_PUT = lambda self: self._serve("PUT")
    do_DELETE = lambda self: self._serve("DELETE")
    do_HEAD = lambda self: self._serve("HEAD")


class EffectState:
    """Actual loopback counter mutation, separate from receipt bookkeeping.

    This cross-check detects ledger corruption; it is not an independent
    implementation of the failure scenario or an external service.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._counts = {}

    def apply(self, op_id):
        with self._lock:
            self._counts[op_id] = self._counts.get(op_id, 0) + 1

    def snapshot(self):
        with self._lock:
            return dict(self._counts)


class CompletionTracker:
    """Track accepted handlers, including those not yet scheduled by the OS."""

    def __init__(self):
        self._condition = threading.Condition()
        self.pending = 0

    def enter(self):
        with self._condition:
            self.pending += 1

    def leave(self):
        with self._condition:
            if self.pending <= 0:
                raise RuntimeError("unbalanced completion tracker")
            self.pending -= 1
            self._condition.notify_all()

    def drain(self, timeout_s):
        if timeout_s < 0:
            raise ValueError("negative drain timeout")
        with self._condition:
            return self._condition.wait_for(lambda: self.pending == 0, timeout_s)


class _QuietHTTPServer(ThreadingHTTPServer):
    """Clients abandoning connections mid-scenario (read timeout, drop)
    is expected behavior — do not spew tracebacks to stderr."""

    def handle_error(self, request, client_address):  # noqa: D102
        pass

    def process_request(self, request, client_address):
        self.fault_server.completion.enter()
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.fault_server.completion.leave()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.fault_server.completion.leave()


class FaultServer:
    """Context-managed deterministic loopback server."""

    def __init__(self, scenario: dict, ledger: ServerLedger | None = None,
                 drain_timeout_s: float = 5.0):
        if drain_timeout_s < 0:
            raise ValueError("negative drain timeout")
        self.scenario = scenario
        self.ledger = ledger or ServerLedger()
        self.effect_state = EffectState()
        self.completion = CompletionTracker()
        self.drain_timeout_s = drain_timeout_s
        self.drain_complete = None
        self._httpd = _QuietHTTPServer(("127.0.0.1", 0), _Handler)
        self._httpd.fault_server = self  # type: ignore[attr-defined]
        self._httpd.daemon_threads = True
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True
        )

    @property
    def url(self) -> str:
        host, port = self._httpd.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> "FaultServer":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)
        # shutdown stops acceptance, not daemon request handlers. Drain only
        # after the acceptance loop has stopped so no new handler can race in.
        self.drain_complete = (not self._thread.is_alive()
                               and self.completion.drain(self.drain_timeout_s))


class DeadPort:
    """A bound-then-closed loopback port: genuine connect-refused target."""

    def __init__(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        self.port = s.getsockname()[1]
        s.close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"
