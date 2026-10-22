"""Event and ledger infrastructure.

Five distinct concepts are tracked, each with its own id space:

- logical operation  : one caller-visible unit of work (op_id)
- wrapper invocation : one call of the inner stack by a retry layer
- transport attempt  : one HTTP request send at the lowest layer
- server receipt     : request fully received by the fault server
- completed effect   : receipt whose scenario marks effect=apply

Client-side events are the ground truth for attempts; ServerLedger
receipts are the ground truth for what the server actually saw.
"""

from __future__ import annotations

import itertools
import json
import threading
import time
from dataclasses import dataclass, field

_op_counter = itertools.count(1)


def new_op_id(prefix: str = "op") -> str:
    return f"{prefix}-{next(_op_counter):04d}"


@dataclass
class Event:
    t_mono: float
    t_wall: float
    op_id: str
    layer: str
    kind: str
    data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "t_mono": round(self.t_mono, 6),
            "t_wall": round(self.t_wall, 6),
            "op_id": self.op_id,
            "layer": self.layer,
            "kind": self.kind,
            "data": self.data,
        }


class OperationLedger:
    """Per-operation client-side event log (thread-safe)."""

    def __init__(self, op_id: str):
        self.op_id = op_id
        self.events: list[Event] = []
        self._lock = threading.Lock()
        self.t0_mono = time.monotonic()

    def record(self, layer: str, kind: str, **data) -> None:
        ev = Event(time.monotonic(), time.time(), self.op_id, layer, kind, data)
        with self._lock:
            self.events.append(ev)

    def elapsed_s(self) -> float:
        return time.monotonic() - self.t0_mono

    # --- derived views -------------------------------------------------
    def count(self, kind: str, layer: str | None = None) -> int:
        with self._lock:
            return sum(
                1
                for e in self.events
                if e.kind == kind and (layer is None or e.layer == layer)
            )

    def kinds(self, kind: str) -> list[Event]:
        with self._lock:
            return [e for e in self.events if e.kind == kind]

    def to_json(self) -> str:
        with self._lock:
            return "\n".join(json.dumps(e.to_dict()) for e in self.events)


class RequestBudget:
    """Existing-best-practice request-scoped budget (baseline C).

    A small object carried through layers: a bounded number of transport
    attempts plus an absolute deadline. try_consume() is called at every
    retry decision point; after the deadline no NEW attempt may start.
    In-flight attempts are not cancelled (blocking I/O cannot be).
    """

    def __init__(self, max_attempts: int, deadline_s: float | None = None):
        self.remaining = max_attempts
        self.deadline_mono = (
            time.monotonic() + deadline_s if deadline_s is not None else None
        )
        self._lock = threading.Lock()
        self.refusals: list[dict] = []

    def try_consume(self, op_id: str, layer: str) -> bool:
        with self._lock:
            now = time.monotonic()
            if self.deadline_mono is not None and now >= self.deadline_mono:
                self.refusals.append(
                    {"op_id": op_id, "layer": layer, "reason": "deadline"}
                )
                return False
            if self.remaining <= 0:
                self.refusals.append(
                    {"op_id": op_id, "layer": layer, "reason": "attempts"}
                )
                return False
            self.remaining -= 1
            return True

    def deadline_passed(self) -> bool:
        return (
            self.deadline_mono is not None
            and time.monotonic() >= self.deadline_mono
        )


class BudgetExhausted(Exception):
    """Raised inside a layer when the shared budget refuses a new attempt."""

    def __init__(self, reason: str):
        super().__init__(f"request budget exhausted: {reason}")
        self.reason = reason


class ServerLedger:
    """Fault-server side record; the only ground truth for receipts/effects."""

    def __init__(self):
        self._lock = threading.Lock()
        self.receipts: dict[str, list[dict]] = {}

    def record_receipt(self, op_id: str, receipt: dict) -> int:
        with self._lock:
            lst = self.receipts.setdefault(op_id, [])
            idx = len(lst) + 1
            receipt = dict(receipt)
            receipt["receipt_idx"] = idx
            receipt["t_wall"] = time.time()
            lst.append(receipt)
            return idx

    def update_last(self, op_id: str, **fields) -> None:
        with self._lock:
            self.receipts[op_id][-1].update(fields)

    def update_receipt(self, op_id: str, receipt_idx: int, **fields) -> None:
        """Update the originating request even when later receipts overlap."""
        with self._lock:
            if receipt_idx < 1 or receipt_idx > len(self.receipts[op_id]):
                raise IndexError(receipt_idx)
            self.receipts[op_id][receipt_idx - 1].update(fields)

    def op(self, op_id: str) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.receipts.get(op_id, [])]

    def counts(self, op_id: str) -> dict:
        rs = self.op(op_id)
        return {
            "receipts": len(rs),
            "effects": sum(1 for r in rs if r.get("effect") == "apply"),
        }

    def to_dict(self) -> dict:
        with self._lock:
            return {k: [dict(r) for r in v] for k, v in self.receipts.items()}
