"""Conservative cumulative quota, persisted before every physical attempt.

The caller holds the exclusive study lock. A crash may charge an unsent
attempt; it cannot silently refund attempts. Persistence cost is instrumentation
overhead and must be disclosed in timing results.
"""
import json
import os
import threading
from pathlib import Path


class PhysicalQuotaExhausted(RuntimeError):
    pass


class PhysicalAttemptQuota:
    def __init__(self, path, historical_used, cap):
        self.path = Path(path)
        self.cap = cap
        self._lock = threading.Lock()
        recorded = json.loads(self.path.read_text())['used'] if self.path.exists() else 0
        self.used = max(historical_used, recorded)
        self.initial = self.used
        self.reserved_until = None
        if self.used > cap:
            raise PhysicalQuotaExhausted('historical usage exceeds cap')

    def consume(self):
        with self._lock:
            ceiling = self.cap if self.reserved_until is None else self.reserved_until
            if self.used >= ceiling:
                raise PhysicalQuotaExhausted('cumulative physical-attempt cap reached')
            candidate = self.used + 1
            if self.reserved_until is None:
                self._persist(candidate)
            self.used = candidate

    def _persist(self, used):
        tmp = self.path.with_suffix('.tmp')
        with tmp.open('w', encoding='utf-8') as f:
            json.dump({'used': used, 'cap': self.cap}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def reserve(self, n):
        """Persist worst-case charge before measurement; no disk I/O in consume.

        Abrupt process loss keeps the entire reservation charged. A live caller
        may settle unused units only after all sequential operations have ended.
        """
        with self._lock:
            if n < 0 or self.used + n > self.cap or self.reserved_until is not None:
                raise PhysicalQuotaExhausted('invalid quota reservation')
            self._persist(self.used + n)
            self.reserved_until = self.used + n

    def settle(self):
        with self._lock:
            self._persist(self.used)
            self.reserved_until = None

    @property
    def charged(self):
        return self.used - self.initial
