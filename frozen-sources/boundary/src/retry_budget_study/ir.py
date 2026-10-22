"""Policy intermediate representation (IR).

A Policy is a sequence of Layers, outermost first; the implicit innermost
node is the transport (one request send). Only constructs listed here are
modelable — anything else yields UNKNOWN downstream rather than a guess.

Layer kinds:
- "urllib3"   : per-type retry counters + status forcelist + Retry-After
- "tenacity"  : stop conditions + wait + exception/result predicates
- "budget"    : request-scoped shared budget (attempts + deadline)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

# --- outcome classes shared by analyzer & simulator --------------------
OUTCOMES = ("conn_err", "read_err", "other_err", "status", "success")


@dataclass(frozen=True)
class Outcome:
    """What a single transport attempt yields."""

    kind: str  # conn_err | read_err | other_err | status | success
    status: int = 0  # for kind="status"
    retry_after_s: float | None = None  # parsed Retry-After, if present
    delay_s: float = 0.0  # server-side hold before responding/starting

    @staticmethod
    def of(kind: str, **kw) -> "Outcome":
        return Outcome(kind, **kw)


@dataclass(frozen=True)
class TimeoutSpec:
    connect_s: float | None = None
    read_s: float | None = None


@dataclass(frozen=True)
class BackoffSpec:
    """Wait model between attempts.

    kind:
      none          -> 0s
      fixed         -> base_s before every retry
      urllib3_exp   -> documented [0, b, 2b, 4b, ...] capped by max_s
      tenacity_exp  -> min(base_s * 2**k, max_s), floored by min_s
      retry_after   -> server-provided value, capped by retry_after_max_s
      httpcore_exp  -> httpcore connect-retry backoff (factor base_s):
                       0 before the first retry, then base_s * 2**(k-2)
    """

    kind: str = "none"
    base_s: float = 0.0
    min_s: float = 0.0
    max_s: float | None = None

    def delay_for(
        self,
        retry_index: int,
        retry_after_s: float | None = None,
        ra_max_s: float | None = None,
    ) -> float:
        """Delay scheduled before attempt `retry_index` (>=1 => a retry).

        A respected Retry-After header overrides the configured backoff,
        matching urllib3 semantics (sleep_for_retry parses the header and
        returns early)."""
        if retry_after_s is not None:
            ra = retry_after_s
            if ra_max_s is not None:
                ra = min(ra, ra_max_s)
            return ra
        k = retry_index
        if self.kind == "none":
            d = 0.0
        elif self.kind == "fixed":
            d = self.base_s
        elif self.kind == "urllib3_exp":
            d = 0.0 if k <= 1 else self.base_s * (2.0 ** (k - 1))
        elif self.kind == "tenacity_exp":
            d = max(self.min_s, self.base_s * (2.0 ** k))
        elif self.kind == "httpcore_exp":
            d = 0.0 if k <= 1 else self.base_s * (2.0 ** (k - 2))
        else:
            d = 0.0
        if self.max_s is not None:
            d = min(d, self.max_s)
        return d


@dataclass(frozen=True)
class RetryPredicate:
    """Eligibility of a failed inner outcome at this layer."""

    on_connect: bool = True
    on_read: bool = True
    on_other: bool = True
    on_statuses: frozenset = frozenset()  # forcelist / result-status retries
    ra_statuses: frozenset = frozenset({413, 429, 503})
    methods: frozenset | None = None  # None => all methods eligible
    exception_names: tuple = ("*",)  # symbolic; "*" = any exception
    unknown: bool = False  # custom predicate code -> UNKNOWN


@dataclass(frozen=True)
class Layer:
    name: str
    kind: str  # urllib3 | tenacity | budget | status_check
    max_attempts: int | None  # normalized cap on inner invocations (None=uncapped)
    predicate: RetryPredicate = field(default_factory=RetryPredicate)
    backoff: BackoffSpec = field(default_factory=BackoffSpec)
    timeout: TimeoutSpec = field(default_factory=TimeoutSpec)
    soft_deadline_s: float | None = None  # stop_after_delay: checked BETWEEN
    respect_retry_after: bool = False
    retry_after_max_s: float | None = None
    raise_on_status: bool = True  # urllib3: exhausted status -> exception
    # urllib3 per-type counters: (connect, read, status, other);
    # None element = that counter unlimited. `total` is folded into
    # max_attempts (total takes precedence in real urllib3).
    type_counters: tuple | None = None
    # budget layer fields
    budget_attempts: int | None = None
    deadline_s: float | None = None  # hard deadline: gates NEW attempts

    def to_dict(self) -> dict:
        d = asdict(self)
        p = dict(d["predicate"])
        for k in ("on_statuses", "ra_statuses", "methods"):
            v = p[k]
            p[k] = None if v is None else sorted(v)
        d["predicate"] = p
        return d


@dataclass(frozen=True)
class Policy:
    layers: tuple  # outermost first; transport implicit at the bottom
    method: str = "GET"
    name: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "method": self.method,
            "layers": [l.to_dict() for l in self.layers],
        }


# --- canned layer builders used by the matrix --------------------------

U3_IDEMPOTENT = frozenset({"DELETE", "GET", "HEAD", "OPTIONS", "PUT", "TRACE"})


def u3_layer(
    total: int | None = 3,
    connect: int | None = None,
    read: int | None = None,
    status: int | None = None,
    other: int | None = None,
    status_forcelist: tuple = (),
    backoff_factor: float = 0.0,
    respect_retry_after: bool = True,
    raise_on_status: bool = True,
    allowed_methods: frozenset | None = U3_IDEMPOTENT,
    read_timeout_s: float | None = None,
    connect_timeout_s: float | None = None,
    retry_after_max_s: float | None = 21600.0,
    name: str = "urllib3",
) -> Layer:
    """urllib3 Retry: `total` counts RETRIES (not attempts) and takes
    precedence over per-type counters. Cap on transport attempts per
    invocation = total + 1 when total is set; when total is None the
    per-type counters bound attempts jointly via the simulator (no simple
    closed-form cap: counters decrement independently)."""
    cap = None if total is None else total + 1
    pred = RetryPredicate(
        on_connect=True if connect is None else connect > 0,
        on_read=True if read is None else read > 0,
        on_other=True if other is None else other > 0,
        on_statuses=frozenset(status_forcelist),
        methods=allowed_methods,
    )
    counters = (connect, read, status, other)
    return Layer(
        name=name,
        kind="urllib3",
        max_attempts=cap,
        predicate=pred,
        backoff=BackoffSpec(
            kind="urllib3_exp" if backoff_factor else "none",
            base_s=backoff_factor,
            max_s=120.0 if backoff_factor else None,
        ),
        timeout=TimeoutSpec(connect_s=connect_timeout_s, read_s=read_timeout_s),
        respect_retry_after=respect_retry_after,
        retry_after_max_s=retry_after_max_s,
        raise_on_status=raise_on_status,
        # real urllib3 decrements `total` AND the matching per-type
        # counter on every retry; exhaustion of EITHER stops retries,
        # so both are kept even when total is set
        type_counters=counters,
    )


def tenacity_layer(
    stop_attempts: int | None = None,
    stop_delay_s: float | None = None,
    wait_kind: str = "fixed",
    wait_s: float = 0.0,
    wait_max_s: float | None = None,
    on_statuses: tuple = (),
    on_exception: bool = True,
    name: str = "tenacity",
) -> Layer:
    pred = RetryPredicate(
        on_connect=on_exception,
        on_read=on_exception,
        on_other=on_exception,
        on_statuses=frozenset(on_statuses),
        methods=None,
    )
    kind = {"fixed": "fixed", "exponential": "tenacity_exp", "none": "none"}[
        wait_kind
    ]
    return Layer(
        name=name,
        kind="tenacity",
        max_attempts=stop_attempts,
        predicate=pred,
        backoff=BackoffSpec(kind=kind, base_s=wait_s, max_s=wait_max_s),
        soft_deadline_s=stop_delay_s,
    )


def budget_layer(
    max_attempts: int,
    deadline_s: float | None = None,
    name: str = "budget",
) -> Layer:
    return Layer(
        name=name,
        kind="budget",
        max_attempts=max_attempts,
        budget_attempts=max_attempts,
        deadline_s=deadline_s,
    )
