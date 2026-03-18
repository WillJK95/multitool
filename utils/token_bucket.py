# multitool/utils/token_bucket.py
"""Token bucket implementation for API rate limiting."""

import threading
import time
from typing import Optional

from ..constants import SMOOTH_BURST_WINDOW_SECONDS, SMOOTH_SAFETY_MARGIN
from ..utils.helpers import log_message


class TokenBucket:
    """
    Thread-safe token bucket for rate limiting API requests.

    Implements a token bucket algorithm where tokens are consumed for each
    request and refilled at a constant rate. When no tokens are available,
    the consume() method blocks until tokens become available.

    Supports two pacing modes:
        - "smooth": Small rolling bucket with steady refill. Spreads
          requests evenly over time.
        - "burst": Bucket capacity equals the full server limit. Allows
          all available tokens to be consumed immediately.

    After the first API response, capacity and refill rate are driven
    entirely by server response headers via sync_from_headers().

    Attributes:
        capacity: Maximum number of tokens the bucket can hold
        refill_rate: Rate at which tokens are added (tokens per second)
        pacing_mode: "smooth" or "burst"
    """

    def __init__(self, capacity: int, refill_rate: float,
                 pacing_mode: str = "smooth"):
        """
        Initialize the token bucket.

        Args:
            capacity: Maximum tokens the bucket can hold
            refill_rate: Tokens added per second
            pacing_mode: "smooth" for steady throughput, "burst" for
                         maximum speed followed by wait
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.pacing_mode = pacing_mode
        self.last_refill_time = time.monotonic()
        self.lock = threading.Lock()

        # Cached server values (populated by sync_from_headers)
        self._server_limit: Optional[int] = None
        self._window_seconds: Optional[int] = None

    def _refill(self) -> None:
        """
        Refill tokens based on elapsed time.

        Called internally before consuming tokens to ensure the bucket
        reflects the correct number of available tokens.
        """
        now = time.monotonic()
        elapsed = now - self.last_refill_time
        tokens_to_add = elapsed * self.refill_rate

        if tokens_to_add > 0:
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill_time = now

    def consume(self, tokens_required: int = 1) -> bool:
        """
        Consume tokens from the bucket, blocking if necessary.

        If insufficient tokens are available, this method will block
        until enough tokens have been refilled.

        Args:
            tokens_required: Number of tokens to consume (default 1)

        Returns:
            True when tokens have been successfully consumed
        """
        with self.lock:
            self._refill()

            while self.tokens < tokens_required:
                # Calculate wait time for required tokens
                tokens_needed = tokens_required - self.tokens
                wait_time = tokens_needed / self.refill_rate

                # Release lock while waiting
                self.lock.release()
                time.sleep(wait_time)
                self.lock.acquire()

                self._refill()

            self.tokens -= tokens_required
            return True

    def try_consume(self, tokens_required: int = 1) -> bool:
        """
        Try to consume tokens without blocking.

        Args:
            tokens_required: Number of tokens to consume

        Returns:
            True if tokens were consumed, False if insufficient tokens
        """
        with self.lock:
            self._refill()

            if self.tokens >= tokens_required:
                self.tokens -= tokens_required
                return True
            return False

    def _apply_pacing(self) -> None:
        """
        Recalculate capacity and refill_rate from cached server data
        and the current pacing mode. Must be called under self.lock.
        """
        if self._server_limit is None or self._window_seconds is None:
            return

        if self.pacing_mode == "burst":
            self.capacity = self._server_limit
            self.refill_rate = self._server_limit / self._window_seconds
        else:
            raw_rate = self._server_limit / self._window_seconds
            self.refill_rate = raw_rate * SMOOTH_SAFETY_MARGIN
            self.capacity = max(10, int(self.refill_rate
                                        * SMOOTH_BURST_WINDOW_SECONDS))

    def update_pacing_mode(self, mode: str) -> None:
        """
        Switch pacing mode at runtime (thread-safe).

        If server data has been received via headers, capacity and
        refill rate are recalculated immediately.

        Args:
            mode: "smooth" or "burst"
        """
        with self.lock:
            self.pacing_mode = mode
            old_capacity = self.capacity
            self._apply_pacing()
            self.tokens = min(self.tokens, self.capacity)
            if self.capacity != old_capacity:
                log_message(
                    f"Pacing mode changed to '{mode}': "
                    f"capacity={self.capacity}, "
                    f"refill_rate={self.refill_rate:.4f}/s"
                )

    def sync_from_headers(self, headers: dict) -> None:
        """
        Sync bucket state from Companies House rate limit response headers.

        Reads X-Ratelimit-Limit, X-Ratelimit-Remain, X-Ratelimit-Reset, and
        X-Ratelimit-Window to align the local token bucket with the server's
        actual rate limit state. Capacity and refill rate are derived through
        the current pacing mode.

        Args:
            headers: Response headers dict (case-insensitive).
        """
        try:
            remain = headers.get("X-Ratelimit-Remain")
            limit = headers.get("X-Ratelimit-Limit")
            reset_epoch = headers.get("X-Ratelimit-Reset")
            window = headers.get("X-Ratelimit-Window")

            if remain is None or reset_epoch is None:
                return

            remain = int(remain)
            reset_epoch = int(reset_epoch)

            # Derive seconds until window resets
            seconds_until_reset = max(1, reset_epoch - int(time.time()))

            with self.lock:
                # Cache server values for pacing recalculations
                if limit is not None:
                    self._server_limit = int(limit)
                if window is not None:
                    parsed = self._parse_window(window)
                    if parsed > 0:
                        self._window_seconds = parsed

                # Recalculate capacity and refill rate via pacing mode
                if self._server_limit and self._window_seconds:
                    self._apply_pacing()

                # Set available tokens to match the server's remaining count,
                # but never exceed capacity
                self.tokens = min(remain, self.capacity)
                self.last_refill_time = time.monotonic()

                # Log noteworthy rate limit states
                if remain == 0:
                    log_message(
                        f"Rate limit depleted: 0/{self.capacity} requests "
                        f"remaining, window resets in {seconds_until_reset}s"
                    )
                elif self.capacity > 0 and remain < self.capacity * 0.1:
                    log_message(
                        f"Rate limit warning: {remain}/{self.capacity} "
                        f"requests remaining, window resets in "
                        f"{seconds_until_reset}s"
                    )

        except (ValueError, TypeError):
            # Malformed headers - ignore and keep local state
            pass

    def get_wait_from_reset(self, headers: dict) -> Optional[float]:
        """
        Calculate seconds to wait based on X-Ratelimit-Reset header.

        Returns:
            Seconds to wait until the rate limit window resets, or None if
            the header is missing/unparseable.
        """
        try:
            reset_epoch = headers.get("X-Ratelimit-Reset")
            if reset_epoch is None:
                return None
            return max(0.5, int(reset_epoch) - time.time())
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_window(window: str) -> int:
        """
        Parse a window string like '5m' into seconds.

        Supports 's' (seconds) and 'm' (minutes) suffixes.
        Returns 0 if the format is unrecognised.
        """
        window = window.strip()
        if window.endswith("m"):
            return int(window[:-1]) * 60
        if window.endswith("s"):
            return int(window[:-1])
        try:
            return int(window)
        except ValueError:
            return 0

    @property
    def available_tokens(self) -> float:
        """Get the current number of available tokens."""
        with self.lock:
            self._refill()
            return self.tokens
