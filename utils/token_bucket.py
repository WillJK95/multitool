# multitool/utils/token_bucket.py
"""Token bucket implementation for API rate limiting."""

import threading
import time
from typing import Optional

from ..utils.helpers import log_message


class TokenBucket:
    """
    Thread-safe token bucket for rate limiting API requests.
    
    Implements a token bucket algorithm where tokens are consumed for each
    request and refilled at a constant rate. When no tokens are available,
    the consume() method blocks until tokens become available.
    
    Attributes:
        capacity: Maximum number of tokens the bucket can hold
        refill_rate: Rate at which tokens are added (tokens per second)
    """
    
    def __init__(self, capacity: int, refill_rate: float):
        """
        Initialize the token bucket.
        
        Args:
            capacity: Maximum tokens the bucket can hold
            refill_rate: Tokens added per second
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill_time = time.monotonic()
        self.lock = threading.Lock()
    
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
    
    def update_params(self, capacity: int, refill_rate: float) -> None:
        """
        Update bucket parameters at runtime (thread-safe).

        Args:
            capacity: New maximum tokens the bucket can hold
            refill_rate: New tokens added per second
        """
        with self.lock:
            self.capacity = capacity
            self.refill_rate = refill_rate
            self.tokens = min(self.tokens, capacity)

    def sync_from_headers(self, headers: dict) -> None:
        """
        Sync bucket state from Companies House rate limit response headers.

        Reads X-Ratelimit-Limit, X-Ratelimit-Remain, X-Ratelimit-Reset, and
        X-Ratelimit-Window to align the local token bucket with the server's
        actual rate limit state.

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
                # Update capacity if the server reports a different limit
                if limit is not None:
                    server_limit = int(limit)
                    if server_limit != self.capacity:
                        self.capacity = server_limit
                        log_message(
                            f"Rate limit capacity updated from server: {server_limit}"
                        )

                # Update refill rate from the window if provided
                if window is not None and limit is not None:
                    window_seconds = self._parse_window(window)
                    if window_seconds > 0:
                        server_limit = int(limit)
                        new_refill = server_limit / window_seconds
                        if abs(new_refill - self.refill_rate) > 0.01:
                            self.refill_rate = new_refill
                            log_message(
                                f"Refill rate updated from server: "
                                f"{new_refill:.4f} tokens/s "
                                f"({server_limit}/{window_seconds}s)"
                            )

                # Set available tokens to match the server's remaining count,
                # but never exceed capacity
                self.tokens = min(remain, self.capacity)
                self.last_refill_time = time.monotonic()

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
