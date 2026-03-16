# multitool/utils/token_bucket.py
"""Token bucket implementation for API rate limiting."""

import threading
import time
from typing import Optional


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

    @property
    def available_tokens(self) -> float:
        """Get the current number of available tokens."""
        with self.lock:
            self._refill()
            return self.tokens
