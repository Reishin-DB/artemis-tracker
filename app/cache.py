"""
Simple thread-safe TTL cache decorator.
"""

import functools
import threading
import time
from typing import Any, Callable


def cached(ttl_seconds: int):
    """
    Decorator that caches the return value of a function for *ttl_seconds*.
    Cache key is built from positional and keyword arguments.
    Thread-safe via threading.Lock.
    """
    def decorator(fn: Callable) -> Callable:
        _cache: dict[str, tuple[float, Any]] = {}
        _lock = threading.Lock()

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = _make_key(args, kwargs)
            now = time.monotonic()

            with _lock:
                if key in _cache:
                    ts, value = _cache[key]
                    if now - ts < ttl_seconds:
                        return value

            # Compute outside the lock so we don't block other readers
            result = fn(*args, **kwargs)

            with _lock:
                _cache[key] = (time.monotonic(), result)

            return result

        def clear_cache():
            """Manually clear the entire cache for this function."""
            with _lock:
                _cache.clear()

        wrapper.clear_cache = clear_cache  # type: ignore[attr-defined]
        return wrapper

    return decorator


def _make_key(args: tuple, kwargs: dict) -> str:
    """Build a hashable cache key from function arguments."""
    parts = [repr(a) for a in args]
    parts.extend(f"{k}={v!r}" for k, v in sorted(kwargs.items()))
    return "|".join(parts)
