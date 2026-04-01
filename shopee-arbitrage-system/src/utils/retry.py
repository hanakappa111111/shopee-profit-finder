"""
Retry utilities — decorators and context managers for handling transient failures.
Supports both synchronous and asynchronous functions.
"""

from __future__ import annotations

import asyncio
import functools
import time
from typing import Any, Callable, Optional, Tuple, Type, TypeVar, Union

from src.utils.logger import logger

F = TypeVar("F", bound=Callable[..., Any])


def retry(
    max_attempts: int = 3,
    backoff_seconds: float = 5.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    exponential: bool = True,
    on_retry: Optional[Callable[[int, Exception], None]] = None,
) -> Callable[[F], F]:
    """
    Decorator that retries a function on specified exceptions.

    Args:
        max_attempts:    Total number of attempts (including the first).
        backoff_seconds: Base wait time between retries.
        exceptions:      Tuple of exception types to catch and retry on.
        exponential:     If True, use exponential backoff (backoff * attempt).
        on_retry:        Optional callback called with (attempt, exception).

    Usage::

        @retry(max_attempts=3, backoff_seconds=2.0, exceptions=(requests.RequestException,))
        def fetch_data(url: str) -> dict:
            ...

        @retry(max_attempts=5, exponential=True)
        async def async_fetch(url: str) -> dict:
            ...
    """
    def decorator(func: F) -> F:
        is_async = asyncio.iscoroutinefunction(func)

        if is_async:
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                last_exc: Optional[Exception] = None
                for attempt in range(1, max_attempts + 1):
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as exc:
                        last_exc = exc
                        if attempt == max_attempts:
                            break
                        wait = backoff_seconds * (attempt if exponential else 1)
                        if on_retry:
                            on_retry(attempt, exc)
                        else:
                            logger.warning(
                                f"{func.__qualname__}: attempt {attempt}/{max_attempts} "
                                f"failed ({type(exc).__name__}: {exc}). "
                                f"Retrying in {wait:.1f}s…"
                            )
                        await asyncio.sleep(wait)
                logger.error(
                    f"{func.__qualname__}: all {max_attempts} attempts exhausted. "
                    f"Last error: {last_exc}"
                )
                raise last_exc  # type: ignore[misc]
            return async_wrapper  # type: ignore[return-value]

        else:
            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                last_exc: Optional[Exception] = None
                for attempt in range(1, max_attempts + 1):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as exc:
                        last_exc = exc
                        if attempt == max_attempts:
                            break
                        wait = backoff_seconds * (attempt if exponential else 1)
                        if on_retry:
                            on_retry(attempt, exc)
                        else:
                            logger.warning(
                                f"{func.__qualname__}: attempt {attempt}/{max_attempts} "
                                f"failed ({type(exc).__name__}: {exc}). "
                                f"Retrying in {wait:.1f}s…"
                            )
                        time.sleep(wait)
                logger.error(
                    f"{func.__qualname__}: all {max_attempts} attempts exhausted. "
                    f"Last error: {last_exc}"
                )
                raise last_exc  # type: ignore[misc]
            return sync_wrapper  # type: ignore[return-value]

    return decorator  # type: ignore[return-value]


def retry_on_network_error(
    max_attempts: int = 3,
    backoff_seconds: float = 5.0,
) -> Callable[[F], F]:
    """
    Convenience wrapper that retries on common network exceptions.
    Covers requests, httpx, and generic IOErrors.
    """
    import requests  # local import to avoid circular dependency

    network_exceptions: Tuple[Type[Exception], ...] = (
        requests.RequestException,
        ConnectionError,
        TimeoutError,
        OSError,
    )
    try:
        import httpx
        network_exceptions = network_exceptions + (httpx.HTTPError,)
    except ImportError:
        pass

    return retry(
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        exceptions=network_exceptions,
    )
