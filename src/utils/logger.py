"""
Structured logging with Loguru.
Provides rotating file logs + coloured stderr output.
"""

from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger as _logger

from src.config.settings import settings


def setup_logger() -> None:
    """Configure Loguru handlers. Call once at startup."""
    _logger.remove()

    # ── Human-readable stderr ─────────────────────────────────────────────────
    _logger.add(
        sys.stderr,
        level=settings.LOG_LEVEL,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
        enqueue=True,
    )

    # ── Daily rotating file ───────────────────────────────────────────────────
    _logger.add(
        str(settings.LOG_DIR / "system_{time:YYYY-MM-DD}.log"),
        level=settings.LOG_LEVEL,
        rotation=settings.LOG_ROTATION,
        retention=settings.LOG_RETENTION,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )

    # ── Errors only ───────────────────────────────────────────────────────────
    _logger.add(
        str(settings.LOG_DIR / "errors_{time:YYYY-MM-DD}.log"),
        level="ERROR",
        rotation=settings.LOG_ROTATION,
        retention=settings.LOG_RETENTION,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )


setup_logger()
logger = _logger
