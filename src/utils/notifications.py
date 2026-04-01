"""Notification system for high-profit alerts.

Supports:

* **Discord Webhook** — sends embed messages to a channel.
* **LINE Notify** — sends text messages to a LINE group/user.

Both are optional.  If the corresponding token/URL is not set in
settings/.env, the notifier silently returns without error.

Usage::

    from src.utils.notifications import notify_profitable_results
    notify_profitable_results(report)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List

import requests

from src.config.settings import settings
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.research_pipeline.pipeline import PipelineReport, ResearchResult


# ── Discord ──────────────────────────────────────────────────────────────────


def _send_discord(webhook_url: str, report: "PipelineReport") -> None:
    """Post an embed message to a Discord channel via webhook."""
    if not report.results:
        return

    # Build top-5 summary
    lines = []
    for i, r in enumerate(report.results[:5], 1):
        lines.append(
            f"**{i}. {r.product_name[:50]}**\n"
            f"   Profit: ¥{r.estimated_profit_jpy:,.0f} | "
            f"ROI: {r.roi_percent:.0f}%\n"
            f"   [Supplier]({r.supplier_url[:100]})"
        )

    description = "\n\n".join(lines)
    if len(report.results) > 5:
        description += f"\n\n... and {len(report.results) - 5} more"

    payload = {
        "embeds": [
            {
                "title": f"Shopee Research: {report.keyword!r}",
                "description": description,
                "color": 0x00CC66,  # green
                "footer": {
                    "text": (
                        f"Scraped {report.products_scraped} | "
                        f"Matched {report.matches_found} | "
                        f"Profitable {report.profitable_count} | "
                        f"{report.elapsed_seconds:.0f}s"
                    )
                },
            }
        ]
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            logger.info("[Notify] Discord message sent")
        else:
            logger.warning(f"[Notify] Discord returned {resp.status_code}")
    except Exception as exc:
        logger.warning(f"[Notify] Discord send failed: {exc}")


# ── LINE Notify ──────────────────────────────────────────────────────────────


def _send_line(token: str, report: "PipelineReport") -> None:
    """Send a text notification via LINE Notify API."""
    if not report.results:
        return

    lines = [f"[Shopee Research] {report.keyword}"]
    lines.append(
        f"Scraped: {report.products_scraped} | "
        f"Profitable: {report.profitable_count}"
    )
    lines.append("")

    for i, r in enumerate(report.results[:5], 1):
        lines.append(
            f"{i}. {r.product_name[:40]}\n"
            f"   Profit: ¥{r.estimated_profit_jpy:,.0f} "
            f"ROI: {r.roi_percent:.0f}%"
        )

    if len(report.results) > 5:
        lines.append(f"\n+{len(report.results) - 5} more")

    message = "\n".join(lines)

    try:
        resp = requests.post(
            "https://notify-api.line.me/api/notify",
            headers={"Authorization": f"Bearer {token}"},
            data={"message": message},
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info("[Notify] LINE message sent")
        else:
            logger.warning(f"[Notify] LINE returned {resp.status_code}")
    except Exception as exc:
        logger.warning(f"[Notify] LINE send failed: {exc}")


# ── Public API ───────────────────────────────────────────────────────────────


def notify_profitable_results(report: "PipelineReport") -> None:
    """Send notifications for a completed research report.

    Checks ``settings.DISCORD_WEBHOOK_URL`` and ``settings.LINE_NOTIFY_TOKEN``
    to decide which channels to notify.  If neither is set, does nothing.
    """
    discord_url = getattr(settings, "DISCORD_WEBHOOK_URL", "")
    line_token = getattr(settings, "LINE_NOTIFY_TOKEN", "")

    if not report.results:
        return

    if discord_url:
        _send_discord(discord_url, report)

    if line_token:
        _send_line(line_token, report)


def notify_text(message: str) -> None:
    """Send a plain text notification to all configured channels."""
    discord_url = getattr(settings, "DISCORD_WEBHOOK_URL", "")
    line_token = getattr(settings, "LINE_NOTIFY_TOKEN", "")

    if discord_url:
        try:
            requests.post(
                discord_url,
                json={"content": message},
                timeout=10,
            )
        except Exception:
            pass

    if line_token:
        try:
            requests.post(
                "https://notify-api.line.me/api/notify",
                headers={"Authorization": f"Bearer {line_token}"},
                data={"message": message},
                timeout=10,
            )
        except Exception:
            pass
