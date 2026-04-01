"""FastAPI application — lightweight web UI for Shopee Profit Finder.

This module creates a thin HTTP layer on top of the existing research
pipeline.  It does NOT modify any pipeline internals, AI modules, or
database schema.

Start the server::

    uvicorn src.api.server:app --reload --host 0.0.0.0 --port 8000

Then open http://localhost:8000 in a browser.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.api.research_controller import router as research_router

# ── Paths ─────────────────────────────────────────────────────────────────────

_WEB_DIR = Path(__file__).resolve().parent.parent / "web"
_TEMPLATE_DIR = _WEB_DIR / "templates"
_STATIC_DIR = _WEB_DIR / "static"

# ── App factory ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Shopee Profit Finder",
    description="Browser UI for the Shopee ↔ Japan supplier arbitrage research pipeline.",
    version="1.0.0",
)

# Static files (CSS, JS, images)
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

# Jinja2 templates — shared with the controller via app.state
templates = Jinja2Templates(directory=str(_TEMPLATE_DIR))
app.state.templates = templates

# Register routes
app.include_router(research_router)
