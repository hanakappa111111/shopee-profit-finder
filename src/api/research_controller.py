"""Research controller — routes for keyword search and result display.

All pipeline logic is delegated to ``run_research_pipeline(keyword)`` which
remains completely unmodified.  This controller only:
  1. accepts user input
  2. launches the pipeline in a background thread (non-blocking)
  3. polls for completion
  4. renders results via Jinja2 templates
"""

from __future__ import annotations

import time
import uuid
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

logger = logging.getLogger("shopee.web")

# ── Background job registry ───────────────────────────────────────────────────

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="pipeline")


@dataclass
class PipelineJob:
    """Tracks one background pipeline execution."""

    job_id: str
    keyword: str
    status: str = "running"          # running | completed | error
    started_at: datetime = field(default_factory=datetime.utcnow)
    elapsed: float = 0.0
    report: Optional[Any] = None     # PipelineReport once finished
    error_message: str = ""
    future: Optional[Future] = field(default=None, repr=False)


# In-memory store — lightweight, no extra DB tables needed.
# Only keeps the last 50 jobs to avoid unbounded growth.
_jobs: Dict[str, PipelineJob] = {}
_MAX_JOBS = 50


def _run_pipeline_safe(keyword: str, job: PipelineJob) -> None:
    """Execute the pipeline in a worker thread.  Never raises."""
    try:
        import os
        if os.environ.get("DEMO_MODE", "").lower() == "true":
            from src.research_pipeline.demo import run_demo_pipeline
            report = run_demo_pipeline(keyword)
        else:
            from src.research_pipeline.pipeline import run_research_pipeline
            report = run_research_pipeline(keyword)
        job.report = report
        job.status = "completed"
    except Exception as exc:
        logger.exception(f"Pipeline failed for keyword={keyword!r}")
        job.error_message = str(exc)
        job.status = "error"
    finally:
        job.elapsed = time.time() - job.started_at.timestamp()


def _submit_job(keyword: str) -> PipelineJob:
    """Submit a pipeline run to the thread pool and return the tracking job."""
    # Evict oldest jobs if at capacity
    if len(_jobs) >= _MAX_JOBS:
        oldest_key = next(iter(_jobs))
        del _jobs[oldest_key]

    job_id = uuid.uuid4().hex[:12]
    job = PipelineJob(job_id=job_id, keyword=keyword)
    job.future = _executor.submit(_run_pipeline_safe, keyword, job)
    _jobs[job_id] = job
    return job


# ── Routes ────────────────────────────────────────────────────────────────────

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Landing page with search form."""
    templates = request.app.state.templates
    return templates.TemplateResponse("index.html", {
        "request": request,
        "jobs": list(reversed(_jobs.values())),
    })


@router.post("/research", response_class=HTMLResponse)
async def start_research(request: Request, keyword: str = Form(...)):
    """Accept a keyword, launch the pipeline in the background, redirect to
    the polling page."""
    keyword = keyword.strip()
    templates = request.app.state.templates

    if not keyword:
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": "Please enter a keyword.",
            "jobs": list(reversed(_jobs.values())),
        })

    job = _submit_job(keyword)
    logger.info(f"Job {job.job_id} started for keyword={keyword!r}")

    # Return the loading partial (HTMX will swap it in)
    return templates.TemplateResponse("_loading.html", {
        "request": request,
        "job": job,
    })


@router.get("/research/{job_id}/status", response_class=HTMLResponse)
async def poll_status(request: Request, job_id: str):
    """HTMX polling endpoint — returns either the loading spinner or final
    results partial depending on job state."""
    templates = request.app.state.templates
    job = _jobs.get(job_id)

    if job is None:
        return templates.TemplateResponse("_error.html", {
            "request": request,
            "error": f"Job {job_id} not found.",
        })

    if job.status == "running":
        elapsed = time.time() - job.started_at.timestamp()
        return templates.TemplateResponse("_loading.html", {
            "request": request,
            "job": job,
            "elapsed": round(elapsed, 1),
        })

    if job.status == "error":
        return templates.TemplateResponse("_error.html", {
            "request": request,
            "error": job.error_message or "Unknown pipeline error.",
            "keyword": job.keyword,
        })

    # status == "completed"
    report = job.report
    return templates.TemplateResponse("_results.html", {
        "request": request,
        "report": report,
        "results": report.results if report else [],
        "keyword": job.keyword,
        "elapsed": round(job.elapsed, 1),
    })


@router.get("/history", response_class=HTMLResponse)
async def history(request: Request):
    """Show past searches."""
    templates = request.app.state.templates
    completed = [j for j in reversed(_jobs.values()) if j.status == "completed"]
    return templates.TemplateResponse("index.html", {
        "request": request,
        "jobs": completed,
    })
