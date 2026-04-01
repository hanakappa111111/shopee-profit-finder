"""Tornado web server for Shopee Profit Finder.
Usage: python run_server.py
Then open http://localhost:8000
"""
from __future__ import annotations

import sys
import os
import time
import uuid
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import OrderedDict

import tornado.ioloop
import tornado.web
import tornado.escape
from jinja2 import Environment, FileSystemLoader, select_autoescape

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR  = BASE_DIR / "src"
WEB_DIR  = SRC_DIR  / "web"
TMPL_DIR = WEB_DIR  / "templates"
STAT_DIR = WEB_DIR  / "static"
sys.path.insert(0, str(BASE_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("shopee.server")

# ── Jinja2 env ───────────────────────────────────────────────────────────────
_jinja = Environment(
    loader=FileSystemLoader(str(TMPL_DIR)),
    autoescape=select_autoescape(["html"]),
)

def render(template_name: str, **ctx) -> str:
    return _jinja.get_template(template_name).render(**ctx)

# ── Background job registry ──────────────────────────────────────────────────
_executor  = ThreadPoolExecutor(max_workers=2, thread_name_prefix="pipeline")
_jobs: OrderedDict[str, "PipelineJob"] = OrderedDict()
_MAX_JOBS  = 50
_jobs_lock = threading.Lock()

@dataclass
class PipelineJob:
    job_id:   str
    keyword:  str
    status:   str   = "running"
    started_at: float = field(default_factory=time.time)
    elapsed:  float = 0.0
    report:   Optional[Any] = None
    error_message: str = ""

def _run_pipeline_safe(keyword: str, job: PipelineJob) -> None:
    try:
        from src.research_pipeline.pipeline import run_research_pipeline
        report = run_research_pipeline(keyword)
        job.report = report
        job.status  = "completed"
    except Exception as exc:
        logger.exception(f"Pipeline failed for keyword={keyword!r}")
        job.error_message = str(exc)
        job.status = "error"
    finally:
        job.elapsed = round(time.time() - job.started_at, 1)

def submit_job(keyword: str) -> PipelineJob:
    with _jobs_lock:
        if len(_jobs) >= _MAX_JOBS:
            _jobs.popitem(last=False)
        job_id = uuid.uuid4().hex[:12]
        job = PipelineJob(job_id=job_id, keyword=keyword)
        _jobs[job_id] = job
    _executor.submit(_run_pipeline_safe, keyword, job)
    return job

# ── Handlers ─────────────────────────────────────────────────────────────────
class BaseHandler(tornado.web.RequestHandler):
    def render_template(self, name: str, **ctx):
        self.set_header("Content-Type", "text/html; charset=utf-8")
        self.write(render(name, **ctx))

class IndexHandler(BaseHandler):
    def get(self):
        with _jobs_lock:
            jobs = list(reversed(list(_jobs.values())))
        self.render_template("index.html", jobs=jobs)

class ResearchHandler(BaseHandler):
    def post(self):
        keyword = (self.get_argument("keyword", "") or "").strip()
        if not keyword:
            with _jobs_lock:
                jobs = list(reversed(list(_jobs.values())))
            self.render_template("index.html", jobs=jobs, error="キーワードを入力してください。")
            return
        job = submit_job(keyword)
        logger.info(f"Job {job.job_id} started — keyword={keyword!r}")
        self.render_template("_loading.html", job=job, elapsed=0.0)

class StatusHandler(BaseHandler):
    def get(self, job_id: str):
        job = _jobs.get(job_id)
        if job is None:
            self.render_template("_error.html", error=f"Job {job_id} が見つかりません。", keyword="")
            return
        if job.status == "running":
            elapsed = round(time.time() - job.started_at, 1)
            self.render_template("_loading.html", job=job, elapsed=elapsed)
            return
        if job.status == "error":
            self.render_template(
                "_error.html",
                error=job.error_message or "不明なエラーが発生しました。",
                keyword=job.keyword,
            )
            return
        # completed
        report = job.report
        self.render_template(
            "_results.html",
            report=report,
            results=report.results if report else [],
            keyword=job.keyword,
            elapsed=job.elapsed,
        )

class HistoryHandler(BaseHandler):
    def get(self):
        with _jobs_lock:
            completed = [j for j in reversed(list(_jobs.values())) if j.status == "completed"]
        self.render_template("index.html", jobs=completed)

# ── App ───────────────────────────────────────────────────────────────────────
def make_app() -> tornado.web.Application:
    static_path = str(STAT_DIR)
    return tornado.web.Application([
        (r"/",                        IndexHandler),
        (r"/research",                ResearchHandler),
        (r"/research/([^/]+)/status", StatusHandler),
        (r"/history",                 HistoryHandler),
        (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": static_path}),
    ], debug=False)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app = make_app()
    app.listen(port)
    logger.info(f"Shopee Profit Finder running at http://0.0.0.0:{port}")
    tornado.ioloop.IOLoop.current().start()
