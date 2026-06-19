"""
Phase 8 — Flask Monitoring Dashboard + Concurrent Task Scheduler
=================================================================
This file wires together every component built in Phases 1-7 and
exposes a live monitoring dashboard via Flask.

Architecture overview:
  ┌───────────────────────────────────────────────────────────┐
  │  main thread  →  Flask HTTP server (port 5000)            │
  │                                                           │
  │  background   →  Worker-1 … Worker-4 (daemon threads)    │
  │                  pulling jobs from the shared JobQueue    │
  └───────────────────────────────────────────────────────────┘

Shared state is stored in app.config["SCHEDULER_STATE"] so that
every Flask request handler can access live queue/worker metrics.
"""

import sys
import os
import threading
import time
import random

# ── Path setup so absolute imports from 'app' work ──────────────────────────
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from flask import Flask, render_template

from app.models.job import Job
from app.queue.job_queue import JobQueue
from app.storage.result_store import ResultStore
from app.scheduler.scheduler import Scheduler
from app.api.routes import api_bp
from app.utils.logger import logger


# ── Flask application factory ────────────────────────────────────────────────
def create_app(scheduler_state: dict) -> Flask:
    """
    Creates and configures the Flask application.
    
    scheduler_state dict is injected into Flask's config so that
    every Blueprint (api_bp) can read the live scheduler metrics
    without needing global variables.
    """
    app = Flask(__name__, template_folder="templates")
    app.config["SCHEDULER_STATE"] = scheduler_state

    # Register the REST API blueprint (routes: /jobs, /stats)
    app.register_blueprint(api_bp)

    # Dashboard root — serves the HTML page
    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    return app


# ── Pre-populate queue with demo jobs ────────────────────────────────────────
def seed_initial_jobs(state: dict, count: int = 10) -> None:
    """
    Enqueues `count` demonstration jobs on startup so the dashboard
    shows activity immediately without manual interaction.
    Jobs 1-7 succeed; jobs 8-10 are set to fail (triggering retry/DLQ).
    """
    logger.info(f"Seeding {count} initial demo jobs into the queue...")
    for i in range(1, count + 1):
        should_fail = (i > 7)               # last 3 jobs will fail
        priority    = random.randint(0, 4)

        with state["counter_lock"]:
            state["jobs_submitted"] += 1
            task_index = state["jobs_submitted"]

        payload = {
            "task_index":  task_index,
            "description": f"Demo task {task_index}",
            "should_fail": should_fail,
        }
        job = Job(payload=payload, priority=priority, max_retries=3)

        with state["registry_lock"]:
            state["job_registry"][job.job_id] = job

        state["queue"].enqueue(job)

    logger.info("Initial demo jobs enqueued.")


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 55)
    logger.info("Concurrent Task Scheduler — Phase 8")
    logger.info("Flask Monitoring Dashboard + Worker Pool")
    logger.info("=" * 55)

    # ── 1. Build shared infrastructure ──────────────────────────────────────
    job_queue    = JobQueue()
    result_store = ResultStore()
    scheduler    = Scheduler(queue=job_queue, result_store=result_store, num_workers=4)

    # Shared state dict injected into Flask config
    # All mutable state is protected by individual locks where needed.
    scheduler_state = {
        "queue":        job_queue,
        "result_store": result_store,
        "scheduler":    scheduler,

        # Job registry: job_id -> Job  (for GET /jobs/<id>)
        "job_registry": {},
        "registry_lock": threading.Lock(),

        # Monotonically increasing counter for human-friendly task_index
        "jobs_submitted": 0,
        "counter_lock":   threading.Lock(),
    }

    # ── 2. Start worker threads in the background ────────────────────────────
    scheduler.start()
    logger.info("Worker pool started (4 workers).")

    # ── 3. Seed demo jobs so the dashboard isn't empty on first load ─────────
    seed_initial_jobs(scheduler_state, count=10)

    # ── 4. Build Flask app ───────────────────────────────────────────────────
    app = create_app(scheduler_state)

    # ── 5. Launch Flask (this blocks the main thread) ────────────────────────
    logger.info("Starting Flask dashboard on http://127.0.0.1:5000")
    logger.info("Open your browser and navigate to: http://127.0.0.1:5000")
    logger.info("Press CTRL+C to stop.")

    try:
        # use_reloader=False is critical — the reloader forks the process,
        # which would create duplicate worker threads.
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down worker pool...")
        scheduler.shutdown()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
