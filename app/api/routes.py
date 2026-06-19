"""
Phase 8 - Flask REST API Routes
================================
Exposes three endpoints for the monitoring dashboard:

  POST /jobs         - Submit a new job to the scheduler.
  GET  /jobs/<id>    - Retrieve a specific job's status and result.
  GET  /stats        - Live metrics: queue size, active workers, throughput, etc.

These routes read from the shared, globally-initialised scheduler_state dict
that is populated in main.py before the Flask app starts.
"""
import time
import uuid
from datetime import datetime
from flask import Blueprint, jsonify, request, current_app
from app.models.job import Job

# Blueprint keeps routes decoupled from the Flask application instance.
api_bp = Blueprint("api", __name__)


# ─────────────────────────────────────────────
# Helper – safely access the shared state dict
# ─────────────────────────────────────────────
def _state():
    """Returns the scheduler_state dict stored in app.config."""
    return current_app.config["SCHEDULER_STATE"]


# ─────────────────────────────────────────────
# POST /jobs
# ─────────────────────────────────────────────
@api_bp.route("/jobs", methods=["POST"])
def submit_job():
    """
    Submit a new job to the queue.

    JSON body (all fields optional):
      {
        "priority":    1,
        "should_fail": false,
        "description": "my task"
      }
    """
    data = request.get_json(silent=True) or {}

    priority    = int(data.get("priority", 0))
    should_fail = bool(data.get("should_fail", False))
    description = str(data.get("description", "API submitted task"))

    state = _state()

    # Build a sequential task index from the jobs_submitted counter
    with state["counter_lock"]:
        state["jobs_submitted"] += 1
        task_index = state["jobs_submitted"]

    payload = {
        "task_index":  task_index,
        "description": description,
        "should_fail": should_fail,
    }

    job = Job(payload=payload, priority=priority, max_retries=3)

    # Track job in the global registry so GET /jobs/<id> can find it
    with state["registry_lock"]:
        state["job_registry"][job.job_id] = job

    state["queue"].enqueue(job)

    return jsonify({
        "job_id":      job.job_id,
        "task_index":  task_index,
        "priority":    priority,
        "status":      job.status,
        "created_at":  job.created_at.isoformat(),
    }), 201


# ─────────────────────────────────────────────
# GET /jobs/<job_id>
# ─────────────────────────────────────────────
@api_bp.route("/jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    """
    Return the current status and result for a specific job.
    """
    state = _state()

    with state["registry_lock"]:
        job = state["job_registry"].get(job_id)

    if job is None:
        return jsonify({"error": f"Job '{job_id}' not found"}), 404

    result = state["result_store"].get_result(job_id)

    return jsonify({
        "job_id":       job.job_id,
        "task_index":   job.payload.get("task_index"),
        "priority":     job.priority,
        "status":       job.status,
        "retry_count":  job.retry_count,
        "max_retries":  job.max_retries,
        "failure_reason": job.failure_reason,
        "created_at":   job.created_at.isoformat(),
        "started_at":   job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "result":       result,
    })


# ─────────────────────────────────────────────
# GET /stats
# ─────────────────────────────────────────────
@api_bp.route("/stats", methods=["GET"])
def get_stats():
    """
    Return live scheduler metrics used by the dashboard.
    
    Metrics:
      queue_size       - Jobs currently waiting in queue.
      active_workers   - Workers that are alive/running.
      completed_jobs   - Total jobs with COMPLETED status.
      failed_jobs      - Total jobs with FAILED status (before DLQ).
      dlq_jobs         - Total jobs moved to dead-letter queue.
      throughput       - Jobs completed in the last 60 seconds.
      total_submitted  - Total jobs ever submitted.
      jobs             - Snapshot list of all known jobs (for the table).
    """
    state = _state()

    # ── active workers ──────────────────────────────────────────────────────
    active_workers = sum(
        1 for w in state["scheduler"].workers
        if w.is_alive() and w.is_running
    )

    # ── job registry snapshot ───────────────────────────────────────────────
    with state["registry_lock"]:
        all_jobs = list(state["job_registry"].values())

    completed_jobs = sum(1 for j in all_jobs if j.status == Job.COMPLETED)
    failed_jobs    = sum(1 for j in all_jobs if j.status == Job.FAILED)

    # ── DLQ ─────────────────────────────────────────────────────────────────
    # Access dead_letter_queue via the condition lock inside JobQueue
    with state["queue"].condition:
        dlq_jobs = len(state["queue"].dead_letter_queue)

    # ── throughput (jobs completed in last 60 s) ─────────────────────────────
    now = datetime.utcnow().timestamp()
    throughput = sum(
        1 for j in all_jobs
        if j.status == Job.COMPLETED
        and j.completed_at is not None
        and (now - j.completed_at.timestamp()) <= 60
    )

    # ── per-job snapshot for dashboard table ────────────────────────────────
    jobs_snapshot = []
    for j in sorted(all_jobs, key=lambda x: x.created_at, reverse=True)[:50]:
        # Determine DLQ membership without re-acquiring condition (already done above)
        with state["queue"].condition:
            in_dlq = j in state["queue"].dead_letter_queue

        display_status = "DLQ" if in_dlq else j.status
        jobs_snapshot.append({
            "job_id":       j.job_id[:8],          # short id for display
            "task_index":   j.payload.get("task_index"),
            "priority":     j.priority,
            "status":       display_status,
            "retry_count":  j.retry_count,
            "created_at":   j.created_at.strftime("%H:%M:%S"),
        })

    return jsonify({
        "queue_size":      state["queue"].size(),
        "active_workers":  active_workers,
        "completed_jobs":  completed_jobs,
        "failed_jobs":     failed_jobs,
        "dlq_jobs":        dlq_jobs,
        "throughput":      throughput,
        "total_submitted": state["jobs_submitted"],
        "jobs":            jobs_snapshot,
    })
