import random
import time
import threading
from datetime import datetime
from app.models.job import Job
from app.queue.job_queue import JobQueue
from app.storage.result_store import ResultStore
from app.utils.logger import logger

class Worker(threading.Thread):
    """
    Responsible for executing jobs pulled from the job queue.
    Runs concurrently as a background thread.
    """
    def __init__(self, worker_id: str, queue: JobQueue, result_store: ResultStore, tracker = None):
        super().__init__(name=worker_id)
        self.queue = queue
        self.result_store = result_store
        self.tracker = tracker
        self.is_running = False

    def run(self) -> None:
        """Continuously pulls jobs from the queue and executes them."""
        self.is_running = True
        while self.is_running:
            # We call dequeue with a timeout to allow checking the self.is_running flag
            # on shutdown, avoiding CPU busy-waiting.
            job = self.queue.dequeue(timeout=0.2)
            if job:
                self.process_job(job)

    def stop(self) -> None:
        """Stops the worker thread loop."""
        self.is_running = False

    def process_job(self, job: Job) -> None:
        """
        Executes a single job sequentially within this worker thread.
        - Mark status (RUNNING, then COMPLETED or FAILED).
        - Sleep between 1 and 3 seconds randomly (skipped if skip_sleep is True).
        - Save execution result to the result store.
        """
        task_idx = job.payload.get('task_index', job.job_id)
        logger.info(f"Worker processing Job {task_idx}")
        job.status = Job.RUNNING
        job.started_at = datetime.utcnow()

        # Simulated execution delay (1 to 3 seconds), unless skip_sleep is requested
        skip_sleep = job.payload.get("skip_sleep", False)
        sleep_duration = 0.0
        if not skip_sleep:
            sleep_duration = random.uniform(1.0, 3.0)
            time.sleep(sleep_duration)

        try:
            should_fail = job.payload.get("should_fail", False)
            if should_fail:
                raise ValueError("Simulated task execution failure")

            job.status = Job.COMPLETED
            result = {
                "status": "SUCCESS",
                "processed_by": self.name,
                "duration_seconds": round(sleep_duration, 2),
                "payload": job.payload
            }
            logger.info(f"Completed Job {task_idx}")
        except Exception as e:
            job.status = Job.FAILED
            job.failure_reason = str(e)
            job.last_attempt_time = datetime.utcnow()
            
            result = {
                "status": "FAILED",
                "processed_by": self.name,
                "duration_seconds": round(sleep_duration, 2),
                "error": str(e),
                "payload": job.payload
            }
            logger.info(f"Failed Job {task_idx}: {e}")

            # Phase 7 Retry & DLQ Routing
            if job.retry_count < job.max_retries:
                job.retry_count += 1
                backoff = 2 ** (job.retry_count - 1)
                logger.info(f"Job {task_idx} failed. Retrying (Attempt {job.retry_count}/{job.max_retries}) in {backoff}s...")
                
                # Asynchronously requeue after exponential backoff delay
                def delayed_requeue(j=job, delay=backoff):
                    time.sleep(delay)
                    j.status = Job.PENDING
                    self.queue.enqueue(j)
                
                threading.Thread(target=delayed_requeue, daemon=True).start()
            else:
                logger.warning(f"Job {task_idx} reached max retries ({job.max_retries}). Moving to Dead Letter Queue.")
                self.queue.move_to_dlq(job)

        job.completed_at = datetime.utcnow()
        self.result_store.save_result(job.job_id, result)

        # Increment shared counter if tracker is present
        if self.tracker:
            use_lock = job.payload.get("use_lock", False)
            if use_lock:
                self.tracker.increment_with_lock()
            else:
                self.tracker.increment_without_lock()
