import threading
from typing import Optional
from app.models.job import Job
from app.utils.logger import logger

class JobQueue:
    """
    Manages the ingestion and distribution of jobs in a thread-safe manner
    using a Producer-Consumer pattern with a Condition Variable.

    Producer-Consumer Pattern:
    --------------------------
    A classic synchronization pattern where producer threads add data to a shared buffer,
    and consumer threads (Workers) retrieve and process that data. A condition variable
    coordinates access so that consumers go to sleep (no CPU consumption) when no work
    is available, and wake up when producers enqueue work.
    """
    def __init__(self):
        # Internal structure as requested
        self.jobs = []
        # Dead Letter Queue storing jobs that have exceeded max retries
        self.dead_letter_queue = []
        # Condition variable wraps an internal lock and provides wait() / notify() coordination
        self.condition = threading.Condition()

    def move_to_dlq(self, job: Job) -> None:
        """Moves a job to the dead letter queue in a thread-safe manner."""
        with self.condition:
            self.dead_letter_queue.append(job)

    def enqueue(self, job: Job) -> None:
        """
        Adds a job to the queue and notifies a waiting worker thread.
        
        notify():
        ---------
        Wakes up one thread waiting on this condition variable, if any.
        It does not release the lock; the lock is released when the 'with' block completes.
        """
        with self.condition:
            self.jobs.append(job)
            # Maintain priority sorting (highest priority first)
            self.jobs.sort(key=lambda j: (-j.priority, j.created_at))
            
            task_idx = job.payload.get('task_index', job.job_id)
            logger.info(f"Producer added Job {task_idx}")
            
            # Notify wakes up a single consumer thread waiting on this condition
            self.condition.notify()

    def dequeue(self, timeout: Optional[float] = None) -> Optional[Job]:
        """
        Removes and returns the highest priority job from the queue.
        If the queue is empty, the worker thread waits on the condition variable.
        
        wait():
        -------
        Releases the underlying condition lock and blocks the calling thread
        until it is awakened by a notify() call or until the timeout expires.
        Once awakened, the thread automatically re-acquires the lock.
        """
        with self.condition:
            while len(self.jobs) == 0:
                logger.info("Worker waiting for jobs...")
                # wait() blocks the thread and releases the lock, avoiding busy-waiting CPU waste
                signaled = self.condition.wait(timeout=timeout)
                if not signaled:
                    # Timeout occurred and queue is still empty; return None to let worker run loop checks
                    return None
            
            # Woken up successfully, lock is re-acquired, and a job is guaranteed to be present
            job = self.jobs.pop(0)
            logger.info("Worker awakened")
            return job

    def is_empty(self) -> bool:
        """Checks if the queue is empty under the condition variable lock."""
        with self.condition:
            return len(self.jobs) == 0

    def size(self) -> int:
        """Returns the size of the queue under the condition variable lock."""
        with self.condition:
            return len(self.jobs)
