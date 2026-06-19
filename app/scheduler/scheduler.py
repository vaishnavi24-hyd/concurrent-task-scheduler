from app.queue.job_queue import JobQueue
from app.storage.result_store import ResultStore
from app.workers.worker import Worker
from app.utils.logger import logger

class Scheduler:
    """
    Orchestrates the entire system, managing queues, workers, and execution flow.
    
    Future Responsibilities:
    - Cron/Interval scheduling (parsing cron expressions to trigger periodic jobs).
    - Dynamic worker pool scaling (spinning up/down threads based on load).
    - Graceful orchestration of system-wide shutdown.
    - Deadlock prevention and synchronization management.
    - Integration with a persistence layer to recover scheduled tasks on restart.
    """
    def __init__(self, queue: JobQueue, result_store: ResultStore, num_workers: int = 4, tracker = None):
        self.queue = queue
        self.result_store = result_store
        self.num_workers = num_workers
        self.tracker = tracker
        self.workers = [
            Worker(worker_id=f"Worker-{i+1}", queue=self.queue, result_store=self.result_store, tracker=self.tracker) 
            for i in range(num_workers)
        ]

    def start(self) -> None:
        """Starts all worker threads concurrently."""
        logger.info(f"Scheduler starting {self.num_workers} concurrent worker threads...")
        for worker in self.workers:
            worker.start()

    def shutdown(self) -> None:
        """Gracefully stops all worker threads and joins them."""
        logger.info("Scheduler shutting down workers...")
        for worker in self.workers:
            worker.stop()
        for worker in self.workers:
            if worker.is_alive():
                worker.join()
        logger.info("Scheduler successfully shut down.")
