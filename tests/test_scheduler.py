import sys
import os
import unittest

# Append the project root to sys.path to ensure modules in 'app' can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.models.job import Job
from app.queue.job_queue import JobQueue
from app.storage.result_store import ResultStore
from app.scheduler.scheduler import Scheduler

class TestScheduler(unittest.TestCase):
    def test_job_initialization(self):
        payload = {"task": "test"}
        job = Job(payload=payload, priority=2)
        self.assertEqual(job.status, Job.PENDING)
        self.assertEqual(job.priority, 2)
        self.assertEqual(job.payload, payload)
        self.assertIsNotNone(job.job_id)

    def test_queue_priority_sorting(self):
        queue = JobQueue()
        job_low = Job(payload={"name": "low"}, priority=0)
        job_high = Job(payload={"name": "high"}, priority=5)
        job_mid = Job(payload={"name": "mid"}, priority=3)

        queue.enqueue(job_low)
        queue.enqueue(job_high)
        queue.enqueue(job_mid)

        self.assertEqual(queue.size(), 3)
        
        # Check that highest priority is dequeued first
        first = queue.dequeue()
        self.assertEqual(first.priority, 5)
        
        second = queue.dequeue()
        self.assertEqual(second.priority, 3)
        
        third = queue.dequeue()
        self.assertEqual(third.priority, 0)
        
        self.assertTrue(queue.is_empty())

    def test_scheduler_sequential_execution(self):
        queue = JobQueue()
        store = ResultStore()
        scheduler = Scheduler(queue=queue, result_store=store, num_workers=1)

        job1 = Job(payload={"task": "1", "should_fail": False}, priority=1)
        job2 = Job(payload={"task": "2", "should_fail": True}, priority=2) # Will fail

        # We patch time.sleep inside workers to speed up test execution
        import app.workers.worker
        original_sleep = app.workers.worker.time.sleep
        app.workers.worker.time.sleep = lambda x: None

        try:
            queue.enqueue(job1)
            queue.enqueue(job2)

            scheduler.start()

            # Wait for the worker thread to process all jobs
            import time
            wait_start = time.time()
            while len(store.get_all_results()) < 2:
                time.sleep(0.01)
                if time.time() - wait_start > 5.0:
                    break

            scheduler.shutdown()

            results = store.get_all_results()
            self.assertEqual(len(results), 2)
            
            # job2 has higher priority, executed first
            res2 = results[job2.job_id]
            self.assertEqual(res2["status"], "FAILED")
            self.assertIn("Simulated task execution failure", res2["error"])

            res1 = results[job1.job_id]
            self.assertEqual(res1["status"], "SUCCESS")
        finally:
            app.workers.worker.time.sleep = original_sleep

if __name__ == "__main__":
    unittest.main()
