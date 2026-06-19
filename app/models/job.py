from datetime import datetime
from typing import Any, Dict, Optional
import uuid

class Job:
    """
    Represents a task/job in the scheduling system.

    Attributes:
        job_id (str): Unique identifier for the job.
        payload (Dict[str, Any]): Task details and parameters.
        priority (int): Priority level of the job (higher numbers run first).
        status (str): Current status of the job (PENDING, RUNNING, COMPLETED, FAILED).
        created_at (datetime): Timestamp when the job was created.
    """
    
    # Status constants
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    def __init__(self, payload: Dict[str, Any], priority: int = 0, job_id: Optional[str] = None, max_retries: int = 3):
        self.job_id = job_id or str(uuid.uuid4())
        self.payload = payload
        self.priority = priority
        self.status = Job.PENDING
        self.created_at = datetime.utcnow()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        
        # Phase 7 attributes
        self.retry_count = 0
        self.max_retries = max_retries
        self.failure_reason: Optional[str] = None
        self.last_attempt_time: Optional[datetime] = None

    def __repr__(self) -> str:
        return f"<Job job_id={self.job_id} priority={self.priority} status={self.status} retries={self.retry_count}>"
