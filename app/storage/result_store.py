from typing import Any, Dict, Optional

class ResultStore:
    """
    Persists and retrieves the outputs/results of executed jobs.
    
    Future Responsibilities:
    - Support relational databases (SQLite/PostgreSQL) and key-value stores (Redis) for persistence.
    - Implement data retention/cleanup policies (e.g., TTL for expired results).
    - Thread-safe storage updates from concurrent workers.
    - Detailed query interface for retrieving job runs and logs.
    """
    def __init__(self):
        # Initializing an in-memory storage placeholder
        self.store: Dict[str, Any] = {}

    def save_result(self, job_id: str, result: Any) -> None:
        """Saves a job execution result."""
        self.store[job_id] = result

    def get_result(self, job_id: str) -> Optional[Any]:
        """Retrieves the result of a specific job."""
        return self.store.get(job_id)

    def get_all_results(self) -> Dict[str, Any]:
        """Retrieves all stored results."""
        return self.store
