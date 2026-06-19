import logging
import os
import sys

def setup_logger(name: str = "concurrent_task_scheduler", level: int = logging.INFO) -> logging.Logger:
    """
    Configures and returns a logger instance.
    
    Future Responsibilities:
    - Support structured JSON logging for external monitoring dashboards (e.g., ELK stack, Prometheus).
    - File rotating handler config to avoid unlimited log file growth.
    - Dynamic log level configuration at runtime.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)
        
        # Formatter including timestamp, log level, thread name, and message
        formatter = logging.Formatter(
            '[%(asctime)s] [%(threadName)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File Handler (logs to a directory relative to the runner)
        log_dir = "logs"
        try:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            file_handler = logging.FileHandler(os.path.join(log_dir, "app.log"))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            # Fallback if logs directory cannot be written to
            pass
            
    return logger

# Global logger instance
logger = setup_logger()
