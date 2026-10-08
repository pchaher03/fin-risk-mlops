"""
Structured JSON Logger Module.

Provides standard JSON logging compatible with local FastAPI runtimes
and distributed PySpark execution environments.
"""

import json
import logging
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Custom Formatter to format logs as structured JSON strings."""

    def format(self, record: logging.LogRecord) -> str:
        log_object = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "filename": record.filename,
            "lineno": record.lineno,
        }

        # Include exception information if available
        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_object)


def get_logger(name: str = "fin-risk-mlops") -> logging.Logger:
    """
    Configures and returns a structured JSON logger instance.

    Args:

        name (str): The name of the logger module.

    Returns:
        logging.Logger: Configured structured logger.
    """
    logger = logging.getLogger(name)

    # Avoid adding multiple handlers if already configured
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Output to stdout (compatible with Spark drivers, workers, and FastAPI stdout)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())

        logger.addHandler(handler)
        logger.propagate = False

    return logger


# Instantiate standard global logger
logger = get_logger()