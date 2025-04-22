import sys
import os
from loguru import logger

# Import settings without creating circular dependencies
try:
    from src.config.config import settings
    log_level = settings.logging.level
    log_format = settings.logging.format
except ImportError:
    # Use default settings if not importable
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_format = os.getenv("LOG_FORMAT", "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

"""
Configure logging for the application.

This module sets up the logger with appropriate formatting and log level.
In a multi-broker environment, it's important to have detailed logs for
troubleshooting cluster issues.
"""

# Remove the default handler
logger.remove()

# Configure loguru logger with custom format
logger.add(
    sys.stdout,
    format=log_format,
    level=log_level,
    backtrace=True,  # Show traceback for errors
    diagnose=True,   # Enable exception diagnosis
    enqueue=True     # Thread-safe logging
)

# Add file logging for persistent records in production
if os.getenv("ENVIRONMENT", "development").lower() == "production":
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Add a rotating file handler for production logs
    logger.add(
        "logs/app.log",
        rotation="10 MB",    # Rotate when the file reaches 10 MB
        retention="1 week",  # Keep logs for 1 week
        compression="zip",   # Compress rotated logs
        format=log_format,
        level=log_level,
        backtrace=True,
        diagnose=True,
        enqueue=True
    )
    
    # Add error-specific log file for easier monitoring
    logger.add(
        "logs/error.log",
        rotation="10 MB",
        retention="1 month",  # Keep error logs longer
        compression="zip",
        format=log_format,
        level="ERROR",
        backtrace=True,
        diagnose=True,
        filter=lambda record: record["level"].name == "ERROR" or record["level"].name == "CRITICAL",
        enqueue=True
    )

# Log application start
logger.info("Logger initialized")

# Export the configured logger
log = logger