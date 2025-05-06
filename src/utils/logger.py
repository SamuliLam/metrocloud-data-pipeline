import sys
import os
from pathlib import Path
from loguru import logger

# Import settings without creating circular dependencies
def get_settings():
    try:
        from src.config.config import settings
        return settings
    except ImportError:
        # Use default settings if not importable (during startup)
        import os
        return type('Settings', (), {
            'logging': type('LoggingSettings', (), {
                'level': os.getenv("LOG_LEVEL", "INFO"),
                'format': os.getenv("LOG_FORMAT", "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"),
                'file_logging_enabled': os.getenv("LOG_FILE_ENABLED", "False").lower() in ("true", "1", "yes"),
                'log_dir': os.getenv("LOG_DIR", "logs"),
                'max_size': os.getenv("LOG_MAX_SIZE", "10MB"),
                'retention': os.getenv("LOG_RETENTION", "1 week"),
                'compression': os.getenv("LOG_COMPRESSION", "zip"),
                'error_log_enabled': os.getenv("ERROR_LOG_ENABLED", "False").lower() in ("true", "1", "yes"),
                'error_log_retention': os.getenv("ERROR_LOG_RETENTION", "1 month")
            })
        })()
"""
Configure logging for the application.

This module sets up the logger with appropriate formatting and log level.
In a multi-broker environment, it's important to have detailed logs for
troubleshooting cluster issues.
"""

def initialize_logger():
    """
    Initialize and configure the logger based on settings.
    """
    settings = get_settings()

    # get log level and format from settings
    log_level = settings.logging.level
    log_format = settings.logging.format

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
    if settings.logging.file_logging_enabled:
        # Create logs directory if it doesn't exist
        log_dir = Path(settings.logging.log_dir)
        log_dir.mkdir(exist_ok=True, parents=True)
        
        # Add a rotating file handler for production logs
        logger.add(
            str(log_dir / "app.log"),
            rotation=settings.logging.max_size,    # Rotate based on configured size
            retention=settings.logging.retention,  # Keep logs for 1 week
            compression=settings.logging.compression,   # Compress rotated logs
            format=log_format,
            level=log_level,
            backtrace=True,
            diagnose=True,
            enqueue=True
        )
        
        # Add error-specific log file for easier monitoring
        if settings.logging.error_log_enabled:
            logger.add(
            str(log_dir / "error.log"),
            rotation=settings.logging.max_size,
            retention=settings.logging.error_log_retention,  # Keep error logs longer
            compression=settings.logging.compression,
            format=log_format,
            level="ERROR",
            backtrace=True,
            diagnose=True,
            filter=lambda record: record["level"].name == "ERROR" or record["level"].name == "CRITICAL",
            enqueue=True
        )
            
    # Log startup information
    environment = getattr(settings, 'environment', 'development')
    logger.info(f"Logger initialized in {environment} environment at {log_level} level")
    if settings.logging.file_logging_enabled:
        logger.info(f"File logging enabled in directory: {settings.logging.log_dir}")

# Initialize the logger
initialize_logger()

# Export the configured logger
log = logger