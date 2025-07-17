"""Logger configuration for the tracker system."""

from loguru import logger

from .tracker_settings import tracker_settings


def setup_logging() -> None:
    """Configure loguru logger for both console and file output.

    Sets up structured logging with:
    - Console output for INFO+ levels with colored output
    - File output with rotation and retention based on settings
    - Configurable log level from settings
    """

    # Remove default loguru handler
    logger.remove()

    if tracker_settings.LOG_TO_CONSOLE:
        logger.add(
            sink=lambda msg: print(msg, end=""),
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level=tracker_settings.LOG_LEVEL,
            colorize=True,
        )

    # Add file handler if enabled
    if tracker_settings.LOG_TO_FILE:
        logger.add(
            sink=str(tracker_settings.log_file_path),
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level=tracker_settings.LOG_LEVEL,
            rotation=tracker_settings.LOG_ROTATION,
            retention=tracker_settings.LOG_RETENTION,
            compression="gz",  # Compress rotated logs
            enqueue=True,  # Thread-safe logging
        )

        logger.info(f"File logging enabled: {tracker_settings.log_file_path}")
        logger.info(f"Log level: {tracker_settings.LOG_LEVEL}")
        logger.info(f"Log rotation: {tracker_settings.LOG_ROTATION}")
        logger.info(f"Log retention: {tracker_settings.LOG_RETENTION}")
