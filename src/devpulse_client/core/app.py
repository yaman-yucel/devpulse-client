import time

from loguru import logger

from .config.logger_config import setup_logging
from .config.tracker_settings import tracker_settings
from .core import ActivityStateTask, HeartbeatTask, ScreenshotTask, SignalHandler, WindowTrackerTask
from .db.connect import test_database_connection
from .db.event_store import EventStore
from .tables.activity_table import ActivityEventType


class DevPulseApp:
    """Main application class for the activity tracker."""

    SUPPORTED_SYSTEMS: set[str] = {"windows", "darwin", "linux"}

    def __init__(self) -> None:
        """Initialize the activity tracker with all tasks and signal handling."""
        # Configure logging first, before any other operations
        setup_logging()

        self.window_tracker = WindowTrackerTask(interval=tracker_settings.WINDOW_EVENT_INTERVAL)
        self.activity_state_task = ActivityStateTask()
        self.heartbeat_task = HeartbeatTask(interval=tracker_settings.HEARTBEAT_EVERY)
        self.screenshot_task = ScreenshotTask(interval=tracker_settings.SCREENSHOT_INTERVAL)

        self.tasks: list = [
            self.heartbeat_task,
            self.screenshot_task,
            self.window_tracker,
            self.activity_state_task,
        ]

        self.signal_handler = SignalHandler()
        self.signal_handler.register_cleanup(self._cleanup_tasks)

    def _cleanup_tasks(self) -> None:
        """Perform cleanup on all tasks during shutdown."""
        now = time.time()
        logger.info("Completing any active window events...")
        self.window_tracker.shutdown(now)
        self.heartbeat_task.shutdown(now)  # Last final heartbeat. Can be used for state recovery.
        # self.screenshot_task.shutdown(now) #Do I need a screenshot at shutdown? Probably not.
        # self.activity_state_task.shutdown(now) #I log shutdown event in the main loop.

    def run(self) -> bool:
        """Start the activity tracker and run the main loop."""
        logger.info("Starting DevPulse")

        if not self.system_pre_run_check():
            return False

        EventStore.log_event(ActivityEventType.STARTED)

        try:
            while True:
                for t in self.tasks:
                    t.tick()
                if tracker_settings.SYSTEM_RUN_DELAY > 0:
                    time.sleep(tracker_settings.SYSTEM_RUN_DELAY)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            return False
        finally:
            if self.signal_handler.is_signal_received():  # sys.exit(0) used to break try loops.
                logger.info(f"Stopping tracker gracefully due to {self.signal_handler.received_signal} signal.")
            else:
                logger.info("Stopping tracker gracefully due to normal application exit")
                self._cleanup_tasks()
                EventStore.log_event(ActivityEventType.NORMAL_SHUTDOWN)
            return True

    def system_pre_run_check(self) -> bool:
        """Check if the system is supported."""
        if tracker_settings.system not in self.SUPPORTED_SYSTEMS:
            logger.error(f"Unsupported system: {tracker_settings.system}")
            return False

        # Test database connection before starting the application
        logger.info("Testing database connection...")
        if not test_database_connection():
            logger.error("Failed to connect to database. Exiting application.")
            return False

        return True
