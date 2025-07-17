"""New DevPulse application using the pipeline architecture.

This is the redesigned version that uses the event pipeline:
Event Hooks → Queuer → WAL → Batcher → Sender → API

Instead of writing directly to the database, events flow through the pipeline.
"""

import time
from pathlib import Path
from typing import Optional

from loguru import logger

from ..config.logger_config import setup_logging
from ..config.tracker_settings import tracker_settings
from ..orchestrator import PipelineConfig, PipelineOrchestrator, create_default_pipeline
from .activity_state_tracker import ActivityStateTask
from .event_normalizer import LegacyEventAdapter
from .heartbeat import HeartbeatTask
from .screenshot_tracker import ScreenshotTask
from .signal_handler import SignalHandler
from .window_tracker import WindowTrackerTask


class DevPulseAppV2:
    """New DevPulse application using pipeline architecture."""

    SUPPORTED_SYSTEMS: set[str] = {"windows", "darwin", "linux"}

    def __init__(
        self,
        api_base_url: str = "http://localhost:8000",
        api_key: str = "your-api-key-here",
        username: Optional[str] = None,
        client_id: Optional[str] = None,
        wal_db_path: Optional[Path] = None,
    ) -> None:
        """Initialize the new DevPulse application.

        Args:
            api_base_url: Base URL of the ingest API
            api_key: API key for authentication
            username: Username for events (uses tracker_settings.user if not provided)
            client_id: Client identifier (auto-generated if not provided)
            wal_db_path: Path for WAL database (optional)
        """
        # Configure logging first
        setup_logging()

        # Set up pipeline configuration
        self.username = username or tracker_settings.user
        self.api_base_url = api_base_url
        self.api_key = api_key

        # Initialize pipeline orchestrator
        self.pipeline = create_default_pipeline(
            api_base_url=api_base_url,
            api_key=api_key,
            username=self.username,
            client_id=client_id,
            wal_db_path=wal_db_path,
        )

        # Create legacy adapter for existing components
        self.legacy_adapter = LegacyEventAdapter()

        # Initialize tracking components (they'll use the legacy adapter)
        self._init_tracking_components()

        # Set up signal handling
        self.signal_handler = SignalHandler()
        self.signal_handler.register_cleanup(self._cleanup_tasks)

        logger.info(f"Initialized DevPulse v2 - Client: {self.pipeline.config.client_id}")

    def _init_tracking_components(self) -> None:
        """Initialize the tracking components."""
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

    def _cleanup_tasks(self) -> None:
        """Perform cleanup on all tasks during shutdown."""
        now = time.time()
        logger.info("Completing any active tracking tasks...")

        try:
            self.window_tracker.shutdown(now)
            self.heartbeat_task.shutdown(now)

            # Force send any remaining events
            if self.pipeline:
                self.pipeline.force_batch_send()

        except Exception as e:
            logger.error(f"Error during task cleanup: {e}")

    def run(self) -> bool:
        """Start the DevPulse application and run the main loop."""
        logger.info("Starting DevPulse v2 with pipeline architecture")

        if not self.system_pre_run_check():
            return False

        try:
            # Start the pipeline
            if not self.pipeline.start():
                logger.error("Failed to start event pipeline")
                return False

            # Enable legacy adapter with pipeline producer
            producer = self.pipeline.get_producer("legacy-adapter")
            self.legacy_adapter.enable(producer)

            # Configure enhanced window tracker with pipeline components
            window_producer = self.pipeline.get_producer("window-tracker")
            self.window_tracker.configure(event_producer=window_producer, wal=self.pipeline.wal, username=self.username, client_id=self.pipeline.config.client_id)

            # Replace EventStore calls in tasks with legacy adapter
            self._patch_legacy_event_store()

            # Log startup event
            self.legacy_adapter.log_event("started")

            # Main tracking loop
            logger.info("Starting main tracking loop...")
            while True:
                for task in self.tasks:
                    task.tick()

                if tracker_settings.SYSTEM_RUN_DELAY > 0:
                    time.sleep(tracker_settings.SYSTEM_RUN_DELAY)

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
            return True
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            return False
        finally:
            self._shutdown_gracefully()
            return True

    def system_pre_run_check(self) -> bool:
        """Check if the system is supported and API is reachable."""
        if tracker_settings.system not in self.SUPPORTED_SYSTEMS:
            logger.error(f"Unsupported system: {tracker_settings.system}")
            return False

        # Test API connection (the pipeline will do this during start)
        logger.info("System pre-run check passed")
        return True

    def _patch_legacy_event_store(self) -> None:
        """Patch the legacy EventStore usage in tracking components."""
        # This is a temporary approach to redirect EventStore calls
        # In a full refactor, we'd modify the components directly

        # For now, we'll monkey-patch the EventStore import
        import sys
        from unittest.mock import MagicMock

        # Create a mock EventStore that delegates to our adapter
        mock_event_store = MagicMock()
        mock_event_store.log_event = self.legacy_adapter.log_event
        mock_event_store.heartbeat = self.legacy_adapter.heartbeat
        mock_event_store.log_window_event = self.legacy_adapter.log_window_event
        mock_event_store.create_incomplete_window_event = self.legacy_adapter.create_incomplete_window_event
        mock_event_store.complete_window_event = self.legacy_adapter.complete_window_event

        # Add dummy methods for compatibility
        mock_event_store.find_and_complete_incomplete_window_events = lambda: None

        # Replace EventStore in the modules
        for module_name in sys.modules:
            if "devpulse-client.core" in module_name or "tracker.db" in module_name:
                module = sys.modules[module_name]
                if hasattr(module, "EventStore"):
                    setattr(module, "EventStore", mock_event_store)
                    logger.debug(f"Patched EventStore in {module_name}")

    def _shutdown_gracefully(self) -> None:
        """Shutdown the application gracefully."""
        logger.info("Shutting down DevPulse v2...")

        try:
            # Handle signal-based shutdown
            if self.signal_handler.is_signal_received():
                signal_name = self.signal_handler.received_signal
                logger.info(f"Stopping tracker gracefully due to {signal_name} signal")

                # Log appropriate shutdown event
                if signal_name == "SIGINT":
                    self.legacy_adapter.log_event("user_interrupt")
                else:
                    self.legacy_adapter.log_event("system_shutdown")
            else:
                logger.info("Stopping tracker gracefully due to normal application exit")
                self._cleanup_tasks()
                self.legacy_adapter.log_event("normal_shutdown")

            # Stop the pipeline (this will send remaining events)
            if self.pipeline:
                self.pipeline.stop()

            # Disable legacy adapter
            self.legacy_adapter.disable()

            logger.info("DevPulse v2 shutdown complete")

        except Exception as e:
            logger.error(f"Error during graceful shutdown: {e}")

    def get_stats(self) -> dict:
        """Get comprehensive application statistics."""
        if not self.pipeline:
            return {"error": "Pipeline not initialized"}

        return self.pipeline.get_pipeline_stats()

    def force_sync(self) -> bool:
        """Force synchronization of pending events."""
        if not self.pipeline:
            return False

        return self.pipeline.force_batch_send()

    def cleanup_old_events(self, hours: int = 24) -> int:
        """Clean up old events from WAL."""
        if not self.pipeline:
            return 0

        return self.pipeline.cleanup_old_events(hours)


def create_devpulse_app(api_base_url: str = "http://localhost:8000", api_key: str = "your-api-key-here", username: Optional[str] = None, **kwargs) -> DevPulseAppV2:
    """Create a DevPulse application with default settings.

    Args:
        api_base_url: Base URL of the ingest API
        api_key: API key for authentication
        username: Username for events
        **kwargs: Additional arguments for DevPulseAppV2

    Returns:
        Configured DevPulse application
    """
    return DevPulseAppV2(api_base_url=api_base_url, api_key=api_key, username=username, **kwargs)


if __name__ == "__main__":
    # Example usage
    app = create_devpulse_app(api_base_url="http://localhost:8000", api_key="dev-api-key-123", username="test-user")

    success = app.run()
    if success:
        logger.info("Application completed successfully")
    else:
        logger.error("Application encountered errors")
