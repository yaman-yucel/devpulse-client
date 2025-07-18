"""Clean DevPulse application with simplified credential management.

This is the DevPulse client that integrates:
- Simple MAC address-based device enrollment and validation
- Dynamic configuration from server
- Clean tracking components
- Event pipeline architecture
"""

from __future__ import annotations

import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger

from ..config import DevPulseConfig, get_config_manager
from ..enroll.client.enrollment_client import CredentialClient, EnrollStatus, ValidateStatus
from ..orchestrator import PipelineConfig, PipelineOrchestrator, create_default_pipeline
from .events import ActivityEventType
from .trackers import ActivityTracker, HeartbeatTracker, ScreenshotTracker, WindowTracker


class DevPulseClient:
    """Clean DevPulse client with simplified credential management."""

    def __init__(
        self,
        server_url: str,
        # config_dir: Optional[Path] = None,
    ):
        """Initialize DevPulse client.

        Args:
            server_url: URL of the DevPulse server
            config_dir: Directory for configuration files
        """
        self.server_url = server_url
        # self.config_dir = config_dir

        # Initialize managers
        self.credential_client = CredentialClient(server_url=server_url)
        # self.config_manager = get_config_manager()

        # Initialize components (will be set up after enrollment/config)
        # self.config: Optional[DevPulseConfig] = None
        # self.pipeline: Optional[PipelineOrchestrator] = None
        # self.trackers: list = []

        # Set up signal handling
        # self._setup_signal_handling()

        logger.info(f"Initialized DevPulse client for server: {server_url}")

    def enroll(
        self,
        username: str,
        user_email: str,
    ) -> bool:
        logger.info(f"Starting enrollment for user: {username}")

        response = self.credential_client.enroll_device(username, user_email)
        print(response.message)
        if response.status == EnrollStatus.SUCCESS:
            logger.info(f"✅ {response.message}")

            return True
        else:
            logger.error(f"❌ Enrollment failed: {response.message}")
            return False

    def start(self, user_email: str) -> bool:
        """Start the DevPulse client.

        Returns:
            True if started successfully, False otherwise
        """
        logger.info("Starting DevPulse client...")

        try:
            # Validate credentials with server
            logger.info("Validating credentials with server...")
            response = self.credential_client.validate_credentials(user_email)
            if response.status == ValidateStatus.FAILURE:
                logger.error(f"❌ {response.message}")
                return False

            logger.info(f"✅ {response.message}")
            return True

        #     # Initialize configuration
        #     self.config = self.config_manager.load_config(
        #         server_url=self.server_url,
        #         username=credentials["user_id"],
        #         client_id=credentials["device_id"],
        #     )

        #     # Apply configuration from credentials
        #     credential_config = self.credential_manager.get_config()
        #     if credential_config:
        #         # Convert credential config to the format expected by config manager
        #         enrollment_config = {
        #             "api_key": credentials["api_key"],
        #             "batch_max_size": credential_config.get("batch_max_events", 100),
        #             "batch_max_age_seconds": credential_config.get("batch_max_interval_ms", 1000) // 1000,
        #             "heartbeat_interval": credential_config.get("heartbeat_interval_s", 5),
        #         }
        #         self.config_manager.apply_enrollment(enrollment_config)

        #     # Validate configuration
        #     is_valid, errors = self.config_manager.validate_config()
        #     if not is_valid:
        #         logger.error(f"Invalid configuration: {errors}")
        #         return False

        #     # Create pipeline
        #     pipeline_config = self._create_pipeline_config()
        #     self.pipeline = PipelineOrchestrator(pipeline_config)

        #     # Start pipeline
        #     if not self.pipeline.start():
        #         logger.error("Failed to start event pipeline")
        #         return False

        #     # Initialize tracking components
        #     self._initialize_trackers()

        #     logger.info("DevPulse client started successfully")
        #     return True

        except Exception as e:
            logger.error(f"Failed to start client: {e}")
            return False


#     def run(self) -> bool:
#         """Run the main tracking loop.

#         Returns:
#             True if completed successfully, False if errors occurred
#         """
#         if not self.config or not self.pipeline:
#             logger.error("Client not properly initialized. Call start() first.")
#             return False

#         logger.info("Starting main tracking loop...")

#         try:
#             # Initialize trackers
#             for tracker in self.trackers:
#                 if hasattr(tracker, "initialize"):
#                     tracker.initialize()

#             # Main loop
#             while True:
#                 # Run all trackers
#                 for tracker in self.trackers:
#                     try:
#                         tracker.tick()
#                     except Exception as e:
#                         logger.error(f"Error in tracker {type(tracker).__name__}: {e}")

#                 # Sleep for the configured delay
#                 if self.config.tracker.system_run_delay > 0:
#                     time.sleep(self.config.tracker.system_run_delay)

#         except KeyboardInterrupt:
#             logger.info("Received keyboard interrupt")
#             return True
#         except Exception as e:
#             logger.error(f"Error in main loop: {e}")
#             return False
#         finally:
#             self._shutdown()
#             return True

#     def stop(self) -> None:
#         """Stop the DevPulse client gracefully."""
#         logger.info("Stopping DevPulse client...")
#         self._shutdown()

#     def get_status(self) -> dict:
#         """Get client status information.

#         Returns:
#             Dictionary with status information
#         """
#         status = {
#             "enrolled": self.credential_manager.is_enrolled(),
#             "running": self.pipeline is not None and self.pipeline._running if self.pipeline else False,
#             "server_url": self.server_url,
#         }

#         # Add device information if enrolled
#         if status["enrolled"]:
#             device_id, user_id = self.credential_manager.get_device_info()
#             status.update(
#                 {
#                     "device_id": device_id,
#                     "user_id": user_id,
#                 }
#             )

#         # Add pipeline stats if running
#         if self.pipeline:
#             status["pipeline_stats"] = self.pipeline.get_pipeline_stats()

#         return status

#     def force_sync(self) -> bool:
#         """Force synchronization of pending events.

#         Returns:
#             True if successful, False otherwise
#         """
#         if self.pipeline:
#             return self.pipeline.force_batch_send()
#         return False

#     def _create_pipeline_config(self) -> PipelineConfig:
#         """Create pipeline configuration from current config."""
#         if not self.config:
#             raise RuntimeError("No configuration loaded")

#         return PipelineConfig(
#             client_id=self.config.client_id,
#             username=self.config.username,
#             api_base_url=self.config.server_url,
#             api_key=self.config.api_key,
#             queue_config=self.config.get_queue_config(),
#             wal_config=self.config.get_wal_config(),
#             batcher_config=self.config.get_batcher_config(),
#             sender_config=self.config.get_sender_config(),
#             stats_report_interval=self.config.pipeline.stats_report_interval,
#         )

#     def _initialize_trackers(self) -> None:
#         """Initialize tracking components."""
#         if not self.config or not self.pipeline:
#             raise RuntimeError("Configuration or pipeline not available")

#         # Get event producer for trackers
#         event_producer = self.pipeline.get_producer("trackers")

#         # Create tracking components
#         self.trackers = [
#             HeartbeatTracker(self.config, event_producer),
#             ActivityTracker(self.config, event_producer),
#             WindowTracker(self.config, event_producer),
#             ScreenshotTracker(self.config, event_producer),
#         ]

#         logger.info(f"Initialized {len(self.trackers)} tracking components")

#     def _setup_signal_handling(self) -> None:
#         """Set up signal handlers for graceful shutdown."""

#         def signal_handler(signum, frame):
#             signal_name = signal.Signals(signum).name
#             logger.info(f"Received {signal_name} signal, shutting down...")
#             self._shutdown()
#             sys.exit(0)

#         # Register signal handlers
#         signal.signal(signal.SIGINT, signal_handler)
#         signal.signal(signal.SIGTERM, signal_handler)

#         # Windows-specific
#         if hasattr(signal, "SIGBREAK"):
#             signal.signal(signal.SIGBREAK, signal_handler)

#     def _shutdown(self) -> None:
#         """Perform graceful shutdown."""
#         logger.info("Performing graceful shutdown...")

#         try:
#             # Shutdown trackers
#             for tracker in self.trackers:
#                 if hasattr(tracker, "shutdown"):
#                     try:
#                         tracker.shutdown()
#                     except Exception as e:
#                         logger.error(f"Error shutting down tracker: {e}")

#             # Shutdown pipeline
#             if self.pipeline:
#                 self.pipeline.stop()

#             logger.info("Shutdown complete")

#         except Exception as e:
#             logger.error(f"Error during shutdown: {e}")


# def create_devpulse_client(server_url: str, **kwargs) -> DevPulseClient:
#     """Create a DevPulse client with default configuration.

#     Args:
#         server_url: URL of the DevPulse server
#         **kwargs: Additional arguments for DevPulseClient

#     Returns:
#         Configured DevPulse client
#     """
#     return DevPulseClient(server_url=server_url, **kwargs)
