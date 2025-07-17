"""Pipeline orchestrator for coordinating the DevPulse client event flow.

This module coordinates the entire event pipeline:
Event Hooks → Queuer → WAL → Batcher → Sender → API

It manages the lifecycle of all components and provides a unified interface
for the DevPulse client application.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from ..batcher import BatcherConfig, BatchProcessor, EventBatcher
from ..core.events import BaseEvent, EventBatch
from ..queuer import EventProducer, EventQueue, QueueConfig
from ..sender import HTTPSender, SenderConfig, create_default_sender
from ..wal import EventWAL, WALConfig


@dataclass
class PipelineConfig:
    """Configuration for the entire event pipeline."""

    # Client identification
    client_id: str = ""
    username: str = ""

    # API settings
    api_base_url: str = "http://localhost:8000"
    api_key: str = ""

    # Component configurations
    queue_config: QueueConfig = field(default_factory=QueueConfig)
    wal_config: WALConfig = field(default_factory=WALConfig)
    batcher_config: BatcherConfig = field(default_factory=BatcherConfig)
    sender_config: SenderConfig = field(default_factory=SenderConfig)

    # Pipeline settings
    enable_wal: bool = True
    enable_batch_recovery: bool = True
    stats_report_interval: int = 300  # Report stats every 5 minutes

    def __post_init__(self):
        """Set up derived configurations."""
        if not self.client_id:
            self.client_id = f"devpulse-client-{uuid.uuid4().hex[:8]}"

        # Configure sender with API settings
        self.sender_config.api_base_url = self.api_base_url
        self.sender_config.api_key = self.api_key
        self.sender_config.client_id = self.client_id


class PipelineOrchestrator:
    """Orchestrates the entire event processing pipeline."""

    def __init__(self, config: PipelineConfig = PipelineConfig()):
        """Initialize the pipeline orchestrator.

        Args:
            config: Pipeline configuration
        """
        self.config = config
        self._running = False
        self._stats_thread: Optional[threading.Thread] = None

        # Initialize components
        self._init_components()

        # Statistics
        self._start_time: Optional[datetime] = None
        self._last_stats_report: Optional[datetime] = None

    def _init_components(self) -> None:
        """Initialize all pipeline components."""
        logger.info("Initializing pipeline components...")

        # Initialize WAL (if enabled)
        self.wal: Optional[EventWAL] = None
        if self.config.enable_wal:
            self.wal = EventWAL(self.config.wal_config)
            logger.info("Initialized WAL")

        # Initialize event queue with WAL spill callback
        wal_spill_callback = None
        if self.wal:
            wal_spill_callback = self.wal.write_events

        self.queue = EventQueue(config=self.config.queue_config, wal_spill_callback=wal_spill_callback)
        logger.info("Initialized event queue")

        # Initialize HTTP sender
        self.sender = HTTPSender(self.config.sender_config)
        logger.info("Initialized HTTP sender")

        # Initialize batch processor (for WAL interaction)
        self.batch_processor = BatchProcessor(event_wal=self.wal, max_retries=3, retry_delay=1.0)

        # Initialize batcher with batch ready callback
        self.batcher = EventBatcher(config=self.config.batcher_config, event_queue=self.queue, event_wal=self.wal, batch_ready_callback=self._handle_batch_ready)
        logger.info("Initialized event batcher")

        # Create event producer for core components
        self.event_producer = EventProducer(queue=self.queue, component_name="orchestrator")

        logger.info("All pipeline components initialized")

    def start(self) -> bool:
        """Start the event processing pipeline.

        Returns:
            True if started successfully, False otherwise
        """
        if self._running:
            logger.warning("Pipeline is already running")
            return True

        try:
            logger.info("Starting DevPulse event pipeline...")
            self._start_time = datetime.now()

            # Perform crash recovery if WAL is enabled
            if self.wal and self.config.enable_batch_recovery:
                recovery_events = self._perform_crash_recovery()
                if recovery_events:
                    logger.info(f"Recovered {len(recovery_events)} events from previous session")

            # Test connection to API
            if not self._test_api_connection():
                logger.error("Failed to connect to API, aborting startup")
                return False

            # Start batcher (which will start processing events)
            self.batcher.start()

            # Start stats reporting thread
            self._running = True
            self._stats_thread = threading.Thread(target=self._stats_loop, daemon=True)
            self._stats_thread.start()

            # Log startup success
            logger.info(f"Pipeline started successfully - Client ID: {self.config.client_id}, API: {self.config.api_base_url}")

            return True

        except Exception as e:
            logger.error(f"Failed to start pipeline: {e}")
            return False

    def stop(self) -> bool:
        """Stop the event processing pipeline gracefully.

        Returns:
            True if stopped successfully, False otherwise
        """
        if not self._running:
            logger.warning("Pipeline is not running")
            return True

        try:
            logger.info("Stopping DevPulse event pipeline...")

            # Stop accepting new events
            self._running = False

            # Stop batcher (will send remaining batches)
            self.batcher.stop()

            # Process any remaining events in queue
            self._drain_queue()

            # Stop stats thread
            if self._stats_thread:
                self._stats_thread.join(timeout=5.0)

            # Close WAL
            if self.wal:
                self.wal.close()

            # Log final stats
            self._log_final_stats()

            logger.info("Pipeline stopped successfully")
            return True

        except Exception as e:
            logger.error(f"Error stopping pipeline: {e}")
            return False

    def emit_event(self, event: BaseEvent) -> bool:
        """Emit an event into the pipeline.

        Args:
            event: Event to emit

        Returns:
            True if accepted, False if rejected
        """
        if not self._running:
            logger.warning("Pipeline not running, dropping event")
            return False

        # Set client metadata
        event.username = self.config.username
        event.client_id = self.config.client_id

        return self.event_producer.emit(event)

    def get_producer(self, component_name: str) -> EventProducer:
        """Get an event producer for a component.

        Args:
            component_name: Name of the component

        Returns:
            Event producer configured for this pipeline
        """
        return EventProducer(queue=self.queue, component_name=component_name)

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics.

        Returns:
            Dictionary with pipeline statistics
        """
        stats = {
            "pipeline": {
                "running": self._running,
                "start_time": self._start_time.isoformat() if self._start_time else None,
                "uptime_seconds": ((datetime.now() - self._start_time).total_seconds() if self._start_time else 0),
                "client_id": self.config.client_id,
                "username": self.config.username,
            },
            "queue": self.queue.get_stats(),
            "batcher": self.batcher.get_stats(),
            "sender": self.sender.get_stats(),
        }

        if self.wal:
            stats["wal"] = self.wal.get_stats()

        return stats

    def force_batch_send(self) -> bool:
        """Force sending any pending batch immediately.

        Returns:
            True if successful, False otherwise
        """
        return self.batcher.force_batch_send()

    def cleanup_old_events(self, hours: int = 24) -> int:
        """Clean up old processed events from WAL.

        Args:
            hours: Remove events older than this many hours

        Returns:
            Number of events cleaned up
        """
        if self.wal:
            return self.wal.cleanup_processed_events(hours)
        return 0

    def _perform_crash_recovery(self) -> List[BaseEvent]:
        """Perform crash recovery using WAL capabilities.

        Returns:
            List of recovered events
        """
        recovery_events = []

        try:
            if not self.wal:
                return recovery_events

            logger.info("Performing crash recovery...")

            # Recover incomplete window sessions
            window_recovery_events = self.wal.recover_incomplete_window_sessions()
            recovery_events.extend(window_recovery_events)

            # Clean up stale sessions
            stale_count = self.wal.cleanup_stale_sessions()
            if stale_count > 0:
                logger.info(f"Cleaned up {stale_count} stale window sessions during recovery")

            # If we have recovery events, add them to the queue for processing
            if recovery_events:
                # Add recovered events to WAL for normal processing
                success = self.wal.write_events(recovery_events)
                if success:
                    logger.info(f"Added {len(recovery_events)} recovered events to processing queue")
                else:
                    logger.error("Failed to queue recovered events for processing")

            return recovery_events

        except Exception as e:
            logger.error(f"Error during crash recovery: {e}")
            return recovery_events

    def _test_api_connection(self) -> bool:
        """Test connection to the ingest API."""
        logger.info("Testing API connection...")
        success, message = self.sender.test_connection()

        if success:
            logger.info(f"API connection test successful: {message}")
        else:
            logger.error(f"API connection test failed: {message}")

        return success

    def _handle_batch_ready(self, batch: EventBatch, wal_ids: List[int]) -> None:
        """Handle when a batch is ready to be sent.

        Args:
            batch: Event batch ready for transmission
            wal_ids: WAL IDs for recovery tracking
        """
        try:
            # Send the batch
            success, error_msg = self.sender.send_batch(batch)

            if success:
                # Mark events as processed in WAL
                if wal_ids:
                    self.batch_processor.mark_batch_processed(wal_ids)

                logger.debug(f"Successfully processed batch {batch.batch_id}")
            else:
                # Mark events as failed (increment retry count)
                if wal_ids:
                    self.batch_processor.mark_batch_failed(wal_ids)

                logger.error(f"Failed to process batch {batch.batch_id}: {error_msg}")

        except Exception as e:
            logger.error(f"Error handling batch {batch.batch_id}: {e}")

            # Mark as failed
            if wal_ids:
                self.batch_processor.mark_batch_failed(wal_ids)

    def _drain_queue(self) -> None:
        """Drain any remaining events from the queue."""
        logger.info("Draining remaining events from queue...")

        remaining_events = self.queue.clear()
        if remaining_events:
            logger.info(f"Found {len(remaining_events)} remaining events")

            # Write to WAL if enabled
            if self.wal:
                self.wal.write_events(remaining_events)
                logger.info(f"Wrote {len(remaining_events)} events to WAL for recovery")

    def _stats_loop(self) -> None:
        """Background thread for periodic stats reporting."""
        while self._running:
            try:
                time.sleep(self.config.stats_report_interval)

                if self._running:  # Check again after sleep
                    self._report_stats()

            except Exception as e:
                logger.error(f"Error in stats loop: {e}")
                time.sleep(10)  # Sleep longer on error

    def _report_stats(self) -> None:
        """Report current pipeline statistics."""
        try:
            stats = self.get_pipeline_stats()

            # Log summary stats
            queue_stats = stats["queue"]
            batcher_stats = stats["batcher"]
            sender_stats = stats["sender"]

            # Include WAL stats if available
            wal_info = ""
            if "wal" in stats and self.wal:
                wal_stats = stats["wal"]
                rotation_info = wal_stats.get("rotation", {})

                wal_info = f", WAL: {wal_stats['unprocessed_events']} pending, {wal_stats['active_window_sessions']} active sessions, DB: {wal_stats['database_size_mb']:.1f}MB"

                # Add rotation warning if needed
                if rotation_info.get("rotation_needed", False):
                    wal_info += " [ROTATION NEEDED]"
                elif rotation_info.get("archive_count", 0) > 0:
                    wal_info += f" [{rotation_info['archive_count']} archives]"

                # Perform periodic WAL maintenance
                self._perform_wal_maintenance()

            logger.info(
                f"Pipeline Stats - "
                f"Queue: {queue_stats['current_size']}/{queue_stats['max_size']} "
                f"({queue_stats['utilization']:.1%}), "
                f"Batches sent: {sender_stats['total_batches_sent']}, "
                f"Events sent: {sender_stats['total_events_sent']}, "
                f"Success rate: {sender_stats['success_rate']:.1%}"
                f"{wal_info}"
            )

            self._last_stats_report = datetime.now()

        except Exception as e:
            logger.error(f"Error reporting stats: {e}")

    def _perform_wal_maintenance(self) -> None:
        """Perform periodic WAL maintenance tasks."""
        if not self.wal:
            return

        try:
            # Clean up stale sessions periodically
            stale_count = self.wal.cleanup_stale_sessions()
            if stale_count > 0:
                logger.info(f"Cleaned up {stale_count} stale window sessions during maintenance")

            # Clean up old processed events (keep 24 hours)
            old_events_count = self.wal.cleanup_processed_events(hours=24)
            if old_events_count > 0:
                logger.info(f"Cleaned up {old_events_count} old processed events during maintenance")

            # Check if manual rotation is needed (in case rotation thread is behind)
            rotation_stats = self.wal.get_rotation_stats()
            if rotation_stats.get("rotation_needed", False):
                logger.warning("WAL rotation needed but not yet performed, triggering manual rotation...")
                success = self.wal.force_rotation()
                if success:
                    logger.info("Manual WAL rotation completed successfully")
                else:
                    logger.error("Manual WAL rotation failed")

        except Exception as e:
            logger.error(f"Error during WAL maintenance: {e}")

    def _log_final_stats(self) -> None:
        """Log final pipeline statistics on shutdown."""
        try:
            stats = self.get_pipeline_stats()

            logger.info("Final Pipeline Statistics:")
            logger.info(f"  Uptime: {stats['pipeline']['uptime_seconds']:.1f} seconds")
            logger.info(f"  Queue processed: {stats['queue']['total_dequeued']} events")
            logger.info(f"  Batches sent: {stats['sender']['total_batches_sent']}")
            logger.info(f"  Events sent: {stats['sender']['total_events_sent']}")
            logger.info(f"  Success rate: {stats['sender']['success_rate']:.1%}")

            if "wal" in stats:
                wal_stats = stats["wal"]
                logger.info(f"  WAL unprocessed: {wal_stats['unprocessed_events']} events")

        except Exception as e:
            logger.error(f"Error logging final stats: {e}")


def create_default_pipeline(
    api_base_url: str,
    api_key: str,
    username: str,
    client_id: Optional[str] = None,
    wal_db_path: Optional[Path] = None,
) -> PipelineOrchestrator:
    """Create a pipeline orchestrator with default configuration.

    Args:
        api_base_url: Base URL of the ingest API
        api_key: Bootstrap API key for authentication
        username: Username for events
        client_id: Optional client identifier (auto-generated if not provided)
        wal_db_path: Optional path for WAL database

    Returns:
        Configured pipeline orchestrator
    """
    config = PipelineConfig(
        api_base_url=api_base_url,
        api_key=api_key,
        username=username,
        client_id=client_id or f"devpulse-client-{uuid.uuid4().hex[:8]}",
    )

    if wal_db_path:
        config.wal_config.db_path = wal_db_path

    return PipelineOrchestrator(config)
