"""Event batcher for collecting events into batches for efficient transmission.

This module provides batching logic to group events from the queue and WAL
into optimally-sized batches for transmission to the ingest API. It supports
both size-based and time-based batching strategies.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from loguru import logger

from ..core.events import BaseEvent, EventBatch
from ..queuer import EventQueue
from ..wal import EventWAL


@dataclass
class BatcherConfig:
    """Configuration for the event batcher."""

    max_batch_size: int = 100  # Maximum events per batch
    max_batch_age_seconds: int = 30  # Maximum time to wait before sending batch
    min_batch_size: int = 1  # Minimum events before sending (unless timeout)
    max_memory_batches: int = 5  # Maximum batches to hold in memory
    batch_timeout_check_interval: float = 1.0  # How often to check for timeouts


@dataclass
class PendingBatch:
    """A batch that is being built."""

    batch: EventBatch = field(default_factory=EventBatch)
    created_at: datetime = field(default_factory=datetime.now)
    wal_ids: List[int] = field(default_factory=list)  # WAL IDs for recovery

    def add_event(self, event: BaseEvent, wal_id: Optional[int] = None) -> None:
        """Add an event to this batch."""
        self.batch.add_event(event)
        if wal_id is not None:
            self.wal_ids.append(wal_id)

    def is_full(self, max_size: int) -> bool:
        """Check if batch is full."""
        return self.batch.size() >= max_size

    def is_expired(self, max_age_seconds: int) -> bool:
        """Check if batch has expired."""
        age = datetime.now() - self.created_at
        return age.total_seconds() >= max_age_seconds

    def should_send(self, config: BatcherConfig) -> bool:
        """Check if batch should be sent."""
        return self.is_full(config.max_batch_size) or (self.batch.size() >= config.min_batch_size and self.is_expired(config.max_batch_age_seconds))


class EventBatcher:
    """Batches events for efficient transmission to the ingest API."""

    def __init__(
        self,
        config: BatcherConfig = BatcherConfig(),
        event_queue: Optional[EventQueue] = None,
        event_wal: Optional[EventWAL] = None,
        batch_ready_callback: Optional[Callable[[EventBatch, List[int]], None]] = None,
    ):
        """Initialize the event batcher.

        Args:
            config: Batcher configuration
            event_queue: Event queue to read from
            event_wal: WAL to read recovery events from
            batch_ready_callback: Function to call when batch is ready
        """
        self.config = config
        self.event_queue = event_queue
        self.event_wal = event_wal
        self.batch_ready_callback = batch_ready_callback

        self._current_batch: Optional[PendingBatch] = None
        self._lock = threading.RLock()
        self._running = False
        self._batch_thread: Optional[threading.Thread] = None

        # Statistics
        self._total_events_batched = 0
        self._total_batches_created = 0
        self._total_batches_sent = 0

    def start(self) -> None:
        """Start the batcher background thread."""
        with self._lock:
            if self._running:
                logger.warning("Batcher is already running")
                return

            self._running = True
            self._batch_thread = threading.Thread(target=self._batch_loop, daemon=True)
            self._batch_thread.start()
            logger.info("Started event batcher")

    def stop(self) -> None:
        """Stop the batcher and send any pending batches."""
        with self._lock:
            if not self._running:
                return

            self._running = False

        # Wait for thread to finish
        if self._batch_thread:
            self._batch_thread.join(timeout=5.0)

        # Send any remaining batch
        self._send_current_batch(force=True)

        logger.info(f"Stopped event batcher. Stats - Events: {self._total_events_batched}, Batches created: {self._total_batches_created}, Batches sent: {self._total_batches_sent}")

    def force_batch_send(self) -> bool:
        """Force sending the current batch regardless of size/age."""
        return self._send_current_batch(force=True)

    def get_stats(self) -> Dict[str, Any]:
        """Get batcher statistics."""
        with self._lock:
            current_batch_size = self._current_batch.batch.size() if self._current_batch else 0
            current_batch_age = None

            if self._current_batch:
                age = datetime.now() - self._current_batch.created_at
                current_batch_age = age.total_seconds()

            return {
                "running": self._running,
                "current_batch_size": current_batch_size,
                "current_batch_age_seconds": current_batch_age,
                "total_events_batched": self._total_events_batched,
                "total_batches_created": self._total_batches_created,
                "total_batches_sent": self._total_batches_sent,
                "config": {
                    "max_batch_size": self.config.max_batch_size,
                    "max_batch_age_seconds": self.config.max_batch_age_seconds,
                    "min_batch_size": self.config.min_batch_size,
                },
            }

    def _batch_loop(self) -> None:
        """Main batching loop."""
        logger.debug("Started batcher loop")

        while self._running:
            try:
                # Process events from queue
                self._process_queue_events()

                # Process events from WAL (recovery)
                self._process_wal_events()

                # Check if current batch should be sent
                self._check_batch_timeout()

                # Sleep briefly
                time.sleep(self.config.batch_timeout_check_interval)

            except Exception as e:
                logger.error(f"Error in batcher loop: {e}")
                time.sleep(1.0)  # Longer sleep on error

        logger.debug("Batcher loop finished")

    def _process_queue_events(self) -> None:
        """Process events from the in-memory queue."""
        if not self.event_queue:
            return

        # Get a batch of events from queue
        events = self.event_queue.dequeue_batch(max_size=min(50, self.config.max_batch_size))

        for event in events:
            self._add_event_to_batch(event)

    def _process_wal_events(self) -> None:
        """Process unprocessed events from WAL."""
        if not self.event_wal:
            return

        # Read unprocessed events from WAL
        wal_events = self.event_wal.read_unprocessed_events(limit=min(50, self.config.max_batch_size))

        for wal_event in wal_events:
            # Convert WAL event back to BaseEvent
            event = self._recreate_event_from_wal(wal_event)
            if event:
                self._add_event_to_batch(event, wal_id=wal_event["wal_id"])

    def _add_event_to_batch(self, event: BaseEvent, wal_id: Optional[int] = None) -> None:
        """Add an event to the current batch."""
        with self._lock:
            # Create new batch if needed
            if self._current_batch is None:
                self._current_batch = PendingBatch()
                self._total_batches_created += 1
                logger.debug("Created new batch")

            # Add event to batch
            self._current_batch.add_event(event, wal_id)
            self._total_events_batched += 1

            logger.debug(f"Added {event.event_type} event to batch (size: {self._current_batch.batch.size()})")

            # Check if batch should be sent
            if self._current_batch.should_send(self.config):
                self._send_current_batch()

    def _check_batch_timeout(self) -> None:
        """Check if current batch has timed out."""
        with self._lock:
            if self._current_batch and self._current_batch.is_expired(self.config.max_batch_age_seconds):
                if self._current_batch.batch.size() >= self.config.min_batch_size:
                    logger.debug("Sending batch due to timeout")
                    self._send_current_batch()

    def _send_current_batch(self, force: bool = False) -> bool:
        """Send the current batch if ready."""
        with self._lock:
            if not self._current_batch:
                return True

            batch = self._current_batch

            # Check if we should send
            if not force and not batch.should_send(self.config):
                return True

            if batch.batch.size() == 0:
                self._current_batch = None
                return True

            # Send the batch
            try:
                if self.batch_ready_callback:
                    self.batch_ready_callback(batch.batch, batch.wal_ids)
                    self._total_batches_sent += 1

                    logger.info(f"Sent batch {batch.batch.batch_id} with {batch.batch.size()} events")
                else:
                    logger.warning("No batch ready callback configured")

                # Clear current batch
                self._current_batch = None
                return True

            except Exception as e:
                logger.error(f"Failed to send batch: {e}")
                # Keep the batch for retry
                return False

    def _recreate_event_from_wal(self, wal_event: Dict[str, Any]) -> Optional[BaseEvent]:
        """Recreate an event object from WAL data."""
        try:
            event_type = wal_event["event_type"]
            event_data = wal_event["event_data"]

            # Import here to avoid circular imports
            from ..wal.event_wal import _recreate_event_from_dict

            return _recreate_event_from_dict(event_type, event_data)

        except Exception as e:
            logger.error(f"Failed to recreate event from WAL: {e}")
            return None


class BatchProcessor:
    """Helper class to process batches with retry logic."""

    def __init__(
        self,
        event_wal: Optional[EventWAL] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """Initialize batch processor.

        Args:
            event_wal: WAL for marking events as processed
            max_retries: Maximum retry attempts
            retry_delay: Delay between retries
        """
        self.event_wal = event_wal
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def mark_batch_processed(self, wal_ids: List[int]) -> bool:
        """Mark a batch as successfully processed.

        Args:
            wal_ids: List of WAL IDs that were processed

        Returns:
            True if successful, False otherwise
        """
        if not self.event_wal or not wal_ids:
            return True

        return self.event_wal.mark_events_processed(wal_ids)

    def mark_batch_failed(self, wal_ids: List[int]) -> bool:
        """Mark a batch as failed (increment retry count).

        Args:
            wal_ids: List of WAL IDs that failed

        Returns:
            True if successful, False otherwise
        """
        if not self.event_wal or not wal_ids:
            return True

        return self.event_wal.increment_retry_count(wal_ids)
