"""In-memory event queue for the DevPulse client.

This module provides a thread-safe queue for buffering events before they
are processed by the WAL or batcher components. It handles backpressure by
spilling events to the WAL when the queue becomes full.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional

from loguru import logger

from ..core.events import BaseEvent


@dataclass
class QueueConfig:
    """Configuration for the event queue."""

    max_size: int = 1000  # Maximum events in memory
    spill_threshold: float = 0.8  # Spill to WAL when 80% full
    drain_batch_size: int = 50  # How many events to drain at once
    timeout_seconds: float = 0.1  # Timeout for queue operations


class EventQueue:
    """Thread-safe in-memory queue for events with WAL spillover."""

    def __init__(
        self,
        config: QueueConfig = QueueConfig(),
        wal_spill_callback: Optional[Callable[[list[BaseEvent]], None]] = None,
    ):
        """Initialize the event queue.

        Args:
            config: Queue configuration
            wal_spill_callback: Function to call when spilling events to WAL
        """
        self.config = config
        self._queue: deque[BaseEvent] = deque()
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._wal_spill_callback = wal_spill_callback

        # Statistics
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._total_spilled = 0
        self._running = True

    def enqueue(self, event: BaseEvent) -> bool:
        """Add an event to the queue.

        Args:
            event: Event to enqueue

        Returns:
            True if enqueued successfully, False if queue is full
        """
        with self._lock:
            if not self._running:
                logger.warning("Queue is shut down, dropping event")
                return False

            # Check if we need to spill to WAL before adding
            if self._should_spill():
                self._spill_to_wal()

            # Add the event if there's space
            if len(self._queue) < self.config.max_size:
                self._queue.append(event)
                self._total_enqueued += 1
                self._not_empty.notify()

                logger.debug(f"Enqueued {event.event_type} event, queue size: {len(self._queue)}")
                return True
            else:
                # Queue is full even after spilling
                logger.warning(f"Queue full, dropping {event.event_type} event")
                return False

    def dequeue(self, timeout: Optional[float] = None) -> Optional[BaseEvent]:
        """Remove and return an event from the queue.

        Args:
            timeout: Maximum time to wait for an event

        Returns:
            Event if available, None if timeout or shutdown
        """
        timeout = timeout or self.config.timeout_seconds

        with self._lock:
            if not self._running and len(self._queue) == 0:
                return None

            if len(self._queue) == 0:
                if not self._not_empty.wait(timeout):
                    return None

            if len(self._queue) > 0:
                event = self._queue.popleft()
                self._total_dequeued += 1
                logger.debug(f"Dequeued {event.event_type} event, queue size: {len(self._queue)}")
                return event

        return None

    def dequeue_batch(self, max_size: Optional[int] = None) -> list[BaseEvent]:
        """Remove and return multiple events from the queue.

        Args:
            max_size: Maximum number of events to return

        Returns:
            List of events (may be empty)
        """
        max_size = max_size or self.config.drain_batch_size
        events = []

        with self._lock:
            while len(events) < max_size and len(self._queue) > 0:
                event = self._queue.popleft()
                events.append(event)
                self._total_dequeued += 1

        if events:
            logger.debug(f"Dequeued batch of {len(events)} events, queue size: {len(self._queue)}")

        return events

    def size(self) -> int:
        """Return the current queue size."""
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        with self._lock:
            return len(self._queue) == 0

    def is_full(self) -> bool:
        """Check if the queue is at capacity."""
        with self._lock:
            return len(self._queue) >= self.config.max_size

    def clear(self) -> list[BaseEvent]:
        """Clear all events from the queue and return them."""
        with self._lock:
            events = list(self._queue)
            self._queue.clear()
            logger.info(f"Cleared {len(events)} events from queue")
            return events

    def shutdown(self) -> list[BaseEvent]:
        """Shutdown the queue and return any remaining events."""
        with self._lock:
            self._running = False
            remaining_events = list(self._queue)
            self._queue.clear()
            self._not_empty.notify_all()

            logger.info(f"Queue shutdown. Stats - Enqueued: {self._total_enqueued}, Dequeued: {self._total_dequeued}, Spilled: {self._total_spilled}, Remaining: {len(remaining_events)}")

            return remaining_events

    def get_stats(self) -> dict:
        """Get queue statistics."""
        with self._lock:
            return {
                "current_size": len(self._queue),
                "max_size": self.config.max_size,
                "total_enqueued": self._total_enqueued,
                "total_dequeued": self._total_dequeued,
                "total_spilled": self._total_spilled,
                "running": self._running,
                "utilization": len(self._queue) / self.config.max_size,
            }

    def _should_spill(self) -> bool:
        """Check if we should spill events to WAL."""
        current_size = len(self._queue)
        threshold = int(self.config.max_size * self.config.spill_threshold)
        return current_size >= threshold

    def _spill_to_wal(self) -> None:
        """Spill some events to WAL to make room."""
        if not self._wal_spill_callback:
            logger.warning("No WAL spill callback configured, cannot spill events")
            return

        # Spill a batch of events to WAL
        spill_count = min(self.config.drain_batch_size, len(self._queue))
        if spill_count > 0:
            events_to_spill = []
            for _ in range(spill_count):
                events_to_spill.append(self._queue.popleft())

            try:
                self._wal_spill_callback(events_to_spill)
                self._total_spilled += spill_count
                logger.info(f"Spilled {spill_count} events to WAL due to backpressure")
            except Exception as e:
                # Put events back in queue if WAL fails
                for event in reversed(events_to_spill):
                    self._queue.appendleft(event)
                logger.error(f"Failed to spill events to WAL: {e}")


class EventProducer:
    """Helper class for components to produce events."""

    def __init__(self, queue: EventQueue, component_name: str = "unknown"):
        """Initialize producer.

        Args:
            queue: Event queue to send events to
            component_name: Name of the component producing events
        """
        self.queue = queue
        self.component_name = component_name

    def emit(self, event: BaseEvent) -> bool:
        """Emit an event to the queue.

        Args:
            event: Event to emit

        Returns:
            True if successful, False if queue rejected the event
        """
        success = self.queue.enqueue(event)
        if not success:
            logger.error(f"{self.component_name}: Failed to emit {event.event_type} event")
        return success
