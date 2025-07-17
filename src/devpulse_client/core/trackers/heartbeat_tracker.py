"""Heartbeat tracker for DevPulse client.

This module sends periodic heartbeat events to indicate the client is alive.
"""

from __future__ import annotations

import time
from typing import Optional

from loguru import logger

from ..events import HeartbeatEvent
from .base import BaseTracker


class HeartbeatTracker(BaseTracker):
    """Sends periodic heartbeat events."""

    def __init__(self, config, event_emitter):
        """Initialize heartbeat tracker.

        Args:
            config: DevPulse configuration
            event_emitter: Event emitter for the pipeline
        """
        super().__init__(config, event_emitter)
        self._last_heartbeat: Optional[float] = None

    def tick(self) -> None:
        """Send heartbeat if interval has elapsed."""
        now = time.time()

        if self._last_heartbeat is None or now - self._last_heartbeat >= self.config.tracker.heartbeat_interval:
            self._emit_heartbeat()
            self._last_heartbeat = now

    def shutdown(self) -> None:
        """Send final heartbeat."""
        event = HeartbeatEvent(
            heartbeat_type="final",
            username=self.config.username,
            client_id=self.config.client_id,
        )

        success = self.event_emitter.emit(event)
        if not success:
            logger.warning("Failed to emit final heartbeat")

    def _emit_heartbeat(self) -> None:
        """Emit a regular heartbeat event."""
        event = HeartbeatEvent(
            username=self.config.username,
            client_id=self.config.client_id,
        )

        success = self.event_emitter.emit(event)
        if not success:
            logger.warning("Failed to emit heartbeat")
