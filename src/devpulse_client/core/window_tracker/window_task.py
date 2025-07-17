"""Improved window tracker with enhanced WAL-based crash recovery.

This module tracks window title changes and records window events using
the enhanced WAL capabilities for robust crash recovery.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from loguru import logger

from ...orchestrator import EventProducer
from ...wal import EventWAL
from ..events import WindowEvent
from .window_title_provider import WindowTitleProvider


@dataclass
class WindowTrackerTask:
    """Enhanced window tracker with WAL-based session recovery."""

    interval: float = 1.0  # Minimum window duration threshold
    _last_title: str | None = None
    _window_start: float | None = None
    _current_session_id: str | None = None
    _initialized: bool = False
    _event_producer: Optional[EventProducer] = None
    _wal: Optional[EventWAL] = None
    _username: str = ""
    _client_id: str = ""

    def configure(self, event_producer: EventProducer, wal: Optional[EventWAL] = None, username: str = "", client_id: str = "") -> None:
        """Configure the window tracker with pipeline components.

        Args:
            event_producer: Event producer for emitting events
            wal: WAL instance for session tracking
            username: Username for events
            client_id: Client identifier
        """
        self._event_producer = event_producer
        self._wal = wal
        self._username = username
        self._client_id = client_id
        logger.debug("Configured window tracker with pipeline components")

    def tick(self) -> None:
        """Main tracking loop - checks for window changes and handles events."""
        if not self._event_producer:
            logger.warning("Window tracker not properly configured, skipping tick")
            return

        now = time.time()
        current_title = WindowTitleProvider.current_title()

        # Handle initialization on first run
        if not self._initialized:
            self._initialized = True
            logger.info("Window tracker initialized")

        # Check if window title changed
        if current_title != self._last_title:
            self._handle_window_change(current_title, now)

        # Update activity for current session
        if self._current_session_id and self._wal:
            self._wal.update_window_session_activity(self._current_session_id)

    def _handle_window_change(self, new_title: str, now: float) -> None:
        """Handle a window title change event.

        Args:
            new_title: New window title
            now: Current timestamp
        """
        # Complete previous window session if exists
        if self._current_session_id and self._window_start is not None:
            self._complete_current_window_session(now)

        # Start tracking new window
        self._start_new_window_session(new_title, now)

    def _complete_current_window_session(self, end_time: float) -> None:
        """Complete the current window session and emit event.

        Args:
            end_time: End timestamp
        """
        if not self._current_session_id or self._window_start is None:
            return

        try:
            # Calculate duration
            duration = end_time - self._window_start
            start_datetime = datetime.fromtimestamp(self._window_start)
            end_datetime = datetime.fromtimestamp(end_time)

            # End the session in WAL and get session details
            if self._wal:
                session = self._wal.end_window_session(self._current_session_id)
                if session:
                    # Use session details for the event
                    window_title = session.window_title
                    start_datetime = session.start_time
                    end_datetime = session.last_activity
                    duration = (end_datetime - start_datetime).total_seconds()
                else:
                    window_title = self._last_title or "Unknown"
            else:
                window_title = self._last_title or "Unknown"

            # Only emit event if duration meets threshold
            if duration >= self.interval:
                event = WindowEvent(
                    timestamp=end_datetime, username=self._username, client_id=self._client_id, window_title=window_title, start_time=start_datetime, end_time=end_datetime, duration=duration
                )

                success = self._event_producer.emit(event)
                if success:
                    logger.info(f"Completed window session: '{window_title}' ({duration:.1f}s)")
                else:
                    logger.warning(f"Failed to emit window event for '{window_title}'")
            else:
                logger.debug(f"Window session too short: '{window_title}' ({duration:.1f}s)")

        except Exception as e:
            logger.error(f"Error completing window session: {e}")

    def _start_new_window_session(self, window_title: str, start_time: float) -> None:
        """Start a new window session.

        Args:
            window_title: Title of the new window
            start_time: Session start timestamp
        """
        try:
            # Generate new session ID
            self._current_session_id = f"window_{int(start_time * 1000)}_{uuid.uuid4().hex[:8]}"
            self._last_title = window_title
            self._window_start = start_time

            # Start session tracking in WAL
            if self._wal:
                start_datetime = datetime.fromtimestamp(start_time)
                success = self._wal.start_window_session(session_id=self._current_session_id, window_title=window_title, client_id=self._client_id, username=self._username, start_time=start_datetime)

                if success:
                    logger.debug(f"Started window session: '{window_title}' (ID: {self._current_session_id})")
                else:
                    logger.warning(f"Failed to start WAL session tracking for '{window_title}'")
            else:
                logger.debug(f"Started window tracking: '{window_title}' (no WAL)")

        except Exception as e:
            logger.error(f"Error starting new window session: {e}")

    def shutdown(self, now: float) -> None:
        """Complete any ongoing window session when the tracker shuts down.

        Args:
            now: Current timestamp
        """
        logger.info("Shutting down window tracker...")

        try:
            # Complete current session if exists
            if self._current_session_id:
                self._complete_current_window_session(now)
                self._current_session_id = None

            logger.info("Window tracker shutdown complete")

        except Exception as e:
            logger.error(f"Error during window tracker shutdown: {e}")

    def get_current_session_info(self) -> Optional[dict]:
        """Get information about the current window session.

        Returns:
            Dictionary with current session info, or None if no active session
        """
        if not self._current_session_id or not self._window_start:
            return None

        current_time = time.time()
        duration = current_time - self._window_start

        return {"session_id": self._current_session_id, "window_title": self._last_title, "start_time": datetime.fromtimestamp(self._window_start), "duration": duration}


# Compatibility function for legacy usage
def create_window_tracker(interval: float = 1.0) -> WindowTrackerTask:
    """Create a window tracker with specified interval.

    Args:
        interval: Minimum window duration threshold

    Returns:
        Configured window tracker
    """
    return WindowTrackerTask(interval=interval)
