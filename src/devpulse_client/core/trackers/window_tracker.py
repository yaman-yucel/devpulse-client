"""Window tracker for DevPulse client.

This module tracks active window changes across platforms.
"""

from __future__ import annotations

import ctypes
import subprocess
from datetime import datetime
from typing import Optional

from loguru import logger

from ..events import WindowEvent
from .base import BaseTracker


class WindowTracker(BaseTracker):
    """Tracks active window changes."""

    def __init__(self, config, event_emitter):
        """Initialize window tracker.

        Args:
            config: DevPulse configuration
            event_emitter: Event emitter for the pipeline
        """
        super().__init__(config, event_emitter)
        self._last_window_title: Optional[str] = None
        self._window_start_time: Optional[datetime] = None

    def tick(self) -> None:
        """Check for window changes and emit events."""
        try:
            current_title = self._get_current_window_title()

            if current_title != self._last_window_title:
                now = datetime.now()

                # Emit event for previous window if it existed
                if self._last_window_title is not None and self._window_start_time is not None:
                    self._emit_window_event(window_title=self._last_window_title, start_time=self._window_start_time, end_time=now)

                # Start tracking new window
                self._last_window_title = current_title
                self._window_start_time = now

                # Emit start event for new window
                self._emit_window_event(window_title=current_title, start_time=now)

        except Exception as e:
            logger.error(f"Error in window tracker: {e}")

    def shutdown(self) -> None:
        """Complete any ongoing window tracking."""
        if self._last_window_title is not None and self._window_start_time is not None:
            self._emit_window_event(window_title=self._last_window_title, start_time=self._window_start_time, end_time=datetime.now())

    def _emit_window_event(self, window_title: str, start_time: datetime, end_time: Optional[datetime] = None) -> None:
        """Emit a window event."""
        duration = None
        if end_time and start_time:
            duration = (end_time - start_time).total_seconds()

        event = WindowEvent(
            window_title=window_title,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            username=self.config.username,
            client_id=self.config.client_id,
        )

        success = self.event_emitter.emit(event)
        if not success:
            logger.warning(f"Failed to emit window event: {window_title}")

    def _get_current_window_title(self) -> str:
        """Get the current active window title."""
        system = self.config.platform

        if system == "linux":
            return self._get_window_title_linux()
        elif system == "darwin":
            return self._get_window_title_macos()
        elif system == "windows":
            return self._get_window_title_windows()
        else:
            return "Unknown"

    def _get_window_title_linux(self) -> str:
        """Get window title on Linux using xdotool."""
        try:
            # Get focused window ID
            win_id_result = subprocess.run(["xdotool", "getwindowfocus"], capture_output=True, text=True, timeout=1)
            if win_id_result.returncode != 0:
                return "N/A"

            win_id = win_id_result.stdout.strip()

            # Get window title
            title_result = subprocess.run(["xdotool", "getwindowname", win_id], capture_output=True, text=True, timeout=1)
            if title_result.returncode == 0:
                return title_result.stdout.strip() or "N/A"

        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        return "N/A"

    def _get_window_title_macos(self) -> str:
        """Get window title on macOS using osascript."""
        try:
            result = subprocess.run(["osascript", "-e", 'tell application "System Events" to get title of (process 1 where frontmost is true)'], capture_output=True, text=True, timeout=2)

            if result.returncode == 0:
                return result.stdout.strip() or "N/A"

        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        return "N/A"

    def _get_window_title_windows(self) -> str:
        """Get window title on Windows using GetForegroundWindow."""
        try:
            user32 = ctypes.windll.user32
            hwnd = user32.GetForegroundWindow()

            if hwnd:
                length = user32.GetWindowTextLengthW(hwnd)
                if length > 0:
                    buffer = ctypes.create_unicode_buffer(length + 1)
                    user32.GetWindowTextW(hwnd, buffer, length + 1)
                    return buffer.value or "N/A"

        except Exception:
            pass

        return "N/A"
