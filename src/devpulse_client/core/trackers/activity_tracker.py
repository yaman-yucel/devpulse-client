"""Activity state tracker for DevPulse client.

This module tracks user activity state (active/inactive/locked) across platforms.
"""

from __future__ import annotations

import ctypes
import subprocess
from typing import Optional

from loguru import logger

from ..events import ActivityEvent, ActivityEventType
from .base import BaseTracker


class ActivityTracker(BaseTracker):
    """Tracks user activity state (active/inactive/locked)."""

    def __init__(self, config, event_emitter):
        """Initialize activity tracker.

        Args:
            config: DevPulse configuration
            event_emitter: Event emitter for the pipeline
        """
        super().__init__(config, event_emitter)
        self._last_activity_state: Optional[ActivityEventType] = None
        self._last_screen_state: Optional[bool] = None  # True = locked, False = unlocked

    def tick(self) -> None:
        """Check current activity state and emit events if changed."""
        try:
            # Check screen lock state
            is_locked = self._is_screen_locked()
            if self._last_screen_state != is_locked:
                if is_locked:
                    self._emit_activity_event(ActivityEventType.SCREEN_LOCKED)
                else:
                    self._emit_activity_event(ActivityEventType.SCREEN_UNLOCKED)
                self._last_screen_state = is_locked

            # Only check idle state if screen is not locked
            if not is_locked:
                idle_seconds = self._get_idle_seconds()
                is_idle = idle_seconds >= self.config.tracker.idle_threshold

                current_state = ActivityEventType.INACTIVE if is_idle else ActivityEventType.ACTIVE

                if self._last_activity_state != current_state:
                    self._emit_activity_event(current_state)
                    self._last_activity_state = current_state

        except Exception as e:
            logger.error(f"Error in activity tracker: {e}")

    def initialize(self) -> None:
        """Initialize the tracker and emit startup event."""
        self._emit_activity_event(ActivityEventType.STARTED)
        # Force check of current state
        self._last_activity_state = None
        self._last_screen_state = None
        self.tick()

    def shutdown(self) -> None:
        """Emit shutdown event."""
        self._emit_activity_event(ActivityEventType.NORMAL_SHUTDOWN)

    def _emit_activity_event(self, activity_type: ActivityEventType) -> None:
        """Emit an activity event."""
        event = ActivityEvent(
            activity_type=activity_type,
            username=self.config.username,
            client_id=self.config.client_id,
        )

        success = self.event_emitter.emit(event)
        if not success:
            logger.warning(f"Failed to emit activity event: {activity_type.value}")

    def _get_idle_seconds(self) -> float:
        """Get seconds since last user input."""
        system = self.config.platform

        if system == "linux":
            return self._get_idle_seconds_linux()
        elif system == "darwin":
            return self._get_idle_seconds_macos()
        elif system == "windows":
            return self._get_idle_seconds_windows()
        else:
            logger.warning(f"Idle detection not supported on {system}")
            return 0.0

    def _get_idle_seconds_linux(self) -> float:
        """Get idle seconds on Linux using xprintidle."""
        try:
            result = subprocess.run(["xprintidle"], capture_output=True, text=True, timeout=1)
            if result.returncode == 0:
                return int(result.stdout.strip()) / 1000.0
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
            pass

        # Fallback to xssstate
        try:
            result = subprocess.run(["xssstate", "-i"], capture_output=True, text=True, timeout=1)
            if result.returncode == 0:
                return int(result.stdout.strip()) / 1000.0
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
            pass

        return 0.0

    def _get_idle_seconds_macos(self) -> float:
        """Get idle seconds on macOS using ioreg."""
        try:
            result = subprocess.run(["ioreg", "-c", "IOHIDSystem"], capture_output=True, text=True, timeout=2)
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "HIDIdleTime" in line:
                        # Extract nanoseconds and convert to seconds
                        nanos = int(line.split()[-1])
                        return nanos / 1e9
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
            pass

        return 0.0

    def _get_idle_seconds_windows(self) -> float:
        """Get idle seconds on Windows using GetLastInputInfo."""
        try:

            class LASTINPUTINFO(ctypes.Structure):
                _fields_ = [
                    ("cbSize", ctypes.wintypes.UINT),
                    ("dwTime", ctypes.wintypes.DWORD),
                ]

            lii = LASTINPUTINFO()
            lii.cbSize = ctypes.sizeof(lii)

            if ctypes.windll.user32.GetLastInputInfo(ctypes.byref(lii)):
                millis = ctypes.windll.kernel32.GetTickCount() - lii.dwTime
                return millis / 1000.0
        except Exception:
            pass

        return 0.0

    def _is_screen_locked(self) -> bool:
        """Check if screen is locked."""
        system = self.config.platform

        if system == "linux":
            return self._is_screen_locked_linux()
        elif system == "darwin":
            return self._is_screen_locked_macos()
        elif system == "windows":
            return self._is_screen_locked_windows()
        else:
            return False

    def _is_screen_locked_linux(self) -> bool:
        """Check if screen is locked on Linux."""
        # Try gnome-screensaver
        try:
            result = subprocess.run(["gnome-screensaver-command", "-q"], capture_output=True, timeout=1)
            if b"is active" in result.stdout:
                return True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Try loginctl
        try:
            result = subprocess.run(["loginctl", "show-session", "-p", "LockedHint"], capture_output=True, timeout=1)
            if b"LockedHint=yes" in result.stdout:
                return True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        return False

    def _is_screen_locked_macos(self) -> bool:
        """Check if screen is locked on macOS."""
        try:
            result = subprocess.run(["/System/Library/CoreServices/Menu Extras/User.menu/Contents/Resources/CGSession", "-s"], capture_output=True, timeout=1)
            output = result.stdout.decode()
            return "kCGSSessionScreenIsLocked = 1" in output or "ScreenIsLocked=1" in output
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        return False

    def _is_screen_locked_windows(self) -> bool:
        """Check if screen is locked on Windows."""
        try:
            user32 = ctypes.windll.User32
            hDesktop = user32.OpenDesktopW("Default", 0, False, 0x100)
            if hDesktop:
                locked = user32.SwitchDesktop(hDesktop) == 0
                user32.CloseDesktop(hDesktop)
                return locked
        except Exception:
            pass

        return False
