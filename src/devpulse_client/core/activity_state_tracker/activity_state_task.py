from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from tracker.config.tracker_settings import tracker_settings
from tracker.db.event_store import EventStore
from tracker.tables.activity_table import ActivityEventType

from .idle_detector import IdleDetector
from .screen_lock_detector import ScreenLockDetector


@dataclass
class ActivityStateTask:
    _locked: bool | None = None
    _idle: bool | None = None  # state is non-existent until initialized or screen is locked
    _idle_detector: IdleDetector = IdleDetector()
    _lock_detector: ScreenLockDetector = ScreenLockDetector()

    def _log_activity(self, activity_type: ActivityEventType) -> None:
        logger.info(f"Logging activity: {activity_type.value}")
        EventStore.log_event(activity_type)

    def _check_idle_state(self) -> tuple[bool, ActivityEventType.INACTIVE | ActivityEventType.ACTIVE]:
        """Helper method to check idle state and return activity type."""
        idle_seconds = self._idle_detector.seconds_idle()
        is_idle = idle_seconds >= tracker_settings.IDLE_THRESHOLD
        activity_type = ActivityEventType.INACTIVE if is_idle else ActivityEventType.ACTIVE
        return is_idle, activity_type

    def initialize(self) -> None:
        locked = self._lock_detector.is_locked()

        if locked:
            self._log_activity(ActivityEventType.SCREEN_LOCKED)
            self._locked = True
            self._idle = None
        else:
            self._log_activity(ActivityEventType.SCREEN_UNLOCKED)
            self._locked = False

            self._idle, activity_type = self._check_idle_state()
            self._log_activity(activity_type)

    def tick(self) -> None:
        locked = self._lock_detector.is_locked()

        # If not initialized yet, initialize with current state
        if self._locked is None:
            self.initialize()
            return

        # Handle screen lock state changes
        if locked and not self._locked:
            self._log_activity(ActivityEventType.SCREEN_LOCKED)
            self._locked = True
            self._idle = None
        elif not locked and self._locked:
            self._log_activity(ActivityEventType.SCREEN_UNLOCKED)
            self._locked = False

            # Check idle state and log appropriate activity when unlocking
            self._idle, activity_type = self._check_idle_state()
            self._log_activity(activity_type)

        # Handle idle state changes when screen is unlocked (skip if just unlocked)
        if not locked:
            is_idle, activity_type = self._check_idle_state()

            if self._idle != is_idle:
                self._log_activity(activity_type)
                self._idle = is_idle
