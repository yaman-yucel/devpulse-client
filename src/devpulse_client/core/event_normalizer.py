"""Event normalizer for converting core component events to standardized format.

This module provides utilities to convert events from the legacy core components
to the new standardized event format used in the pipeline architecture.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from loguru import logger

from .events import ActivityEvent, ActivityEventType, BaseEvent, HeartbeatEvent, HeartbeatType, ScreenshotEvent, WindowEvent


class EventNormalizer:
    """Converts legacy core component events to standardized events."""

    @staticmethod
    def normalize_activity_event(
        activity_type: str,
        timestamp: Optional[datetime] = None,
        username: str = "",
        client_id: str = "",
    ) -> ActivityEvent:
        """Normalize an activity event from the legacy format.

        Args:
            activity_type: Legacy activity type string
            timestamp: Event timestamp
            username: Username for the event
            client_id: Client identifier

        Returns:
            Standardized ActivityEvent
        """
        # Map legacy activity types to new enum
        activity_type_mapping = {
            "active": ActivityEventType.ACTIVE,
            "inactive": ActivityEventType.INACTIVE,
            "started": ActivityEventType.STARTED,
            "screen_locked": ActivityEventType.SCREEN_LOCKED,
            "screen_unlocked": ActivityEventType.SCREEN_UNLOCKED,
            "normal_shutdown": ActivityEventType.NORMAL_SHUTDOWN,
            "system_shutdown": ActivityEventType.SYSTEM_SHUTDOWN,
            "user_interrupt": ActivityEventType.USER_INTERRUPT,
        }

        normalized_type = activity_type_mapping.get(activity_type.lower(), ActivityEventType.ACTIVE)

        return ActivityEvent(
            timestamp=timestamp or datetime.now(),
            username=username,
            client_id=client_id,
            activity_type=normalized_type,
        )

    @staticmethod
    def normalize_heartbeat_event(
        heartbeat_type: str = "regular",
        timestamp: Optional[datetime] = None,
        username: str = "",
        client_id: str = "",
    ) -> HeartbeatEvent:
        """Normalize a heartbeat event from the legacy format.

        Args:
            heartbeat_type: Legacy heartbeat type string
            timestamp: Event timestamp
            username: Username for the event
            client_id: Client identifier

        Returns:
            Standardized HeartbeatEvent
        """
        # Map legacy heartbeat types to new enum
        heartbeat_type_mapping = {
            "regular": HeartbeatType.REGULAR,
            "final": HeartbeatType.FINAL,
        }

        normalized_type = heartbeat_type_mapping.get(heartbeat_type.lower(), HeartbeatType.REGULAR)

        return HeartbeatEvent(
            timestamp=timestamp or datetime.now(),
            username=username,
            client_id=client_id,
            heartbeat_type=normalized_type,
        )

    @staticmethod
    def normalize_window_event(
        window_title: str,
        timestamp: Optional[datetime] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        duration: Optional[float] = None,
        username: str = "",
        client_id: str = "",
    ) -> WindowEvent:
        """Normalize a window event from the legacy format.

        Args:
            window_title: Title of the window
            timestamp: Event timestamp
            start_time: Window focus start time
            end_time: Window focus end time
            duration: Window focus duration in seconds
            username: Username for the event
            client_id: Client identifier

        Returns:
            Standardized WindowEvent
        """
        return WindowEvent(
            timestamp=timestamp or datetime.now(),
            username=username,
            client_id=client_id,
            window_title=window_title,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
        )

    @staticmethod
    def normalize_screenshot_event(
        screenshot_path: str,
        timestamp: Optional[datetime] = None,
        monitor_count: int = 1,
        screenshot_hash: Optional[str] = None,
        username: str = "",
        client_id: str = "",
    ) -> ScreenshotEvent:
        """Normalize a screenshot event.

        Args:
            screenshot_path: Path to the screenshot file
            timestamp: Event timestamp
            monitor_count: Number of monitors captured
            screenshot_hash: Hash of screenshot for deduplication
            username: Username for the event
            client_id: Client identifier

        Returns:
            Standardized ScreenshotEvent
        """
        return ScreenshotEvent(
            timestamp=timestamp or datetime.now(),
            username=username,
            client_id=client_id,
            screenshot_path=screenshot_path,
            monitor_count=monitor_count,
            screenshot_hash=screenshot_hash,
        )


class LegacyEventAdapter:
    """Adapter to convert legacy EventStore calls to pipeline events."""

    def __init__(self, pipeline_producer=None):
        """Initialize the adapter.

        Args:
            pipeline_producer: Event producer for the pipeline
        """
        self.pipeline_producer = pipeline_producer
        self._enabled = pipeline_producer is not None

    def log_event(self, activity_type, username: str = "", client_id: str = "") -> None:
        """Legacy-compatible method for logging activity events.

        Args:
            activity_type: Activity event type (enum or string)
            username: Username for the event
            client_id: Client identifier
        """
        if not self._enabled:
            logger.warning("Pipeline producer not configured, dropping event")
            return

        try:
            # Convert enum to string if needed
            activity_type_str = activity_type.value if hasattr(activity_type, "value") else str(activity_type)

            # Normalize and emit event
            event = EventNormalizer.normalize_activity_event(
                activity_type=activity_type_str,
                username=username,
                client_id=client_id,
            )

            success = self.pipeline_producer.emit(event)
            if not success:
                logger.warning(f"Failed to emit activity event: {activity_type_str}")

        except Exception as e:
            logger.error(f"Error logging activity event: {e}")

    def heartbeat(
        self,
        timestamp: Optional[datetime] = None,
        heartbeat_type="regular",
        username: str = "",
        client_id: str = "",
    ) -> None:
        """Legacy-compatible method for logging heartbeat events.

        Args:
            timestamp: Event timestamp
            heartbeat_type: Heartbeat type (enum or string)
            username: Username for the event
            client_id: Client identifier
        """
        if not self._enabled:
            logger.warning("Pipeline producer not configured, dropping heartbeat")
            return

        try:
            # Convert enum to string if needed
            heartbeat_type_str = heartbeat_type.value if hasattr(heartbeat_type, "value") else str(heartbeat_type)

            # Normalize and emit event
            event = EventNormalizer.normalize_heartbeat_event(
                heartbeat_type=heartbeat_type_str,
                timestamp=timestamp,
                username=username,
                client_id=client_id,
            )

            success = self.pipeline_producer.emit(event)
            if not success:
                logger.warning(f"Failed to emit heartbeat event: {heartbeat_type_str}")

        except Exception as e:
            logger.error(f"Error logging heartbeat event: {e}")

    def log_window_event(
        self,
        window_title: str,
        timestamp: Optional[datetime] = None,
        duration: float = 0.0,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        username: str = "",
        client_id: str = "",
    ) -> None:
        """Legacy-compatible method for logging window events.

        Args:
            window_title: Title of the window
            timestamp: Event timestamp
            duration: Window focus duration
            start_time: Window focus start time
            end_time: Window focus end time
            username: Username for the event
            client_id: Client identifier
        """
        if not self._enabled:
            logger.warning("Pipeline producer not configured, dropping window event")
            return

        try:
            # Normalize and emit event
            event = EventNormalizer.normalize_window_event(
                window_title=window_title,
                timestamp=timestamp,
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                username=username,
                client_id=client_id,
            )

            success = self.pipeline_producer.emit(event)
            if not success:
                logger.warning(f"Failed to emit window event: {window_title}")

        except Exception as e:
            logger.error(f"Error logging window event: {e}")

    def create_incomplete_window_event(
        self,
        window_title: str,
        start_time: datetime,
        username: str = "",
        client_id: str = "",
    ) -> int:
        """Legacy-compatible method for creating incomplete window events.

        Note: In the new architecture, we don't track incomplete events the same way.
        This returns a dummy ID for compatibility but immediately emits the start event.

        Args:
            window_title: Title of the window
            start_time: When the window became focused
            username: Username for the event
            client_id: Client identifier

        Returns:
            Dummy event ID for compatibility
        """
        if not self._enabled:
            logger.warning("Pipeline producer not configured, dropping window start event")
            return -1

        try:
            # In the new architecture, we emit window start events immediately
            event = EventNormalizer.normalize_window_event(
                window_title=window_title,
                timestamp=start_time,
                start_time=start_time,
                username=username,
                client_id=client_id,
            )

            success = self.pipeline_producer.emit(event)
            if success:
                # Return a timestamp-based ID for compatibility
                return int(start_time.timestamp() * 1000)
            else:
                logger.warning(f"Failed to emit window start event: {window_title}")
                return -1

        except Exception as e:
            logger.error(f"Error creating window start event: {e}")
            return -1

    def complete_window_event(
        self,
        event_id: int,
        end_time: datetime,
        username: str = "",
        client_id: str = "",
    ) -> None:
        """Legacy-compatible method for completing window events.

        Note: In the new architecture, window completion is handled differently.
        This logs the completion but doesn't update an existing event.

        Args:
            event_id: Legacy event ID (ignored in new architecture)
            end_time: When the window lost focus
            username: Username for the event
            client_id: Client identifier
        """
        if not self._enabled:
            logger.warning("Pipeline producer not configured, dropping window completion")
            return

        try:
            # In the new architecture, we could emit a window end event
            # For now, we just log it
            logger.debug(f"Window event {event_id} completed at {end_time}")

        except Exception as e:
            logger.error(f"Error completing window event: {e}")

    def enable(self, pipeline_producer) -> None:
        """Enable the adapter with a pipeline producer.

        Args:
            pipeline_producer: Event producer for the pipeline
        """
        self.pipeline_producer = pipeline_producer
        self._enabled = True
        logger.info("Enabled legacy event adapter")

    def disable(self) -> None:
        """Disable the adapter."""
        self.pipeline_producer = None
        self._enabled = False
        logger.info("Disabled legacy event adapter")
