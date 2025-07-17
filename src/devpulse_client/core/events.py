"""Event models for the DevPulse client architecture.

This module defines standardized event structures that flow through the
client pipeline: Event Hooks → Queuer → WAL → Batcher → Sender → API
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict


class EventType(str, Enum):
    """Types of events that can be tracked by the DevPulse client."""

    ACTIVITY = "activity"
    HEARTBEAT = "heartbeat"
    WINDOW = "window"
    SCREENSHOT = "screenshot"


class ActivityEventType(str, Enum):
    """Activity event subtypes."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    STARTED = "started"
    SCREEN_LOCKED = "screen_locked"
    SCREEN_UNLOCKED = "screen_unlocked"
    NORMAL_SHUTDOWN = "normal_shutdown"
    SYSTEM_SHUTDOWN = "system_shutdown"
    USER_INTERRUPT = "user_interrupt"


class HeartbeatType(str, Enum):
    """Heartbeat event subtypes."""

    REGULAR = "regular"
    FINAL = "final"


@dataclass
class BaseEvent(ABC):
    """Base class for all events in the DevPulse system."""

    event_type: EventType
    timestamp: datetime = field(default_factory=datetime.now)
    username: str = ""
    client_id: str = ""

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization."""
        pass

    def to_batch_item(self) -> Dict[str, Any]:
        """Convert to format suitable for batch API payload."""
        return self.to_dict()


@dataclass
class ActivityEvent(BaseEvent):
    """Activity state change event."""

    event_type: EventType = EventType.ACTIVITY
    activity_type: ActivityEventType = ActivityEventType.ACTIVE

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "activity_type": self.activity_type.value,
            "timestamp": self.timestamp.isoformat(),
            "username": self.username,
            "client_id": self.client_id,
        }


@dataclass
class HeartbeatEvent(BaseEvent):
    """Heartbeat event to show client is alive."""

    event_type: EventType = EventType.HEARTBEAT
    heartbeat_type: HeartbeatType = HeartbeatType.REGULAR

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "heartbeat_type": self.heartbeat_type.value,
            "timestamp": self.timestamp.isoformat(),
            "username": self.username,
            "client_id": self.client_id,
        }


@dataclass
class WindowEvent(BaseEvent):
    """Window focus change event."""

    event_type: EventType = EventType.WINDOW
    window_title: str = ""
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration: float | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "window_title": self.window_title,
            "timestamp": self.timestamp.isoformat(),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "username": self.username,
            "client_id": self.client_id,
        }


@dataclass
class ScreenshotEvent(BaseEvent):
    """Screenshot capture event."""

    event_type: EventType = EventType.SCREENSHOT
    screenshot_path: str = ""
    monitor_count: int = 1
    screenshot_hash: str | None = None  # For deduplication

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "screenshot_path": self.screenshot_path,
            "monitor_count": self.monitor_count,
            "screenshot_hash": self.screenshot_hash,
            "timestamp": self.timestamp.isoformat(),
            "username": self.username,
            "client_id": self.client_id,
        }


@dataclass
class EventBatch:
    """A batch of events to be sent to the ingest API."""

    events: list[BaseEvent] = field(default_factory=list)
    batch_id: str = field(default_factory=lambda: f"batch_{int(time.time() * 1000)}")
    created_at: datetime = field(default_factory=datetime.now)

    def add_event(self, event: BaseEvent) -> None:
        """Add an event to this batch."""
        self.events.append(event)

    def size(self) -> int:
        """Return the number of events in this batch."""
        return len(self.events)

    def to_dict(self) -> Dict[str, Any]:
        """Convert batch to dictionary for API payload."""
        return {
            "batch_id": self.batch_id,
            "created_at": self.created_at.isoformat(),
            "events": [event.to_batch_item() for event in self.events],
            "event_count": len(self.events),
        }
