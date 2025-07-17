"""Core DevPulse client components - Clean architecture without legacy dependencies."""

from .clean_app import DevPulseClient, create_devpulse_client
from .events import ActivityEvent, ActivityEventType, BaseEvent, EventBatch, EventType, HeartbeatEvent, HeartbeatType, ScreenshotEvent, WindowEvent
from .trackers import ActivityTracker, HeartbeatTracker, ScreenshotTracker, WindowTracker

__all__ = [
    # Event architecture
    "BaseEvent",
    "ActivityEvent",
    "ActivityEventType",
    "HeartbeatEvent",
    "HeartbeatType",
    "WindowEvent",
    "ScreenshotEvent",
    "EventBatch",
    "EventType",
    # Clean tracking components
    "ActivityTracker",
    "HeartbeatTracker",
    "WindowTracker",
    "ScreenshotTracker",
    # Clean application
    "DevPulseClient",
    "create_devpulse_client",
]
