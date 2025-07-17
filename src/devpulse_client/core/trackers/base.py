"""Base classes and interfaces for DevPulse trackers."""

from __future__ import annotations

from typing import Protocol

from ...config.settings import DevPulseConfig


class EventEmitter(Protocol):
    """Protocol for emitting events to the pipeline."""

    def emit(self, event) -> bool:
        """Emit an event to the pipeline."""
        ...


class BaseTracker:
    """Base class for all DevPulse trackers."""

    def __init__(self, config: DevPulseConfig, event_emitter: EventEmitter):
        """Initialize base tracker.

        Args:
            config: DevPulse configuration
            event_emitter: Event emitter for the pipeline
        """
        self.config = config
        self.event_emitter = event_emitter

    def tick(self) -> None:
        """Perform one tracking cycle. Override in subclasses."""
        pass

    def initialize(self) -> None:
        """Initialize the tracker. Override in subclasses if needed."""
        pass

    def shutdown(self) -> None:
        """Shutdown the tracker. Override in subclasses if needed."""
        pass
