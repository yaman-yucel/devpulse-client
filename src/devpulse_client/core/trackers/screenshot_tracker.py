"""Screenshot tracker for DevPulse client.

This module captures periodic screenshots for activity monitoring.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger

from ..events import ScreenshotEvent
from .base import BaseTracker


class ScreenshotTracker(BaseTracker):
    """Captures periodic screenshots."""

    def __init__(self, config, event_emitter):
        """Initialize screenshot tracker.

        Args:
            config: DevPulse configuration
            event_emitter: Event emitter for the pipeline
        """
        super().__init__(config, event_emitter)
        self._last_screenshot: Optional[float] = None

    def tick(self) -> None:
        """Capture screenshot if interval has elapsed."""
        now = time.time()

        if self._last_screenshot is None or now - self._last_screenshot >= self.config.tracker.screenshot_interval:
            self._capture_screenshot()
            self._last_screenshot = now

    def _capture_screenshot(self) -> None:
        """Capture a screenshot and emit event."""
        try:
            timestamp = datetime.now()
            filename = f"screenshot_{timestamp.strftime('%Y%m%d_%H%M%S')}.{self.config.tracker.image_format}"
            filepath = self.config.tracker.screenshot_dir / filename

            # Try to capture screenshot using available methods
            if self._capture_with_pillow(filepath):
                # Calculate hash for deduplication
                screenshot_hash = self._calculate_file_hash(filepath)

                event = ScreenshotEvent(
                    screenshot_path=str(filepath),
                    screenshot_hash=screenshot_hash,
                    username=self.config.username,
                    client_id=self.config.client_id,
                )

                success = self.event_emitter.emit(event)
                if not success:
                    logger.warning("Failed to emit screenshot event")
            else:
                logger.warning("Failed to capture screenshot")

        except Exception as e:
            logger.error(f"Error capturing screenshot: {e}")

    def _capture_with_pillow(self, filepath: Path) -> bool:
        """Capture screenshot using Pillow/PIL."""
        try:
            from PIL import ImageGrab

            screenshot = ImageGrab.grab()
            if self.config.tracker.image_format.lower() == "jpeg":
                screenshot.save(filepath, quality=self.config.tracker.image_quality)
            else:
                screenshot.save(filepath)

            return True

        except ImportError:
            logger.warning("PIL/Pillow not available for screenshots")
            return False
        except Exception as e:
            logger.error(f"Failed to capture screenshot with Pillow: {e}")
            return False

    def _calculate_file_hash(self, filepath: Path) -> str:
        """Calculate SHA-256 hash of file for deduplication."""
        try:
            hash_sha256 = hashlib.sha256()
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception:
            return ""
