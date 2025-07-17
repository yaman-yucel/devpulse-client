from __future__ import annotations

import time
from dataclasses import dataclass

from tracker.config.tracker_settings import tracker_settings

from .screenshot_capturer import ScreenshotCapturer


@dataclass
class ScreenshotTask:
    interval: int
    _capturer: ScreenshotCapturer = ScreenshotCapturer(tracker_settings.screenshot_dir)
    _last: float | None = None

    def tick(self) -> None:
        now = time.time()
        if self._last is None or now - self._last >= self.interval:
            self._last = now
            self._capturer.capture_all_monitors()
