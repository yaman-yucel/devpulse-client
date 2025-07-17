from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from loguru import logger

from tracker.db.event_store import EventStore
from tracker.tables.heartbeat_table import HeartbeatType


@dataclass
class HeartbeatTask:
    interval: int
    _last: float | None = None

    def tick(self) -> None:
        now = time.time()
        if self._last is None or now - self._last >= self.interval:
            self._last = now
            logger.info("Regular heartbeat")
            EventStore.heartbeat(timestamp=datetime.fromtimestamp(now), type=HeartbeatType.REGULAR)

    def shutdown(self, now: float) -> None:
        logger.info("Sending final heartbeat")
        EventStore.heartbeat(timestamp=datetime.fromtimestamp(now), type=HeartbeatType.FINAL)
