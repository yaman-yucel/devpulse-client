"""Write-Ahead Log (WAL) for durable event storage using SQLite.

This module provides persistent storage for events when the in-memory queue
overflows or when durability is required. It supports recovery after crashes
and maintains order guarantees. Enhanced with window session tracking for
robust crash recovery.
"""

from __future__ import annotations

import gzip
import json
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from ..core.events import ActivityEvent, BaseEvent, EventType, HeartbeatEvent, ScreenshotEvent, WindowEvent


@dataclass
class WALConfig:
    """Configuration for the Write-Ahead Log."""

    db_path: Path = Path("devpulse_wal.db")
    max_batch_size: int = 100  # Maximum events to read/write in one operation
    checkpoint_interval: int = 1000  # Checkpoint WAL after N operations
    vacuum_threshold: int = 10000  # VACUUM database after N deleted records
    enable_wal_mode: bool = True  # Use SQLite WAL mode for better concurrency
    auto_flush_interval: float = 5.0  # Auto-flush events every N seconds
    window_session_timeout: int = 300  # Consider window sessions stale after N seconds

    # WAL Rotation settings
    max_wal_size_mb: int = 100  # Maximum WAL size before rotation (MB)
    max_archived_files: int = 10  # Maximum number of archived WAL files to keep
    archive_directory: Optional[Path] = None  # Directory for archived WAL files
    rotation_check_interval: int = 3600  # Check for rotation every N seconds
    max_unprocessed_events: int = 50000  # Maximum unprocessed events before rotation
    enable_compression: bool = True  # Compress archived WAL files


@dataclass
class ActiveWindowSession:
    """Represents an active window session for crash recovery."""

    session_id: str
    window_title: str
    start_time: datetime
    last_activity: datetime
    client_id: str
    username: str


class EventWAL:
    """SQLite-based Write-Ahead Log for event persistence with enhanced crash recovery."""

    def __init__(self, config: WALConfig = WALConfig()):
        """Initialize the WAL.

        Args:
            config: WAL configuration
        """
        self.config = config
        self._lock = threading.RLock()
        self._operation_count = 0
        self._deleted_count = 0
        self._auto_flush_thread: Optional[threading.Thread] = None
        self._rotation_thread: Optional[threading.Thread] = None
        self._shutdown_flag = threading.Event()
        self._last_rotation_check = time.time()

        # Create database directory if needed
        self.config.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Set up archive directory
        if self.config.archive_directory is None:
            self.config.archive_directory = self.config.db_path.parent / "wal_archives"
        self.config.archive_directory.mkdir(parents=True, exist_ok=True)

        # Initialize database
        self._init_database()

        # Start background threads
        self._start_auto_flush()
        self._start_rotation_monitor()

    def _init_database(self) -> None:
        """Initialize the SQLite database with the required schema."""
        with sqlite3.connect(self.config.db_path) as conn:
            # Enable WAL mode for better concurrency
            if self.config.enable_wal_mode:
                conn.execute("PRAGMA journal_mode=WAL")

            # Enable foreign keys for data integrity
            conn.execute("PRAGMA foreign_keys=ON")

            # Create events table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sequence_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    event_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT FALSE,
                    retry_count INTEGER DEFAULT 0
                )
            """)

            # Create index for efficient queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_processed_sequence 
                ON events(processed, sequence_id)
            """)

            # Create sequence counter table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS wal_metadata (
                    key TEXT PRIMARY KEY,
                    value INTEGER
                )
            """)

            # Create active window sessions table for crash recovery
            conn.execute("""
                CREATE TABLE IF NOT EXISTS active_window_sessions (
                    session_id TEXT PRIMARY KEY,
                    window_title TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    last_activity TIMESTAMP NOT NULL,
                    client_id TEXT NOT NULL,
                    username TEXT NOT NULL
                )
            """)

            # Create pending window events table (for incomplete sessions)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_window_events (
                    session_id TEXT PRIMARY KEY,
                    wal_event_id INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (wal_event_id) REFERENCES events(id)
                )
            """)

            # Initialize sequence counter if not exists
            conn.execute("""
                INSERT OR IGNORE INTO wal_metadata (key, value) 
                VALUES ('next_sequence_id', 1)
            """)

            conn.commit()
            logger.info(f"Initialized enhanced WAL database at {self.config.db_path}")

    def _start_auto_flush(self) -> None:
        """Start the auto-flush thread for periodic WAL safety."""
        if self.config.auto_flush_interval > 0:
            self._auto_flush_thread = threading.Thread(target=self._auto_flush_loop, daemon=True, name="WAL-AutoFlush")
            self._auto_flush_thread.start()
            logger.debug(f"Started auto-flush thread with {self.config.auto_flush_interval}s interval")

    def _auto_flush_loop(self) -> None:
        """Auto-flush loop to ensure WAL is frequently checkpointed."""
        while not self._shutdown_flag.wait(self.config.auto_flush_interval):
            try:
                self._force_checkpoint()
            except Exception as e:
                logger.error(f"Error in auto-flush loop: {e}")

    def _force_checkpoint(self) -> None:
        """Force a WAL checkpoint for crash safety."""
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                logger.debug("Performed WAL checkpoint")
        except Exception as e:
            logger.error(f"Failed to perform WAL checkpoint: {e}")

    def _start_rotation_monitor(self) -> None:
        """Start the rotation monitoring thread."""
        if self.config.rotation_check_interval > 0:
            self._rotation_thread = threading.Thread(target=self._rotation_monitor_loop, daemon=True, name="WAL-RotationMonitor")
            self._rotation_thread.start()
            logger.debug(f"Started rotation monitor with {self.config.rotation_check_interval}s interval")

    def _rotation_monitor_loop(self) -> None:
        """Monitor WAL for rotation conditions."""
        while not self._shutdown_flag.wait(self.config.rotation_check_interval):
            try:
                current_time = time.time()

                # Only check if enough time has passed
                if current_time - self._last_rotation_check >= self.config.rotation_check_interval:
                    self._check_rotation_conditions()
                    self._last_rotation_check = current_time

            except Exception as e:
                logger.error(f"Error in rotation monitor loop: {e}")

    def _check_rotation_conditions(self) -> None:
        """Check if WAL rotation is needed and perform if necessary."""
        try:
            # Check database size
            db_size_mb = self._get_database_size_mb()

            # Check unprocessed event count
            unprocessed_count = self._get_unprocessed_event_count()

            # Check if rotation is needed
            needs_rotation = db_size_mb >= self.config.max_wal_size_mb or unprocessed_count >= self.config.max_unprocessed_events

            if needs_rotation:
                logger.info(
                    f"WAL rotation triggered - Size: {db_size_mb:.1f}MB (limit: {self.config.max_wal_size_mb}MB), Unprocessed: {unprocessed_count} (limit: {self.config.max_unprocessed_events})"
                )
                self._perform_wal_rotation()
            else:
                logger.debug(f"WAL rotation check - Size: {db_size_mb:.1f}MB, Unprocessed: {unprocessed_count} - No rotation needed")

        except Exception as e:
            logger.error(f"Error checking rotation conditions: {e}")

    def _get_database_size_mb(self) -> float:
        """Get current database size in MB."""
        try:
            size_bytes = self.config.db_path.stat().st_size
            return size_bytes / (1024 * 1024)
        except Exception:
            return 0.0

    def _get_unprocessed_event_count(self) -> int:
        """Get count of unprocessed events."""
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM events WHERE processed = FALSE")
                return cursor.fetchone()[0]
        except Exception:
            return 0

    def _perform_wal_rotation(self) -> bool:
        """Perform WAL rotation by archiving current WAL and creating a new one.

        Returns:
            True if rotation successful, False otherwise
        """
        try:
            with self._lock:
                logger.info("Starting WAL rotation...")

                # Generate archive filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_name = f"wal_archive_{timestamp}.db"
                archive_path = self.config.archive_directory / archive_name

                # Force a final checkpoint
                self._force_checkpoint()

                # Copy current WAL to archive
                shutil.copy2(self.config.db_path, archive_path)

                # Compress if enabled
                if self.config.enable_compression:
                    compressed_path = archive_path.with_suffix(".db.gz")
                    with open(archive_path, "rb") as f_in:
                        with gzip.open(compressed_path, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)

                    # Remove uncompressed file
                    archive_path.unlink()
                    archive_path = compressed_path
                    logger.info(f"Compressed archive created: {archive_path}")
                else:
                    logger.info(f"Archive created: {archive_path}")

                # Clean up old processed events from current WAL
                deleted_count = self._cleanup_processed_events_for_rotation()

                # Vacuum the database to reclaim space
                self._vacuum_database()

                # Clean up old archives
                self._cleanup_old_archives()

                logger.info(f"WAL rotation completed - Archived to {archive_path.name}, cleaned {deleted_count} processed events")

                return True

        except Exception as e:
            logger.error(f"Failed to perform WAL rotation: {e}")
            return False

    def _cleanup_processed_events_for_rotation(self) -> int:
        """Clean up all processed events during rotation.

        Returns:
            Number of events cleaned up
        """
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                cursor = conn.cursor()

                # Delete all processed events (they're now archived)
                cursor.execute("DELETE FROM events WHERE processed = TRUE")
                deleted_count = cursor.rowcount

                conn.commit()
                return deleted_count

        except Exception as e:
            logger.error(f"Failed to cleanup processed events: {e}")
            return 0

    def _vacuum_database(self) -> None:
        """Vacuum the database to reclaim space."""
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute("VACUUM")
            logger.debug("Database vacuumed after rotation")
        except Exception as e:
            logger.error(f"Failed to vacuum database: {e}")

    def _cleanup_old_archives(self) -> None:
        """Clean up old archive files beyond the retention limit."""
        try:
            # Get all archive files
            if self.config.enable_compression:
                archives = list(self.config.archive_directory.glob("wal_archive_*.db.gz"))
            else:
                archives = list(self.config.archive_directory.glob("wal_archive_*.db"))

            # Sort by creation time (newest first)
            archives.sort(key=lambda p: p.stat().st_ctime, reverse=True)

            # Remove archives beyond the limit
            for archive in archives[self.config.max_archived_files :]:
                try:
                    archive.unlink()
                    logger.info(f"Removed old archive: {archive.name}")
                except Exception as e:
                    logger.error(f"Failed to remove old archive {archive.name}: {e}")

        except Exception as e:
            logger.error(f"Failed to cleanup old archives: {e}")

    def force_rotation(self) -> bool:
        """Force WAL rotation immediately.

        Returns:
            True if rotation successful, False otherwise
        """
        logger.info("Forcing WAL rotation...")
        return self._perform_wal_rotation()

    def get_rotation_stats(self) -> Dict[str, Any]:
        """Get WAL rotation statistics.

        Returns:
            Dictionary with rotation statistics
        """
        try:
            db_size_mb = self._get_database_size_mb()
            unprocessed_count = self._get_unprocessed_event_count()

            # Count archive files
            if self.config.enable_compression:
                archives = list(self.config.archive_directory.glob("wal_archive_*.db.gz"))
            else:
                archives = list(self.config.archive_directory.glob("wal_archive_*.db"))

            # Calculate total archive size
            total_archive_size_mb = sum(archive.stat().st_size for archive in archives) / (1024 * 1024)

            return {
                "current_db_size_mb": db_size_mb,
                "max_db_size_mb": self.config.max_wal_size_mb,
                "unprocessed_events": unprocessed_count,
                "max_unprocessed_events": self.config.max_unprocessed_events,
                "rotation_needed": (db_size_mb >= self.config.max_wal_size_mb or unprocessed_count >= self.config.max_unprocessed_events),
                "archive_count": len(archives),
                "max_archives": self.config.max_archived_files,
                "total_archive_size_mb": total_archive_size_mb,
                "compression_enabled": self.config.enable_compression,
                "next_rotation_check": (self._last_rotation_check + self.config.rotation_check_interval - time.time()),
            }

        except Exception as e:
            logger.error(f"Failed to get rotation stats: {e}")
            return {}

    def start_window_session(self, session_id: str, window_title: str, client_id: str, username: str, start_time: Optional[datetime] = None) -> bool:
        """Start tracking a window session for crash recovery.

        Args:
            session_id: Unique session identifier
            window_title: Title of the window
            client_id: Client identifier
            username: Username
            start_time: Session start time (defaults to now)

        Returns:
            True if successful, False otherwise
        """
        start_time = start_time or datetime.now()

        try:
            with self._lock:
                with sqlite3.connect(self.config.db_path) as conn:
                    # Insert or update active session
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO active_window_sessions 
                        (session_id, window_title, start_time, last_activity, client_id, username)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """,
                        (session_id, window_title, start_time, start_time, client_id, username),
                    )

                    conn.commit()
                    logger.debug(f"Started window session tracking: {session_id} - '{window_title}'")
                    return True

        except Exception as e:
            logger.error(f"Failed to start window session tracking: {e}")
            return False

    def update_window_session_activity(self, session_id: str) -> bool:
        """Update the last activity time for a window session.

        Args:
            session_id: Session identifier

        Returns:
            True if successful, False otherwise
        """
        try:
            with self._lock:
                with sqlite3.connect(self.config.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        UPDATE active_window_sessions 
                        SET last_activity = CURRENT_TIMESTAMP 
                        WHERE session_id = ?
                    """,
                        (session_id,),
                    )

                    if cursor.rowcount > 0:
                        conn.commit()
                        return True
                    else:
                        logger.warning(f"Window session not found for update: {session_id}")
                        return False

        except Exception as e:
            logger.error(f"Failed to update window session activity: {e}")
            return False

    def end_window_session(self, session_id: str) -> Optional[ActiveWindowSession]:
        """End a window session and return its details.

        Args:
            session_id: Session identifier

        Returns:
            ActiveWindowSession if found, None otherwise
        """
        try:
            with self._lock:
                with sqlite3.connect(self.config.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()

                    # Get session details
                    cursor.execute(
                        """
                        SELECT session_id, window_title, start_time, last_activity, client_id, username
                        FROM active_window_sessions 
                        WHERE session_id = ?
                    """,
                        (session_id,),
                    )

                    row = cursor.fetchone()
                    if not row:
                        logger.warning(f"Window session not found for ending: {session_id}")
                        return None

                    session = ActiveWindowSession(
                        session_id=row["session_id"],
                        window_title=row["window_title"],
                        start_time=datetime.fromisoformat(row["start_time"]),
                        last_activity=datetime.fromisoformat(row["last_activity"]),
                        client_id=row["client_id"],
                        username=row["username"],
                    )

                    # Remove from active sessions
                    cursor.execute("DELETE FROM active_window_sessions WHERE session_id = ?", (session_id,))

                    # Remove any pending events
                    cursor.execute("DELETE FROM pending_window_events WHERE session_id = ?", (session_id,))

                    conn.commit()
                    logger.debug(f"Ended window session: {session_id}")
                    return session

        except Exception as e:
            logger.error(f"Failed to end window session: {e}")
            return None

    def get_active_window_sessions(self) -> List[ActiveWindowSession]:
        """Get all active window sessions.

        Returns:
            List of active window sessions
        """
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT session_id, window_title, start_time, last_activity, client_id, username
                    FROM active_window_sessions
                    ORDER BY start_time
                """)

                sessions = []
                for row in cursor.fetchall():
                    sessions.append(
                        ActiveWindowSession(
                            session_id=row["session_id"],
                            window_title=row["window_title"],
                            start_time=datetime.fromisoformat(row["start_time"]),
                            last_activity=datetime.fromisoformat(row["last_activity"]),
                            client_id=row["client_id"],
                            username=row["username"],
                        )
                    )

                return sessions

        except Exception as e:
            logger.error(f"Failed to get active window sessions: {e}")
            return []

    def recover_incomplete_window_sessions(self) -> List[WindowEvent]:
        """Recover incomplete window sessions after a crash.

        This method should be called on startup to handle any window sessions
        that were active when the client crashed or was forcibly terminated.

        Returns:
            List of recovery window events to emit
        """
        recovery_events = []

        try:
            with self._lock:
                active_sessions = self.get_active_window_sessions()
                current_time = datetime.now()

                for session in active_sessions:
                    try:
                        # Calculate session duration
                        duration = (session.last_activity - session.start_time).total_seconds()

                        # Only recover sessions that had meaningful duration
                        if duration > 1.0:  # At least 1 second
                            recovery_event = WindowEvent(
                                timestamp=session.last_activity,
                                username=session.username,
                                client_id=session.client_id,
                                window_title=session.window_title,
                                start_time=session.start_time,
                                end_time=session.last_activity,
                                duration=duration,
                            )

                            recovery_events.append(recovery_event)
                            logger.info(f"Recovered window session: '{session.window_title}' ({duration:.1f}s)")
                        else:
                            logger.debug(f"Skipped short window session: '{session.window_title}' ({duration:.1f}s)")

                        # Clean up the session
                        self.end_window_session(session.session_id)

                    except Exception as e:
                        logger.error(f"Error recovering window session {session.session_id}: {e}")

                if recovery_events:
                    logger.info(f"Recovered {len(recovery_events)} window sessions from crash")
                else:
                    logger.info("No window sessions to recover")

        except Exception as e:
            logger.error(f"Failed to recover window sessions: {e}")

        return recovery_events

    def cleanup_stale_sessions(self) -> int:
        """Clean up stale window sessions based on timeout.

        Returns:
            Number of sessions cleaned up
        """
        try:
            with self._lock:
                with sqlite3.connect(self.config.db_path) as conn:
                    cursor = conn.cursor()

                    # Delete sessions older than timeout
                    cursor.execute(
                        """
                        DELETE FROM active_window_sessions 
                        WHERE last_activity < datetime('now', '-{} seconds')
                    """.format(self.config.window_session_timeout)
                    )

                    cleaned_count = cursor.rowcount

                    # Also clean up associated pending events
                    cursor.execute("""
                        DELETE FROM pending_window_events 
                        WHERE session_id NOT IN (SELECT session_id FROM active_window_sessions)
                    """)

                    conn.commit()

                    if cleaned_count > 0:
                        logger.info(f"Cleaned up {cleaned_count} stale window sessions")

                    return cleaned_count

        except Exception as e:
            logger.error(f"Failed to cleanup stale sessions: {e}")
            return 0

    def write_events(self, events: List[BaseEvent]) -> bool:
        """Write events to the WAL.

        Args:
            events: List of events to write

        Returns:
            True if successful, False otherwise
        """
        if not events:
            return True

        try:
            with self._lock:
                with sqlite3.connect(self.config.db_path) as conn:
                    cursor = conn.cursor()

                    # Get next sequence IDs
                    cursor.execute("UPDATE wal_metadata SET value = value + ? WHERE key = 'next_sequence_id'", (len(events),))

                    cursor.execute("SELECT value FROM wal_metadata WHERE key = 'next_sequence_id'")
                    next_sequence_id = cursor.fetchone()[0]
                    start_sequence_id = next_sequence_id - len(events)

                    # Insert events
                    event_rows = []
                    for i, event in enumerate(events):
                        event_rows.append(
                            (
                                start_sequence_id + i,
                                event.event_type.value,
                                json.dumps(event.to_dict()),
                            )
                        )

                    cursor.executemany("INSERT INTO events (sequence_id, event_type, event_data) VALUES (?, ?, ?)", event_rows)

                    conn.commit()
                    self._operation_count += len(events)

                    logger.debug(f"Wrote {len(events)} events to WAL")
                    self._maybe_checkpoint()

                    return True

        except Exception as e:
            logger.error(f"Failed to write events to WAL: {e}")
            return False

    def read_unprocessed_events(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Read unprocessed events from the WAL.

        Args:
            limit: Maximum number of events to read

        Returns:
            List of event dictionaries with metadata
        """
        limit = limit or self.config.max_batch_size

        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT id, sequence_id, event_type, event_data, created_at, retry_count
                    FROM events 
                    WHERE processed = FALSE 
                    ORDER BY sequence_id 
                    LIMIT ?
                """,
                    (limit,),
                )

                rows = cursor.fetchall()
                events = []

                for row in rows:
                    try:
                        event_data = json.loads(row["event_data"])
                        events.append(
                            {
                                "wal_id": row["id"],
                                "sequence_id": row["sequence_id"],
                                "event_type": row["event_type"],
                                "event_data": event_data,
                                "created_at": row["created_at"],
                                "retry_count": row["retry_count"],
                            }
                        )
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse event data for WAL ID {row['id']}: {e}")
                        continue

                if events:
                    logger.debug(f"Read {len(events)} unprocessed events from WAL")

                return events

        except Exception as e:
            logger.error(f"Failed to read events from WAL: {e}")
            return []

    def mark_events_processed(self, wal_ids: List[int]) -> bool:
        """Mark events as processed (successfully sent).

        Args:
            wal_ids: List of WAL IDs to mark as processed

        Returns:
            True if successful, False otherwise
        """
        if not wal_ids:
            return True

        try:
            with self._lock:
                with sqlite3.connect(self.config.db_path) as conn:
                    placeholders = ",".join("?" * len(wal_ids))
                    conn.execute(f"UPDATE events SET processed = TRUE WHERE id IN ({placeholders})", wal_ids)
                    conn.commit()

                    self._operation_count += len(wal_ids)
                    logger.debug(f"Marked {len(wal_ids)} events as processed")

                    return True

        except Exception as e:
            logger.error(f"Failed to mark events as processed: {e}")
            return False

    def increment_retry_count(self, wal_ids: List[int]) -> bool:
        """Increment retry count for failed events.

        Args:
            wal_ids: List of WAL IDs to increment retry count for

        Returns:
            True if successful, False otherwise
        """
        if not wal_ids:
            return True

        try:
            with self._lock:
                with sqlite3.connect(self.config.db_path) as conn:
                    placeholders = ",".join("?" * len(wal_ids))
                    conn.execute(f"UPDATE events SET retry_count = retry_count + 1 WHERE id IN ({placeholders})", wal_ids)
                    conn.commit()

                    logger.debug(f"Incremented retry count for {len(wal_ids)} events")
                    return True

        except Exception as e:
            logger.error(f"Failed to increment retry count: {e}")
            return False

    def cleanup_processed_events(self, older_than_hours: int = 24) -> int:
        """Clean up processed events older than specified hours.

        Args:
            older_than_hours: Remove processed events older than this many hours

        Returns:
            Number of events deleted
        """
        try:
            with self._lock:
                with sqlite3.connect(self.config.db_path) as conn:
                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        DELETE FROM events 
                        WHERE processed = TRUE 
                        AND created_at < datetime('now', '-{} hours')
                    """.format(older_than_hours)
                    )

                    deleted_count = cursor.rowcount
                    conn.commit()

                    self._deleted_count += deleted_count
                    self._operation_count += deleted_count

                    if deleted_count > 0:
                        logger.info(f"Cleaned up {deleted_count} processed events from WAL")

                    self._maybe_vacuum()
                    return deleted_count

        except Exception as e:
            logger.error(f"Failed to cleanup processed events: {e}")
            return 0

    def get_stats(self) -> Dict[str, Any]:
        """Get WAL statistics.

        Returns:
            Dictionary with WAL statistics
        """
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                cursor = conn.cursor()

                # Count total events
                cursor.execute("SELECT COUNT(*) FROM events")
                total_events = cursor.fetchone()[0]

                # Count unprocessed events
                cursor.execute("SELECT COUNT(*) FROM events WHERE processed = FALSE")
                unprocessed_events = cursor.fetchone()[0]

                # Count events by type
                cursor.execute("""
                    SELECT event_type, COUNT(*) as count 
                    FROM events 
                    WHERE processed = FALSE 
                    GROUP BY event_type
                """)
                events_by_type = dict(cursor.fetchall())

                # Count active window sessions
                cursor.execute("SELECT COUNT(*) FROM active_window_sessions")
                active_sessions = cursor.fetchone()[0]

                # Get database size
                cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
                db_size = cursor.fetchone()[0]

                # Get rotation stats
                rotation_stats = self.get_rotation_stats()

                return {
                    "total_events": total_events,
                    "unprocessed_events": unprocessed_events,
                    "processed_events": total_events - unprocessed_events,
                    "events_by_type": events_by_type,
                    "active_window_sessions": active_sessions,
                    "database_size_bytes": db_size,
                    "database_size_mb": db_size / (1024 * 1024),
                    "operation_count": self._operation_count,
                    "deleted_count": self._deleted_count,
                    "auto_flush_enabled": self.config.auto_flush_interval > 0,
                    "rotation_enabled": self.config.rotation_check_interval > 0,
                    "rotation": rotation_stats,
                }

        except Exception as e:
            logger.error(f"Failed to get WAL stats: {e}")
            return {}

    def _maybe_checkpoint(self) -> None:
        """Checkpoint WAL if threshold is reached."""
        if self._operation_count >= self.config.checkpoint_interval:
            try:
                with sqlite3.connect(self.config.db_path) as conn:
                    conn.execute("PRAGMA wal_checkpoint")
                    self._operation_count = 0
                    logger.debug("Checkpointed WAL")
            except Exception as e:
                logger.error(f"Failed to checkpoint WAL: {e}")

    def _maybe_vacuum(self) -> None:
        """Vacuum database if delete threshold is reached."""
        if self._deleted_count >= self.config.vacuum_threshold:
            try:
                with sqlite3.connect(self.config.db_path) as conn:
                    conn.execute("VACUUM")
                    self._deleted_count = 0
                    logger.info("Vacuumed WAL database")
            except Exception as e:
                logger.error(f"Failed to vacuum WAL database: {e}")

    def close(self) -> None:
        """Close the WAL and perform final cleanup."""
        try:
            # Signal shutdown to background threads
            self._shutdown_flag.set()

            # Wait for auto-flush thread to complete
            if self._auto_flush_thread and self._auto_flush_thread.is_alive():
                self._auto_flush_thread.join(timeout=5.0)
                if self._auto_flush_thread.is_alive():
                    logger.warning("Auto-flush thread did not stop gracefully")

            # Wait for rotation thread to complete
            if self._rotation_thread and self._rotation_thread.is_alive():
                self._rotation_thread.join(timeout=5.0)
                if self._rotation_thread.is_alive():
                    logger.warning("Rotation thread did not stop gracefully")

            with self._lock:
                # Clean up any stale sessions before closing
                stale_count = self.cleanup_stale_sessions()
                if stale_count > 0:
                    logger.info(f"Cleaned up {stale_count} stale sessions during shutdown")

                # Final checkpoint to ensure all data is persisted
                with sqlite3.connect(self.config.db_path) as conn:
                    if self.config.enable_wal_mode:
                        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

                logger.info("Closed enhanced WAL with rotation support")

        except Exception as e:
            logger.error(f"Error closing WAL: {e}")


def _recreate_event_from_dict(event_type: str, event_data: Dict[str, Any]) -> Optional[BaseEvent]:
    """Recreate an event object from dictionary data.

    Args:
        event_type: Event type string
        event_data: Event data dictionary

    Returns:
        Recreated event object or None if failed
    """
    try:
        # Parse timestamp
        timestamp = datetime.fromisoformat(event_data["timestamp"])

        if event_type == EventType.ACTIVITY:
            from ..core.events import ActivityEventType

            return ActivityEvent(
                timestamp=timestamp,
                username=event_data.get("username", ""),
                client_id=event_data.get("client_id", ""),
                activity_type=ActivityEventType(event_data["activity_type"]),
            )
        elif event_type == EventType.HEARTBEAT:
            from ..core.events import HeartbeatType

            return HeartbeatEvent(
                timestamp=timestamp,
                username=event_data.get("username", ""),
                client_id=event_data.get("client_id", ""),
                heartbeat_type=HeartbeatType(event_data["heartbeat_type"]),
            )
        elif event_type == EventType.WINDOW:
            return WindowEvent(
                timestamp=timestamp,
                username=event_data.get("username", ""),
                client_id=event_data.get("client_id", ""),
                window_title=event_data["window_title"],
                start_time=datetime.fromisoformat(event_data["start_time"]) if event_data.get("start_time") else None,
                end_time=datetime.fromisoformat(event_data["end_time"]) if event_data.get("end_time") else None,
                duration=event_data.get("duration"),
            )
        elif event_type == EventType.SCREENSHOT:
            return ScreenshotEvent(
                timestamp=timestamp,
                username=event_data.get("username", ""),
                client_id=event_data.get("client_id", ""),
                screenshot_path=event_data["screenshot_path"],
                monitor_count=event_data.get("monitor_count", 1),
                screenshot_hash=event_data.get("screenshot_hash"),
            )
        else:
            logger.error(f"Unknown event type: {event_type}")
            return None

    except Exception as e:
        logger.error(f"Failed to recreate event from dict: {e}")
        return None
