"""Test script for enhanced WAL functionality.

This script demonstrates the improved WAL capabilities including:
- Window session tracking
- Crash recovery
- Auto-flush functionality
- Stale session cleanup
"""

import tempfile
import time
from datetime import datetime
from pathlib import Path

from loguru import logger

from src.devpulse_client.core.events import WindowEvent
from src.devpulse_client.wal import ActiveWindowSession, EventWAL, WALConfig


def test_window_session_tracking():
    """Test window session tracking functionality."""
    logger.info("Testing window session tracking...")

    # Create temporary WAL database
    with tempfile.TemporaryDirectory() as temp_dir:
        wal_config = WALConfig(
            db_path=Path(temp_dir) / "test_wal.db",
            auto_flush_interval=1.0,  # Flush every second for testing
            window_session_timeout=60,  # 1 minute timeout
        )

        wal = EventWAL(wal_config)

        try:
            # Start a window session
            session_id = "test_session_1"
            start_time = datetime.now()

            success = wal.start_window_session(session_id=session_id, window_title="Test Window - Visual Studio Code", client_id="test_client", username="test_user", start_time=start_time)

            assert success, "Failed to start window session"
            logger.info("✓ Window session started successfully")

            # Get active sessions
            active_sessions = wal.get_active_window_sessions()
            assert len(active_sessions) == 1, f"Expected 1 active session, got {len(active_sessions)}"

            session = active_sessions[0]
            assert session.session_id == session_id
            assert session.window_title == "Test Window - Visual Studio Code"
            logger.info("✓ Active session retrieved successfully")

            # Update activity
            time.sleep(0.1)
            success = wal.update_window_session_activity(session_id)
            assert success, "Failed to update session activity"
            logger.info("✓ Session activity updated successfully")

            # End the session
            ended_session = wal.end_window_session(session_id)
            assert ended_session is not None, "Failed to end session"
            assert ended_session.session_id == session_id
            logger.info("✓ Window session ended successfully")

            # Verify no active sessions remain
            active_sessions = wal.get_active_window_sessions()
            assert len(active_sessions) == 0, f"Expected 0 active sessions, got {len(active_sessions)}"
            logger.info("✓ No active sessions remain")

        finally:
            wal.close()

    logger.info("✓ Window session tracking test passed!")


def test_crash_recovery():
    """Test crash recovery functionality."""
    logger.info("Testing crash recovery...")

    with tempfile.TemporaryDirectory() as temp_dir:
        wal_path = Path(temp_dir) / "crash_test_wal.db"
        wal_config = WALConfig(db_path=wal_path, auto_flush_interval=0.5, window_session_timeout=300)

        # Simulate a crash scenario
        logger.info("Simulating application crash...")

        # First session: start some window sessions and "crash"
        wal1 = EventWAL(wal_config)

        try:
            # Start multiple window sessions
            sessions = [("session_1", "Firefox - GitHub", datetime.now()), ("session_2", "Terminal - /home/user", datetime.now()), ("session_3", "Slack - General", datetime.now())]

            for session_id, title, start_time in sessions:
                success = wal1.start_window_session(session_id=session_id, window_title=title, client_id="test_client", username="test_user", start_time=start_time)
                assert success, f"Failed to start session {session_id}"

            # Simulate some activity
            time.sleep(0.2)
            for session_id, _, _ in sessions:
                wal1.update_window_session_activity(session_id)

            # Verify sessions are active
            active_sessions = wal1.get_active_window_sessions()
            assert len(active_sessions) == 3, f"Expected 3 active sessions, got {len(active_sessions)}"
            logger.info("✓ Set up crash scenario with 3 active sessions")

        finally:
            # Simulate crash by closing without proper cleanup
            wal1._shutdown_flag.set()  # Stop auto-flush
            wal1._force_checkpoint()  # Ensure data is persisted

        # Second session: recover from crash
        logger.info("Simulating application restart and recovery...")

        wal2 = EventWAL(wal_config)

        try:
            # Perform crash recovery
            recovery_events = wal2.recover_incomplete_window_sessions()

            assert len(recovery_events) == 3, f"Expected 3 recovery events, got {len(recovery_events)}"
            logger.info(f"✓ Recovered {len(recovery_events)} window sessions")

            # Verify recovery events have correct data
            for event in recovery_events:
                assert isinstance(event, WindowEvent)
                assert event.start_time is not None
                assert event.end_time is not None
                assert event.duration is not None
                assert event.duration > 0
                logger.info(f"✓ Recovered session: '{event.window_title}' ({event.duration:.2f}s)")

            # Verify no active sessions remain after recovery
            active_sessions = wal2.get_active_window_sessions()
            assert len(active_sessions) == 0, f"Expected 0 active sessions after recovery, got {len(active_sessions)}"
            logger.info("✓ All sessions cleaned up after recovery")

        finally:
            wal2.close()

    logger.info("✓ Crash recovery test passed!")


def test_auto_flush_and_maintenance():
    """Test auto-flush and maintenance functionality."""
    logger.info("Testing auto-flush and maintenance...")

    with tempfile.TemporaryDirectory() as temp_dir:
        wal_config = WALConfig(
            db_path=Path(temp_dir) / "maintenance_test_wal.db",
            auto_flush_interval=0.5,  # Very frequent for testing
            window_session_timeout=2,  # Short timeout for testing
        )

        wal = EventWAL(wal_config)

        try:
            # Test auto-flush
            logger.info("Testing auto-flush...")

            # Start a session
            wal.start_window_session(session_id="maintenance_test", window_title="Test Window", client_id="test_client", username="test_user")

            # Wait for auto-flush
            time.sleep(1.0)
            logger.info("✓ Auto-flush completed (no errors)")

            # Test stale session cleanup
            logger.info("Testing stale session cleanup...")

            # Wait for session to become stale
            time.sleep(2.5)  # Wait longer than timeout

            # Clean up stale sessions
            stale_count = wal.cleanup_stale_sessions()
            assert stale_count == 1, f"Expected 1 stale session, cleaned {stale_count}"
            logger.info("✓ Stale session cleanup successful")

            # Verify no active sessions remain
            active_sessions = wal.get_active_window_sessions()
            assert len(active_sessions) == 0, f"Expected 0 active sessions, got {len(active_sessions)}"
            logger.info("✓ All stale sessions removed")

        finally:
            wal.close()

    logger.info("✓ Auto-flush and maintenance test passed!")


def test_wal_stats():
    """Test WAL statistics functionality."""
    logger.info("Testing WAL statistics...")

    with tempfile.TemporaryDirectory() as temp_dir:
        wal_config = WALConfig(db_path=Path(temp_dir) / "stats_test_wal.db", auto_flush_interval=1.0)

        wal = EventWAL(wal_config)

        try:
            # Get initial stats
            stats = wal.get_stats()
            assert "total_events" in stats
            assert "active_window_sessions" in stats
            assert "auto_flush_enabled" in stats
            assert stats["auto_flush_enabled"] is True
            logger.info("✓ Initial stats retrieved")

            # Start some sessions
            for i in range(3):
                wal.start_window_session(session_id=f"stats_test_{i}", window_title=f"Test Window {i}", client_id="test_client", username="test_user")

            # Get updated stats
            stats = wal.get_stats()
            assert stats["active_window_sessions"] == 3
            logger.info(f"✓ Stats show {stats['active_window_sessions']} active sessions")

            # Write some events
            events = [WindowEvent(window_title="Stats Test Window", start_time=datetime.now(), end_time=datetime.now(), duration=10.0)]

            success = wal.write_events(events)
            assert success, "Failed to write events"

            # Get final stats
            stats = wal.get_stats()
            assert stats["total_events"] > 0
            logger.info(f"✓ Stats show {stats['total_events']} total events")

        finally:
            wal.close()

    logger.info("✓ WAL statistics test passed!")


def main():
    """Run all enhanced WAL tests."""
    logger.info("Starting enhanced WAL tests...")

    try:
        test_window_session_tracking()
        test_crash_recovery()
        test_auto_flush_and_maintenance()
        test_wal_stats()

        logger.info("🎉 All enhanced WAL tests passed successfully!")

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    main()
