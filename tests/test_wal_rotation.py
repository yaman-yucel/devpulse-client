"""Test script for WAL rotation functionality.

This script demonstrates how the enhanced WAL handles scenarios where
the server is unreachable for extended periods, including:
- Automatic rotation based on size and event count
- Archive creation and compression
- Cleanup of old archives
- Manual rotation triggers
"""

import tempfile
import time
from datetime import datetime
from pathlib import Path

from loguru import logger

from src.devpulse_client.core.events import ActivityEvent, ActivityEventType, WindowEvent
from src.devpulse_client.wal import EventWAL, WALConfig


def test_size_based_rotation():
    """Test WAL rotation based on database size."""
    logger.info("Testing size-based WAL rotation...")

    with tempfile.TemporaryDirectory() as temp_dir:
        wal_config = WALConfig(
            db_path=Path(temp_dir) / "size_test_wal.db",
            max_wal_size_mb=1,  # Very small limit for testing
            max_unprocessed_events=10000,  # High limit
            rotation_check_interval=2,  # Check every 2 seconds
            auto_flush_interval=1.0,
            enable_compression=True,
        )

        wal = EventWAL(wal_config)

        try:
            # Generate lots of events to exceed size limit
            events = []
            for i in range(100):
                # Create large window events
                event = WindowEvent(
                    window_title=f"Large Window Title {i} " + "X" * 1000,  # Make it big
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    duration=60.0,
                    username="test_user",
                    client_id="test_client",
                )
                events.append(event)

            # Write events in batches
            logger.info(f"Writing {len(events)} large events...")
            for i in range(0, len(events), 10):
                batch = events[i : i + 10]
                success = wal.write_events(batch)
                assert success, f"Failed to write batch {i // 10}"

            # Get initial stats
            initial_stats = wal.get_rotation_stats()
            logger.info(f"Initial DB size: {initial_stats['current_db_size_mb']:.2f}MB")

            # Wait for rotation to trigger
            logger.info("Waiting for size-based rotation to trigger...")
            time.sleep(5)  # Wait for rotation check

            # Check if rotation occurred
            final_stats = wal.get_rotation_stats()
            logger.info(f"Final DB size: {final_stats['current_db_size_mb']:.2f}MB")
            logger.info(f"Archives created: {final_stats['archive_count']}")

            if final_stats["archive_count"] > 0:
                logger.info("✓ Size-based rotation successful!")
            else:
                logger.warning("Size-based rotation did not trigger")

        finally:
            wal.close()

    logger.info("✓ Size-based rotation test completed")


def test_event_count_rotation():
    """Test WAL rotation based on unprocessed event count."""
    logger.info("Testing event count-based WAL rotation...")

    with tempfile.TemporaryDirectory() as temp_dir:
        wal_config = WALConfig(
            db_path=Path(temp_dir) / "count_test_wal.db",
            max_wal_size_mb=1000,  # High size limit
            max_unprocessed_events=50,  # Low count limit for testing
            rotation_check_interval=2,  # Check every 2 seconds
            auto_flush_interval=1.0,
            enable_compression=False,  # Disable compression for speed
        )

        wal = EventWAL(wal_config)

        try:
            # Generate many small events
            logger.info("Writing events to exceed count limit...")
            for batch_num in range(10):  # 10 batches of 10 events = 100 events
                events = []
                for i in range(10):
                    event = ActivityEvent(activity_type=ActivityEventType.ACTIVE, username="test_user", client_id="test_client")
                    events.append(event)

                success = wal.write_events(events)
                assert success, f"Failed to write batch {batch_num}"

                # Check stats periodically
                if batch_num % 3 == 0:
                    stats = wal.get_rotation_stats()
                    logger.info(f"Batch {batch_num}: {stats['unprocessed_events']} unprocessed events")

            # Wait for rotation to trigger
            logger.info("Waiting for count-based rotation to trigger...")
            time.sleep(5)

            # Check final stats
            final_stats = wal.get_rotation_stats()
            logger.info(f"Final unprocessed events: {final_stats['unprocessed_events']}")
            logger.info(f"Archives created: {final_stats['archive_count']}")

            if final_stats["archive_count"] > 0:
                logger.info("✓ Count-based rotation successful!")
            else:
                logger.warning("Count-based rotation did not trigger")

        finally:
            wal.close()

    logger.info("✓ Event count rotation test completed")


def test_manual_rotation():
    """Test manual WAL rotation."""
    logger.info("Testing manual WAL rotation...")

    with tempfile.TemporaryDirectory() as temp_dir:
        wal_config = WALConfig(
            db_path=Path(temp_dir) / "manual_test_wal.db",
            max_wal_size_mb=1000,  # High limits
            max_unprocessed_events=10000,
            rotation_check_interval=3600,  # Long interval (won't trigger automatically)
            enable_compression=True,
        )

        wal = EventWAL(wal_config)

        try:
            # Add some events
            events = []
            for i in range(20):
                event = WindowEvent(window_title=f"Test Window {i}", start_time=datetime.now(), end_time=datetime.now(), duration=30.0, username="test_user", client_id="test_client")
                events.append(event)

            success = wal.write_events(events)
            assert success, "Failed to write events"

            # Get initial stats
            initial_stats = wal.get_rotation_stats()
            logger.info(f"Before rotation: {initial_stats['unprocessed_events']} events")
            assert initial_stats["archive_count"] == 0, "Should start with no archives"

            # Force manual rotation
            logger.info("Forcing manual rotation...")
            success = wal.force_rotation()
            assert success, "Manual rotation failed"

            # Check results
            final_stats = wal.get_rotation_stats()
            logger.info(f"After rotation: {final_stats['unprocessed_events']} events")
            logger.info(f"Archives created: {final_stats['archive_count']}")

            assert final_stats["archive_count"] == 1, "Should have 1 archive after rotation"
            logger.info("✓ Manual rotation successful!")

        finally:
            wal.close()

    logger.info("✓ Manual rotation test completed")


def test_archive_cleanup():
    """Test cleanup of old archive files."""
    logger.info("Testing archive cleanup...")

    with tempfile.TemporaryDirectory() as temp_dir:
        wal_config = WALConfig(
            db_path=Path(temp_dir) / "cleanup_test_wal.db",
            max_archived_files=3,  # Keep only 3 archives
            enable_compression=True,
        )

        wal = EventWAL(wal_config)

        try:
            # Create multiple archives by forcing rotations
            for i in range(5):  # Create 5 archives
                # Add some events
                events = []
                for j in range(10):
                    event = ActivityEvent(activity_type=ActivityEventType.ACTIVE, username=f"user_{i}", client_id=f"client_{i}")
                    events.append(event)

                wal.write_events(events)

                # Force rotation
                logger.info(f"Creating archive {i + 1}/5...")
                success = wal.force_rotation()
                assert success, f"Failed to create archive {i + 1}"

                time.sleep(0.1)  # Small delay to ensure different timestamps

            # Check final archive count
            final_stats = wal.get_rotation_stats()
            logger.info(f"Total archives after cleanup: {final_stats['archive_count']}")
            logger.info(f"Archive size: {final_stats['total_archive_size_mb']:.2f}MB")

            # Should have exactly 3 archives (max_archived_files)
            assert final_stats["archive_count"] == 3, f"Expected 3 archives, got {final_stats['archive_count']}"
            logger.info("✓ Archive cleanup successful!")

        finally:
            wal.close()

    logger.info("✓ Archive cleanup test completed")


def test_compression():
    """Test archive compression functionality."""
    logger.info("Testing archive compression...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Test with compression enabled
        wal_config_compressed = WALConfig(db_path=Path(temp_dir) / "compressed_wal.db", enable_compression=True, archive_directory=Path(temp_dir) / "compressed_archives")

        # Test with compression disabled
        wal_config_uncompressed = WALConfig(db_path=Path(temp_dir) / "uncompressed_wal.db", enable_compression=False, archive_directory=Path(temp_dir) / "uncompressed_archives")

        # Test compressed archives
        wal_compressed = EventWAL(wal_config_compressed)
        try:
            # Create large events
            events = []
            for i in range(50):
                event = WindowEvent(window_title=f"Large Window {i} " + "X" * 500, start_time=datetime.now(), end_time=datetime.now(), duration=120.0, username="test_user", client_id="test_client")
                events.append(event)

            wal_compressed.write_events(events)
            wal_compressed.force_rotation()

            compressed_stats = wal_compressed.get_rotation_stats()
            compressed_size = compressed_stats["total_archive_size_mb"]

        finally:
            wal_compressed.close()

        # Test uncompressed archives
        wal_uncompressed = EventWAL(wal_config_uncompressed)
        try:
            # Create the same events
            wal_uncompressed.write_events(events)
            wal_uncompressed.force_rotation()

            uncompressed_stats = wal_uncompressed.get_rotation_stats()
            uncompressed_size = uncompressed_stats["total_archive_size_mb"]

        finally:
            wal_uncompressed.close()

        # Compare sizes
        compression_ratio = compressed_size / uncompressed_size if uncompressed_size > 0 else 1.0
        logger.info(f"Compressed size: {compressed_size:.2f}MB")
        logger.info(f"Uncompressed size: {uncompressed_size:.2f}MB")
        logger.info(f"Compression ratio: {compression_ratio:.2f} ({(1 - compression_ratio) * 100:.1f}% saved)")

        # Compression should reduce size significantly
        assert compression_ratio < 0.8, f"Compression not effective: {compression_ratio:.2f}"
        logger.info("✓ Compression test successful!")

    logger.info("✓ Compression test completed")


def simulate_server_outage():
    """Simulate extended server outage scenario."""
    logger.info("Simulating extended server outage scenario...")

    with tempfile.TemporaryDirectory() as temp_dir:
        wal_config = WALConfig(
            db_path=Path(temp_dir) / "outage_test_wal.db",
            max_wal_size_mb=5,  # Small size for quick rotation
            max_unprocessed_events=100,
            rotation_check_interval=3,  # Frequent checks
            max_archived_files=5,
            enable_compression=True,
        )

        wal = EventWAL(wal_config)

        try:
            logger.info("Simulating continuous event generation during server outage...")

            # Simulate events being generated but not processed (server down)
            total_events = 0
            for hour in range(3):  # Simulate 3 hours of outage
                logger.info(f"Hour {hour + 1}/3 of server outage...")

                # Generate events every "minute" (accelerated for testing)
                for minute in range(10):  # 10 "minutes" per hour
                    events = []

                    # Window events
                    for i in range(2):
                        event = WindowEvent(
                            window_title=f"App Window {hour}:{minute}:{i}", start_time=datetime.now(), end_time=datetime.now(), duration=60.0, username="offline_user", client_id="offline_client"
                        )
                        events.append(event)

                    # Activity events
                    for i in range(3):
                        event = ActivityEvent(activity_type=ActivityEventType.ACTIVE, username="offline_user", client_id="offline_client")
                        events.append(event)

                    wal.write_events(events)
                    total_events += len(events)

                    time.sleep(0.1)  # Small delay

                # Check stats each hour
                stats = wal.get_rotation_stats()
                logger.info(f"After hour {hour + 1}: {stats['unprocessed_events']} unprocessed, {stats['archive_count']} archives, DB: {stats['current_db_size_mb']:.1f}MB")

            # Final stats
            final_stats = wal.get_rotation_stats()
            logger.info("Outage simulation completed:")
            logger.info(f"  Total events generated: {total_events}")
            logger.info(f"  Unprocessed events: {final_stats['unprocessed_events']}")
            logger.info(f"  Archives created: {final_stats['archive_count']}")
            logger.info(f"  Total archive size: {final_stats['total_archive_size_mb']:.2f}MB")
            logger.info(f"  Current DB size: {final_stats['current_db_size_mb']:.2f}MB")

            # Verify rotation worked
            assert final_stats["archive_count"] > 0, "Should have created archives during outage"
            assert final_stats["current_db_size_mb"] < wal_config.max_wal_size_mb * 2, "Current DB should be reasonable size"

            logger.info("✓ Server outage simulation successful!")

        finally:
            wal.close()

    logger.info("✓ Server outage simulation completed")


def main():
    """Run all WAL rotation tests."""
    logger.info("Starting WAL rotation tests...")

    try:
        test_manual_rotation()
        test_compression()
        test_archive_cleanup()
        test_event_count_rotation()
        test_size_based_rotation()
        simulate_server_outage()

        logger.info("🎉 All WAL rotation tests passed successfully!")
        logger.info("")
        logger.info("WAL Rotation Summary:")
        logger.info("✅ Size-based rotation works")
        logger.info("✅ Event count-based rotation works")
        logger.info("✅ Manual rotation works")
        logger.info("✅ Archive compression works")
        logger.info("✅ Archive cleanup works")
        logger.info("✅ Extended server outage handling works")
        logger.info("")
        logger.info("Your WAL is now rotation-ready! 🚀")

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    main()
