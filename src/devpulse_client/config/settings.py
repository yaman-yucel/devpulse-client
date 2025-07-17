"""Configuration management for DevPulse client.

This module provides configuration management that integrates with
enrollment credentials and allows environment variable overrides.
"""

from __future__ import annotations

import os
import platform
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from loguru import logger


@dataclass
class TrackerConfig:
    """Configuration for event tracking components."""

    # Tracking intervals (seconds)
    heartbeat_interval: int = 5
    window_event_interval: float = 0.0  # 0 = immediate
    screenshot_interval: int = 60
    idle_threshold: int = 300  # 5 minutes

    # System settings
    system_run_delay: float = 0.1  # Main loop delay

    # Screenshot settings
    screenshot_dir: Path = field(default_factory=lambda: Path.cwd() / "screenshots")
    image_format: str = "png"
    image_quality: int = 85  # For JPEG

    def __post_init__(self):
        """Ensure screenshot directory exists."""
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class PipelineConfig:
    """Configuration for the event pipeline."""

    # Queue settings
    queue_max_size: int = 1000
    queue_spill_threshold: float = 0.8
    queue_drain_batch_size: int = 50
    queue_timeout_seconds: float = 0.1

    # WAL settings
    wal_db_path: Path = field(default_factory=lambda: Path.cwd() / "devpulse_wal.db")
    wal_max_batch_size: int = 100
    wal_checkpoint_interval: int = 1000
    wal_vacuum_threshold: int = 10000

    # Batcher settings
    batch_max_size: int = 100
    batch_max_age_seconds: int = 30
    batch_min_size: int = 1
    batch_timeout_check_interval: float = 1.0

    # Sender settings
    sender_timeout_seconds: int = 30
    sender_max_retries: int = 3
    sender_retry_backoff_base: float = 1.0
    sender_retry_backoff_max: float = 60.0
    sender_token_refresh_margin: int = 300

    # Pipeline settings
    stats_report_interval: int = 300  # 5 minutes


@dataclass
class DevPulseConfig:
    """Complete DevPulse client configuration."""

    # Server settings
    server_url: str = "http://localhost:8000"

    # Client identification
    username: str = ""
    client_id: str = ""
    device_id: str = ""
    api_key: str = ""

    # Component configurations
    tracker: TrackerConfig = field(default_factory=TrackerConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)

    # System information
    hostname: str = field(default_factory=lambda: os.uname().nodename if hasattr(os, "uname") else "unknown")
    platform: str = field(default_factory=lambda: platform.system().lower())

    def __post_init__(self):
        """Apply environment variable overrides."""
        self._apply_env_overrides()

    def _apply_env_overrides(self):
        """Apply configuration overrides from environment variables."""
        # Server settings
        if server_url := os.getenv("DEVPULSE_SERVER_URL"):
            self.server_url = server_url

        # Client identification
        if username := os.getenv("DEVPULSE_USERNAME"):
            self.username = username

        if client_id := os.getenv("DEVPULSE_CLIENT_ID"):
            self.client_id = client_id

        if api_key := os.getenv("DEVPULSE_API_KEY"):
            self.api_key = api_key

        # Tracker settings
        if heartbeat_interval := os.getenv("DEVPULSE_HEARTBEAT_INTERVAL"):
            try:
                self.tracker.heartbeat_interval = int(heartbeat_interval)
            except ValueError:
                logger.warning(f"Invalid heartbeat interval: {heartbeat_interval}")

        if screenshot_interval := os.getenv("DEVPULSE_SCREENSHOT_INTERVAL"):
            try:
                self.tracker.screenshot_interval = int(screenshot_interval)
            except ValueError:
                logger.warning(f"Invalid screenshot interval: {screenshot_interval}")

        if idle_threshold := os.getenv("DEVPULSE_IDLE_THRESHOLD"):
            try:
                self.tracker.idle_threshold = int(idle_threshold)
            except ValueError:
                logger.warning(f"Invalid idle threshold: {idle_threshold}")

        # Pipeline settings
        if batch_max_size := os.getenv("DEVPULSE_BATCH_MAX_SIZE"):
            try:
                self.pipeline.batch_max_size = int(batch_max_size)
            except ValueError:
                logger.warning(f"Invalid batch max size: {batch_max_size}")

        if batch_max_age := os.getenv("DEVPULSE_BATCH_MAX_AGE"):
            try:
                self.pipeline.batch_max_age_seconds = int(batch_max_age)
            except ValueError:
                logger.warning(f"Invalid batch max age: {batch_max_age}")

        # Paths
        if wal_db_path := os.getenv("DEVPULSE_WAL_DB_PATH"):
            self.pipeline.wal_db_path = Path(wal_db_path)

        if screenshot_dir := os.getenv("DEVPULSE_SCREENSHOT_DIR"):
            self.tracker.screenshot_dir = Path(screenshot_dir)

    def apply_enrollment_config(self, enrollment_config: dict) -> None:
        """Apply configuration from enrollment response.

        Args:
            enrollment_config: Configuration dict from enrollment
        """
        # Update API key
        if api_key := enrollment_config.get("api_key"):
            self.api_key = api_key

        # Update batch settings
        if batch_max_size := enrollment_config.get("batch_max_size"):
            self.pipeline.batch_max_size = batch_max_size

        if batch_max_age := enrollment_config.get("batch_max_age_seconds"):
            self.pipeline.batch_max_age_seconds = batch_max_age

        # Update heartbeat interval
        if heartbeat_interval := enrollment_config.get("heartbeat_interval"):
            self.tracker.heartbeat_interval = heartbeat_interval

        logger.info("Applied configuration from enrollment")

    def get_queue_config(self) -> dict:
        """Get configuration for the event queue."""
        return {
            "max_size": self.pipeline.queue_max_size,
            "spill_threshold": self.pipeline.queue_spill_threshold,
            "drain_batch_size": self.pipeline.queue_drain_batch_size,
            "timeout_seconds": self.pipeline.queue_timeout_seconds,
        }

    def get_wal_config(self) -> dict:
        """Get configuration for the WAL."""
        return {
            "db_path": self.pipeline.wal_db_path,
            "max_batch_size": self.pipeline.wal_max_batch_size,
            "checkpoint_interval": self.pipeline.wal_checkpoint_interval,
            "vacuum_threshold": self.pipeline.wal_vacuum_threshold,
        }

    def get_batcher_config(self) -> dict:
        """Get configuration for the batcher."""
        return {
            "max_batch_size": self.pipeline.batch_max_size,
            "max_batch_age_seconds": self.pipeline.batch_max_age_seconds,
            "min_batch_size": self.pipeline.batch_min_size,
            "batch_timeout_check_interval": self.pipeline.batch_timeout_check_interval,
        }

    def get_sender_config(self) -> dict:
        """Get configuration for the HTTP sender."""
        return {
            "api_base_url": self.server_url,
            "api_key": self.api_key,
            "client_id": self.client_id,
            "timeout_seconds": self.pipeline.sender_timeout_seconds,
            "max_retries": self.pipeline.sender_max_retries,
            "retry_backoff_base": self.pipeline.sender_retry_backoff_base,
            "retry_backoff_max": self.pipeline.sender_retry_backoff_max,
            "token_refresh_margin": self.pipeline.sender_token_refresh_margin,
        }

    def validate(self) -> tuple[bool, list[str]]:
        """Validate the configuration.

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        # Check required fields
        if not self.server_url:
            errors.append("Server URL is required")

        if not self.username:
            errors.append("Username is required")

        if not self.api_key:
            errors.append("API key is required")

        # Validate intervals
        if self.tracker.heartbeat_interval <= 0:
            errors.append("Heartbeat interval must be positive")

        if self.tracker.idle_threshold <= 0:
            errors.append("Idle threshold must be positive")

        if self.pipeline.batch_max_size <= 0:
            errors.append("Batch max size must be positive")

        if self.pipeline.batch_max_age_seconds <= 0:
            errors.append("Batch max age must be positive")

        return len(errors) == 0, errors


class ConfigManager:
    """Manages DevPulse client configuration."""

    def __init__(self):
        """Initialize configuration manager."""
        self._config: Optional[DevPulseConfig] = None

    def load_config(
        self,
        server_url: Optional[str] = None,
        username: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> DevPulseConfig:
        """Load configuration with optional overrides.

        Args:
            server_url: Server URL override
            username: Username override
            client_id: Client ID override

        Returns:
            Configured DevPulseConfig instance
        """
        config = DevPulseConfig()

        # Apply parameter overrides
        if server_url:
            config.server_url = server_url

        if username:
            config.username = username

        if client_id:
            config.client_id = client_id

        self._config = config
        return config

    def get_config(self) -> Optional[DevPulseConfig]:
        """Get current configuration."""
        return self._config

    def apply_enrollment(self, enrollment_config: dict) -> None:
        """Apply enrollment configuration to current config.

        Args:
            enrollment_config: Configuration from enrollment
        """
        if self._config:
            self._config.apply_enrollment_config(enrollment_config)

    def validate_config(self) -> tuple[bool, list[str]]:
        """Validate current configuration.

        Returns:
            Tuple of (is_valid, error_messages)
        """
        if not self._config:
            return False, ["No configuration loaded"]

        return self._config.validate()


# Global configuration manager instance
_config_manager = ConfigManager()


def get_config_manager() -> ConfigManager:
    """Get the global configuration manager."""
    return _config_manager


def get_current_config() -> Optional[DevPulseConfig]:
    """Get the current configuration."""
    return _config_manager.get_config()
