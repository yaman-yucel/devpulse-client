"""Secure credential storage for DevPulse client.

This module handles:
- Storing and retrieving enrollment credentials in a secure local file
- Managing credential lifecycle (creation, validation, removal)
- Ensuring proper file permissions for security
"""

from __future__ import annotations

import json
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger

from ..enroll.models import EnrollmentConfig, EnrollmentResponse


class CredentialStore:
    """Secure storage for DevPulse credentials."""

    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize credential store.

        Args:
            config_dir: Directory for config files (defaults to ~/.devpulse)
        """
        if config_dir is None:
            self.config_dir = Path.home() / ".devpulse"
        else:
            self.config_dir = Path(config_dir)

        self.credentials_file = self.config_dir / "credentials.json"

        # Ensure config directory exists with proper permissions
        self._ensure_config_dir()

    def store_credentials(self, enrollment_response: EnrollmentResponse) -> bool:
        """Store enrollment credentials securely.

        Args:
            enrollment_response: Response from successful enrollment

        Returns:
            True if stored successfully, False otherwise
        """
        try:
            # Use the model's built-in serialization method
            credentials = enrollment_response.to_storage_dict()

            # Write credentials to temporary file first
            temp_file = self.credentials_file.with_suffix(".tmp")

            with open(temp_file, "w") as f:
                json.dump(credentials, f, indent=2)

            # Set secure permissions (owner read/write only)
            os.chmod(temp_file, stat.S_IRUSR | stat.S_IWUSR)

            # Atomically replace the credentials file
            temp_file.replace(self.credentials_file)

            logger.info(f"Stored credentials for device {enrollment_response.device_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to store credentials: {e}")
            return False

    def load_credentials(self) -> tuple[bool, Optional[EnrollmentResponse]]:
        """Load stored credentials.

        Returns:
            Tuple of (success, enrollment_response)
        """
        try:
            if not self.credentials_file.exists():
                logger.debug("No credentials file found")
                return False, None

            # Check file permissions
            if not self._check_file_permissions():
                logger.warning("Credentials file has insecure permissions")
                return False, None

            # Load and parse credentials
            with open(self.credentials_file, "r") as f:
                data = json.load(f)

            # Convert back to EnrollmentResponse
            enrollment_response = self._credentials_to_enrollment_response(data)

            if enrollment_response is None:
                logger.error("Failed to parse stored credentials")
                return False, None

            # Check if credentials are expired
            if self._is_expired(enrollment_response):
                logger.warning("Stored credentials have expired")
                return False, None

            logger.debug(f"Loaded credentials for device {enrollment_response.device_id}")
            return True, enrollment_response

        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            return False, None

    def remove_credentials(self) -> bool:
        """Remove stored credentials.

        Returns:
            True if removed successfully, False otherwise
        """
        try:
            if self.credentials_file.exists():
                self.credentials_file.unlink()
                logger.info("Removed stored credentials")
            return True

        except Exception as e:
            logger.error(f"Failed to remove credentials: {e}")
            return False

    def has_valid_credentials(self) -> bool:
        """Check if valid credentials are stored.

        Returns:
            True if valid credentials exist, False otherwise
        """
        success, enrollment_response = self.load_credentials()
        return success and enrollment_response is not None

    def get_device_info(self) -> tuple[Optional[str], Optional[str]]:
        """Get device and user IDs from stored credentials.

        Returns:
            Tuple of (device_id, user_id) or (None, None) if not available
        """
        success, enrollment_response = self.load_credentials()
        if success and enrollment_response:
            return enrollment_response.device_id, enrollment_response.user_id
        return None, None

    def get_api_key(self) -> Optional[str]:
        """Get API key from stored credentials.

        Returns:
            API key if available, None otherwise
        """
        success, enrollment_response = self.load_credentials()
        if success and enrollment_response:
            return enrollment_response.api_key
        return None

    def get_config(self) -> Optional[EnrollmentConfig]:
        """Get configuration from stored credentials.

        Returns:
            Configuration if available, None otherwise
        """
        success, enrollment_response = self.load_credentials()
        if success and enrollment_response:
            return enrollment_response.config
        return None

    def _ensure_config_dir(self) -> None:
        """Ensure config directory exists with proper permissions."""
        try:
            self.config_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

            # Ensure directory has correct permissions
            os.chmod(self.config_dir, stat.S_IRWXU)  # Owner read/write/execute only

        except Exception as e:
            logger.error(f"Failed to create config directory: {e}")
            raise

    def _check_file_permissions(self) -> bool:
        """Check if credentials file has secure permissions."""
        try:
            file_stat = self.credentials_file.stat()
            file_mode = stat.filemode(file_stat.st_mode)

            # Check if file is readable/writable only by owner
            if file_stat.st_mode & (stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH):
                logger.warning(f"Credentials file has insecure permissions: {file_mode}")
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to check file permissions: {e}")
            return False

    def _credentials_to_enrollment_response(self, data: dict) -> Optional[EnrollmentResponse]:
        """Convert stored credentials data back to EnrollmentResponse."""
        try:
            # Use Pydantic's model validation for proper parsing
            return EnrollmentResponse.model_validate(data)

        except Exception as e:
            logger.error(f"Failed to parse credentials data: {e}")
            return None

    def _is_expired(self, enrollment_response: EnrollmentResponse) -> bool:
        """Check if credentials are expired."""
        if enrollment_response.expires_at is None:
            return False  # No expiration

        # Handle timezone-aware comparison
        now = datetime.now()
        expires_at = enrollment_response.expires_at

        # If expires_at is timezone-aware but now is naive, make now UTC
        if expires_at.tzinfo is not None and now.tzinfo is None:
            from datetime import timezone

            now = now.replace(tzinfo=timezone.utc)
        # If expires_at is naive but now is timezone-aware, strip timezone from now
        elif expires_at.tzinfo is None and now.tzinfo is not None:
            now = now.replace(tzinfo=None)

        return now >= expires_at
