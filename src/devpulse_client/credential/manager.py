"""High-level credential management interface for DevPulse client.

This module provides the main credential management functionality including
credential validation with the server, enrollment status checking, and
pipeline configuration.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from loguru import logger

from ..enroll.collectors import DeviceFingerprintCollector
from ..enroll.models import EnrollmentResponse
from .store import CredentialStore
from .validation import ValidationResult


class CredentialManager:
    """High-level credential management interface."""

    def __init__(
        self,
        config_dir: Optional[Path] = None,
        server_url: Optional[str] = None,
        timeout_seconds: int = 30,
    ):
        """Initialize credential manager.

        Args:
            config_dir: Directory for config files (defaults to ~/.devpulse)
            server_url: Base URL of the DevPulse server for validation
            timeout_seconds: Request timeout for validation
        """
        self.store = CredentialStore(config_dir)
        self.server_url = server_url.rstrip("/") if server_url else None
        self.timeout_seconds = timeout_seconds
        self.validation_endpoint = "/api/v1/auth/validate"
        self._fingerprint_collector = DeviceFingerprintCollector()

    def is_enrolled(self) -> bool:
        """Check if device is enrolled with valid credentials."""
        return self.store.has_valid_credentials()

    def get_credentials(self) -> Optional[EnrollmentResponse]:
        """Get valid credentials if available."""
        success, credentials = self.store.load_credentials()
        return credentials if success else None

    def save_enrollment(self, enrollment_response: EnrollmentResponse) -> bool:
        """Save enrollment credentials."""
        return self.store.store_credentials(enrollment_response)

    def clear_enrollment(self) -> bool:
        """Clear stored enrollment."""
        return self.store.remove_credentials()

    def get_device_identity(self) -> tuple[Optional[str], Optional[str]]:
        """Get device and user identity."""
        return self.store.get_device_info()

    def get_current_mac_address(self) -> Optional[str]:
        """Get the current device MAC address for validation."""
        try:
            fingerprint = self._fingerprint_collector.collect_fingerprint(mac_only=True)
            return fingerprint.mac_address
        except Exception as e:
            logger.warning(f"Failed to collect current MAC address: {e}")
            return None

    def get_pipeline_config(self) -> Optional[dict]:
        """Get configuration for the event pipeline."""
        config = self.store.get_config()
        if config is None:
            return None

        return {
            "api_key": self.store.get_api_key(),
            "batch_max_size": config.batch_max_events,
            "batch_max_age_seconds": config.batch_max_interval_ms // 1000,
            "heartbeat_interval": config.heartbeat_interval_s,
        }

    def validate_credentials(self, remove_invalid: bool = False) -> tuple[bool, str]:
        """Validate stored credentials with the server.

        Args:
            remove_invalid: Whether to remove credentials if server confirms they are invalid

        Returns:
            Tuple of (is_valid, message)
        """
        if self.server_url is None:
            return False, "No server URL configured for validation"

        # Check if we have credentials to validate
        success, credentials = self.store.load_credentials()
        if not success or credentials is None:
            return False, "No credentials found to validate"

        try:
            # Send validation request to server
            result, message = self._send_validation_request(credentials.api_key)

            # Only remove credentials if server explicitly says they are invalid or device mismatch
            if (result == ValidationResult.INVALID or result == ValidationResult.DEVICE_MISMATCH) and remove_invalid:
                logger.warning(f"Server confirmed credentials are invalid ({result.value}) - removing from storage")
                self.store.remove_credentials()
                return False, message
            elif result == ValidationResult.VALID:
                return True, message
            else:
                # Network errors, server errors, etc. - preserve credentials
                logger.debug(f"Validation failed ({result.value}) but preserving credentials: {message}")
                return False, message

        except Exception as e:
            error_msg = f"Validation request failed: {e}"
            logger.error(error_msg)
            return False, error_msg

    def validate_and_refresh(self) -> tuple[bool, str]:
        """Validate credentials and remove them ONLY if server confirms they are invalid.

        Network errors, server downtime, etc. will NOT result in credential removal.

        Returns:
            Tuple of (is_valid, message)
        """
        return self.validate_credentials(remove_invalid=True)

    def check_credentials_for_startup(self) -> tuple[bool, str]:
        """Check credentials for startup - never removes credentials.

        This method is designed for startup checks where you want to know if
        credentials are available and potentially valid, but you don't want to
        remove them even if the server is temporarily unreachable.

        Returns:
            Tuple of (can_proceed, message)
        """
        # First check if we have local credentials
        if not self.is_enrolled():
            return False, "No credentials found - device needs enrollment"

        # If no server URL configured, trust local credentials
        if self.server_url is None:
            logger.debug("No server URL configured - using local credentials")
            return True, "Using local credentials (no server validation configured)"

        # Try to validate with server, but don't remove on any failure
        success, credentials = self.store.load_credentials()
        if not success or credentials is None:
            return False, "Failed to load local credentials"

        try:
            result, message = self._send_validation_request(credentials.api_key)

            if result == ValidationResult.VALID:
                return True, "Credentials are valid"
            elif result == ValidationResult.INVALID:
                return False, f"Credentials are invalid: {message}"
            elif result == ValidationResult.DEVICE_MISMATCH:
                return False, f"Device verification failed: {message}"
            else:
                # Network/server errors - assume credentials are OK for now
                logger.info(f"Could not validate credentials with server ({result.value}), but will proceed with local credentials")
                return True, f"Using local credentials (server validation failed: {message})"

        except Exception as e:
            logger.info(f"Could not validate credentials due to error: {e}, but will proceed with local credentials")
            return True, f"Using local credentials (validation error: {e})"

    def is_enrolled_and_valid(self) -> bool:
        """Check if device is enrolled with valid credentials (includes server validation).

        This method does NOT remove credentials on validation failure - it's designed
        for status checking, not credential management.

        Returns:
            True if enrolled and credentials are valid on server, False otherwise
        """
        if not self.is_enrolled():
            return False

        if self.server_url is None:
            # If no server URL, fall back to local validation only
            logger.debug("No server URL configured, using local validation only")
            return True

        # Check validity but don't remove credentials on failure
        is_valid, _ = self.validate_credentials(remove_invalid=False)
        return is_valid

    def should_reenroll(self) -> tuple[bool, str]:
        """Determine if device should re-enroll based on credential status.

        This method is more conservative and only suggests re-enrollment when
        the server explicitly rejects credentials, not on network errors.

        Returns:
            Tuple of (should_reenroll, reason)
        """
        if not self.is_enrolled():
            return True, "No credentials found"

        if self.server_url is None:
            # No server validation possible - assume credentials are OK
            return False, "Local credentials available (no server validation configured)"

        # Try validation but don't remove credentials
        success, credentials = self.store.load_credentials()
        if not success or credentials is None:
            return True, "Failed to load credentials"

        try:
            result, message = self._send_validation_request(credentials.api_key)

            if result == ValidationResult.VALID:
                return False, "Credentials are valid"
            elif result == ValidationResult.INVALID:
                return True, f"Server rejected credentials: {message}"
            elif result == ValidationResult.DEVICE_MISMATCH:
                return True, f"Device verification failed: {message}"
            else:
                # Network/server errors - don't force re-enrollment
                logger.debug(f"Could not validate credentials ({result.value}), but not forcing re-enrollment")
                return False, f"Credentials available but server validation failed: {message}"

        except Exception as e:
            logger.debug(f"Validation error: {e}, but not forcing re-enrollment")
            return False, f"Credentials available but validation failed: {e}"

    def _send_validation_request(self, api_key: str) -> tuple[ValidationResult, str]:
        """Send validation request to server.

        Args:
            api_key: API key to validate

        Returns:
            Tuple of (validation_result, message)
        """
        try:
            url = urljoin(self.server_url, self.validation_endpoint)

            # Get current MAC address for device verification
            current_mac = self.get_current_mac_address()

            # Create validation request payload
            validation_data = {"device_verification": {"mac_address": current_mac}}

            request_body = json.dumps(validation_data).encode("utf-8")

            # Create validation request
            req = Request(
                url,
                data=request_body,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "User-Agent": "DevPulse-Client/2.0.0",
                    "Content-Type": "application/json",
                },
            )

            # Send request
            with urlopen(req, timeout=self.timeout_seconds) as response:
                if response.status == 200:
                    logger.debug("Credentials validated successfully with server")
                    return ValidationResult.VALID, "Credentials are valid"
                elif response.status == 401:
                    logger.warning("Server rejected credentials as invalid")
                    return ValidationResult.INVALID, "Credentials are invalid or expired"
                elif response.status == 409:
                    logger.warning("Server detected device mismatch")
                    return ValidationResult.DEVICE_MISMATCH, "Device verification failed on server (MAC address mismatch)"
                elif response.status >= 500:
                    error_msg = f"Server error: HTTP {response.status}"
                    logger.warning(error_msg)
                    return ValidationResult.SERVER_ERROR, error_msg
                else:
                    error_msg = f"Unexpected server response: HTTP {response.status}"
                    logger.warning(error_msg)
                    return ValidationResult.SERVER_ERROR, error_msg

        except HTTPError as e:
            if e.code == 401:
                logger.warning("Server rejected credentials as unauthorized")
                return ValidationResult.INVALID, "Credentials are invalid or expired"
            elif e.code == 403:
                logger.warning("Server rejected credentials as forbidden")
                return ValidationResult.INVALID, "Credentials are valid but access is forbidden"
            elif e.code == 409:
                logger.warning("Server detected device mismatch")
                return ValidationResult.DEVICE_MISMATCH, "Device verification failed on server (MAC address mismatch)"
            elif e.code >= 500:
                error_msg = f"Server error: {e.code} {e.reason}"
                logger.warning(error_msg)
                return ValidationResult.SERVER_ERROR, error_msg
            else:
                error_msg = f"HTTP error during validation: {e.code} {e.reason}"
                logger.warning(error_msg)
                return ValidationResult.SERVER_ERROR, error_msg

        except URLError as e:
            error_msg = f"Network error during validation: {e.reason}"
            logger.warning(error_msg)  # Changed from error to warning since this is expected
            return ValidationResult.NETWORK_ERROR, error_msg

        except Exception as e:
            error_msg = f"Validation request error: {e}"
            logger.error(error_msg)
            return ValidationResult.UNKNOWN_ERROR, error_msg


def get_default_credential_manager(server_url: Optional[str] = None) -> CredentialManager:
    """Get the default credential manager instance.

    Args:
        server_url: Optional server URL for credential validation

    Returns:
        Configured credential manager instance
    """
    return CredentialManager(server_url=server_url)


def create_credential_manager(
    config_dir: Optional[Path] = None,
    server_url: Optional[str] = None,
    timeout_seconds: int = 30,
) -> CredentialManager:
    """Create a credential manager with custom configuration.

    Args:
        config_dir: Directory for config files (defaults to ~/.devpulse)
        server_url: Base URL of the DevPulse server for validation
        timeout_seconds: Request timeout for validation

    Returns:
        Configured credential manager instance
    """
    return CredentialManager(
        config_dir=config_dir,
        server_url=server_url,
        timeout_seconds=timeout_seconds,
    )
