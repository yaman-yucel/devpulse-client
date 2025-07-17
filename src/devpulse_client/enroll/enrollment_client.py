"""Client-side enrollment functionality for DevPulse.

This module handles device enrollment with the DevPulse server, including:
- Sending enrollment requests with device information
- Receiving and storing credentials and configuration
- Managing enrollment state and validation
"""

from __future__ import annotations

import json
import platform
import socket
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from loguru import logger


@dataclass
class EnrollmentRequest:
    """Enrollment request data sent to the server."""

    username: str
    hostname: str
    platform: str
    enrollment_secret: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "username": self.username,
            "hostname": self.hostname,
            "platform": self.platform,
            "enrollment_secret": self.enrollment_secret,
        }


@dataclass
class EnrollmentConfig:
    """Configuration received from enrollment response."""

    batch_max_events: int = 100
    batch_max_interval_ms: int = 1000
    heartbeat_interval_s: int = 5

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EnrollmentConfig:
        """Create from enrollment response config dict."""
        batch_config = data.get("batch", {})
        return cls(
            batch_max_events=batch_config.get("max_events", 100),
            batch_max_interval_ms=batch_config.get("max_interval_ms", 1000),
            heartbeat_interval_s=data.get("heartbeat_interval_s", 5),
        )


@dataclass
class EnrollmentResponse:
    """Response received from successful enrollment."""

    user_id: str
    device_id: str
    api_key: str
    config: EnrollmentConfig
    issued_at: datetime
    expires_at: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EnrollmentResponse:
        """Create from enrollment response dict."""
        config_data = data.get("config", {})
        config = EnrollmentConfig.from_dict(config_data)

        issued_at = datetime.now()  # Use current time as issued_at
        expires_at = None
        if "expires_at" in data and data["expires_at"]:
            expires_at = datetime.fromisoformat(data["expires_at"].replace("Z", "+00:00"))

        return cls(
            user_id=data["user_id"],
            device_id=data["device_id"],
            api_key=data["api_key"],
            config=config,
            issued_at=issued_at,
            expires_at=expires_at,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "user_id": self.user_id,
            "device_id": self.device_id,
            "api_key": self.api_key,
            "issued_at": self.issued_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "config": {
                "batch_max_events": self.config.batch_max_events,
                "batch_max_interval_ms": self.config.batch_max_interval_ms,
                "heartbeat_interval_s": self.config.heartbeat_interval_s,
            },
        }


class EnrollmentClient:
    """Client for enrolling devices with the DevPulse server."""

    def __init__(
        self,
        server_url: str,
        timeout_seconds: int = 30,
    ):
        """Initialize enrollment client.

        Args:
            server_url: Base URL of the DevPulse server
            timeout_seconds: Request timeout
        """
        self.server_url = server_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.enrollment_endpoint = "/api/v1/enroll"

    def enroll_device(
        self,
        username: str,
        enrollment_secret: str,
        hostname: Optional[str] = None,
        platform_name: Optional[str] = None,
    ) -> Tuple[bool, str, Optional[EnrollmentResponse]]:
        """Enroll this device with the DevPulse server.

        Args:
            username: Username for enrollment
            enrollment_secret: Secret token provided by admin
            hostname: Device hostname (auto-detected if not provided)
            platform_name: Platform name (auto-detected if not provided)

        Returns:
            Tuple of (success, message, enrollment_response)
        """
        try:
            # Auto-detect system information if not provided
            if hostname is None:
                hostname = socket.gethostname()

            if platform_name is None:
                platform_name = self._detect_platform()

            # Create enrollment request
            request = EnrollmentRequest(
                username=username,
                hostname=hostname,
                platform=platform_name,
                enrollment_secret=enrollment_secret,
            )

            logger.info(f"Enrolling device: {hostname} ({platform_name}) for user: {username}")

            # Send enrollment request
            success, message, response_data = self._send_enrollment_request(request)

            if not success:
                return False, message, None

            # Parse enrollment response
            try:
                enrollment_response = EnrollmentResponse.from_dict(response_data)
                logger.info(f"Enrollment successful - Device ID: {enrollment_response.device_id}, User ID: {enrollment_response.user_id}")
                return True, "Enrollment successful", enrollment_response

            except Exception as e:
                error_msg = f"Failed to parse enrollment response: {e}"
                logger.error(error_msg)
                return False, error_msg, None

        except Exception as e:
            error_msg = f"Enrollment failed: {e}"
            logger.error(error_msg)
            return False, error_msg, None

    def _send_enrollment_request(
        self,
        request: EnrollmentRequest,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Send enrollment request to server.

        Args:
            request: Enrollment request data

        Returns:
            Tuple of (success, message, response_data)
        """
        try:
            url = urljoin(self.server_url, self.enrollment_endpoint)
            payload = json.dumps(request.to_dict())

            # Create HTTP request
            req = Request(
                url,
                data=payload.encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "DevPulse-Client/2.0.0",
                },
            )

            # Send request
            with urlopen(req, timeout=self.timeout_seconds) as response:
                if response.status == 200:
                    response_data = json.loads(response.read().decode("utf-8"))
                    return True, "Success", response_data
                else:
                    error_msg = f"Enrollment failed with HTTP {response.status}"
                    return False, error_msg, None

        except HTTPError as e:
            if e.code == 400:
                error_msg = "Invalid enrollment request (check username and secret)"
            elif e.code == 401:
                error_msg = "Invalid enrollment secret"
            elif e.code == 409:
                error_msg = "Device already enrolled or user exists"
            else:
                error_msg = f"HTTP error: {e.code} {e.reason}"

            logger.error(error_msg)
            return False, error_msg, None

        except URLError as e:
            error_msg = f"Network error: {e.reason}"
            logger.error(error_msg)
            return False, error_msg, None

        except Exception as e:
            error_msg = f"Request error: {e}"
            logger.error(error_msg)
            return False, error_msg, None

    def _detect_platform(self) -> str:
        """Detect the current platform."""
        system = platform.system().lower()

        # Map Python platform names to DevPulse platform names
        platform_mapping = {
            "linux": "linux",
            "darwin": "macos",
            "windows": "windows",
        }

        return platform_mapping.get(system, system)

    def test_connectivity(self) -> Tuple[bool, str]:
        """Test connectivity to the enrollment server.

        Returns:
            Tuple of (success, message)
        """
        try:
            # Try to connect to the base URL
            url = f"{self.server_url}/health"  # Assume there's a health endpoint
            req = Request(url, headers={"User-Agent": "DevPulse-Client/2.0.0"})

            with urlopen(req, timeout=self.timeout_seconds) as response:
                if response.status == 200:
                    return True, "Server connectivity OK"
                else:
                    return False, f"Server returned HTTP {response.status}"

        except HTTPError as e:
            if e.code == 404:
                # Health endpoint might not exist, but server is reachable
                return True, "Server reachable (health endpoint not found)"
            else:
                return False, f"HTTP error: {e.code} {e.reason}"

        except URLError as e:
            return False, f"Network error: {e.reason}"

        except Exception as e:
            return False, f"Connectivity test failed: {e}"


def create_enrollment_client(server_url: str) -> EnrollmentClient:
    """Create an enrollment client with default configuration.

    Args:
        server_url: Base URL of the DevPulse server

    Returns:
        Configured enrollment client
    """
    return EnrollmentClient(server_url=server_url)
