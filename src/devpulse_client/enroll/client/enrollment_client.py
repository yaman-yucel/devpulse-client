"""Enrollment client for DevPulse server communication."""

from __future__ import annotations

import json
import os
import platform
import socket
import stat
from enum import Enum
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from loguru import logger
from pydantic import BaseModel

from ..collectors import DeviceFingerprintCollector
from ..models import CredentialValidationRequest, DeviceFingerprint, EnrollmentRequest


class EnrollStatus(Enum):
    """Enum representing credential operation outcomes."""

    SUCCESS = "success"
    FAILURE = "failure"
    ALREADY_EXISTS = "already_exists"
    INVALID_REQUEST = "invalid_request"


class EnrollResponse(BaseModel):
    """Response model for enrollment."""

    status: EnrollStatus
    message: str


class ValidateStatus(Enum):
    """Enum representing credential operation outcomes."""

    SUCCESS = "success"
    FAILURE = "failure"
    INVALID_REQUEST = "invalid_request"


class ValidateResponse(BaseModel):
    """Response model for validation."""

    status: ValidateStatus
    message: str


class CredentialClient:
    """Client for enrolling and authenticating devices with the DevPulse server."""

    def __init__(
        self,
        server_url: str,
        timeout_seconds: int = 30,
    ):
        self.server_url = server_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.enrollment_endpoint = "/api/credentials/enroll"
        self.authentication_endpoint = "/api/credentials/validate"
        self._fingerprint_collector = DeviceFingerprintCollector()
        self.config_dir = Path.home() / ".devpulse"
        self.credentials_file = self.config_dir / "credentials.json"
        self._ensure_config_dir()

    def enroll_device(self, username: str, user_email: str, hostname: str | None = None, platform_name: str | None = None) -> EnrollResponse:
        # Auto-detect system information if not provided
        if hostname is None:
            hostname = socket.gethostname()

        if platform_name is None:
            platform_name = self._detect_platform()

        device_fingerprint = self._fingerprint_collector.collect_fingerprint()

        # Create enrollment request
        request = EnrollmentRequest(
            username=username,
            user_email=user_email,
            hostname=hostname,
            platform=platform_name,
            device_fingerprint=device_fingerprint,  # holds mac, and other optional fields, mac is used for server side verification
        )

        logger.info(f"Enrolling device: {hostname} ({platform_name}) for user: {username}")

        # Send enrollment request
        response = self._send_enrollment_request(request)

        return response

    def _send_enrollment_request(self, request: EnrollmentRequest) -> EnrollResponse:
        try:
            url = urljoin(self.server_url, self.enrollment_endpoint)
            payload = json.dumps(request.model_dump())

            # Create HTTP request
            req = Request(
                url,
                data=payload.encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "DevPulse-Client/2.0.0",
                },
            )

            with urlopen(req, timeout=self.timeout_seconds) as response:
                if response.status == 200:
                    response_data = json.loads(response.read().decode("utf-8"))
                    return EnrollResponse.model_validate(response_data)
                else:
                    return EnrollResponse(status=EnrollStatus.FAILURE, message=f"Enrollment failed with HTTP {response.status}")

        except HTTPError as e:
            if e.code == 400:
                return EnrollResponse(status=EnrollStatus.FAILURE, message="Invalid enrollment request (check request parameters)")
            else:
                return EnrollResponse(status=EnrollStatus.FAILURE, message=f"HTTP error: {e.code} {e.reason}")

        except URLError as e:
            return EnrollResponse(status=EnrollStatus.FAILURE, message=f"Network error: {e.reason}")

        except Exception as e:
            return EnrollResponse(status=EnrollStatus.FAILURE, message=f"Unknown request error: {e}")

    def _ensure_config_dir(self) -> None:
        """Ensure the config directory exists."""
        try:
            self.config_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

            os.chmod(self.config_dir, stat.S_IRWXU)  # might need to check Windows, MACOs

        except Exception as e:
            logger.error(f"Failed to create config directory: {e}")
            raise

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

    def test_connectivity(self) -> tuple[bool, str]:
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

    def validate_credentials(self, user_email: str) -> ValidateResponse:
        """Validate credentials for a device."""
        try:
            device_fingerprint = self._fingerprint_collector.collect_fingerprint(mac_only=True)
            validation_request = CredentialValidationRequest(user_email=user_email, device_fingerprint=device_fingerprint)
            response = self._send_validation_request(validation_request)
            return response
        except Exception as e:
            return ValidateResponse(status=ValidateStatus.FAILURE, message=f"Credential validation error: {e}")

    def _send_validation_request(self, validation_request: CredentialValidationRequest) -> ValidateResponse:
        """Send credentials to the server for validation and return (is_valid, message)."""
        try:
            url = urljoin(self.server_url, self.authentication_endpoint)
            payload = json.dumps(validation_request.model_dump())
            req = Request(
                url,
                data=payload.encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "DevPulse-Client/2.0.0",
                },
            )
            with urlopen(req, timeout=self.timeout_seconds) as response:
                if response.status == 200:
                    json_response = json.loads(response.read().decode("utf-8"))
                    validate_response = ValidateResponse.model_validate(json_response)
                    return validate_response
                else:
                    return ValidateResponse(status=ValidateStatus.FAILURE, message=f"Credential validation failed with HTTP {response.status}")
        except HTTPError as e:
            return ValidateResponse(status=ValidateStatus.FAILURE, message=f"HTTP error: {e.code} {e.reason}")
        except URLError as e:
            return ValidateResponse(status=ValidateStatus.FAILURE, message=f"Network error: {e.reason}")
        except Exception as e:
            return ValidateResponse(status=ValidateStatus.FAILURE, message=f"Credential validation error: {e}")
