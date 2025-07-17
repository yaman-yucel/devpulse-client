"""Enrollment client for DevPulse server communication."""

from __future__ import annotations

import json
import platform
import socket
from typing import Any, Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from loguru import logger

from ..collectors import DeviceFingerprintCollector
from ..models import EnrollmentRequest, EnrollmentResponse


class EnrollmentClient:
    """Client for enrolling devices with the DevPulse server."""

    def __init__(
        self,
        server_url: str,
        timeout_seconds: int = 30,
    ):
        self.server_url = server_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.enrollment_endpoint = "/api/v1/enroll"
        self._fingerprint_collector = DeviceFingerprintCollector()

    def enroll_device(
        self,
        username: str,
        user_email: str,
        enrollment_secret: str,
        hostname: Optional[str] = None,
        platform_name: Optional[str] = None,
    ) -> Tuple[bool, str, Optional[EnrollmentResponse]]:
        try:
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
                enrollment_secret=enrollment_secret,
                device_fingerprint=device_fingerprint,  # holds mac, and other optional fields, mac is used for server side verification
            )

            logger.info(f"Enrolling device: {hostname} ({platform_name}) for user: {username}")

            # Send enrollment request
            success, message, response_data = self._send_enrollment_request(request)

            if not success:
                logger.error(f"Enrollment failed: {message}")
                return False, message, None

            # Parse enrollment response
            try:
                enrollment_response = EnrollmentResponse.model_validate(response_data)
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
