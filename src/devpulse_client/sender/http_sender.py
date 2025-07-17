"""HTTP sender for transmitting event batches to the ingest API.

This module provides HTTP/HTTPS transport for sending batched events to the
DevPulse ingest API with retry logic, authentication, and error handling.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from loguru import logger

from ..core.events import EventBatch


@dataclass
class SenderConfig:
    """Configuration for the HTTP sender."""

    api_base_url: str = "http://localhost:8000"  # Base URL of ingest API
    batch_endpoint: str = "/api/v1/events/batch"  # Batch ingestion endpoint
    auth_endpoint: str = "/api/v1/auth/token"  # Authentication endpoint

    # Authentication
    api_key: str = ""  # Bootstrap API key
    client_id: str = ""  # Client identifier

    # HTTP settings
    timeout_seconds: int = 30  # Request timeout
    max_retries: int = 3  # Maximum retry attempts
    retry_backoff_base: float = 1.0  # Base backoff delay
    retry_backoff_max: float = 60.0  # Maximum backoff delay

    # Token management
    token_refresh_margin: int = 300  # Refresh token 5 minutes before expiry

    # Connection pooling (if using urllib3 in future)
    max_connections: int = 10


@dataclass
class AuthToken:
    """Authentication token with expiry tracking."""

    token: str
    expires_at: datetime
    token_type: str = "Bearer"

    def is_expired(self, margin_seconds: int = 0) -> bool:
        """Check if token is expired (with optional margin)."""
        return datetime.now() >= (self.expires_at - timedelta(seconds=margin_seconds))

    def to_header(self) -> str:
        """Get authorization header value."""
        return f"{self.token_type} {self.token}"


class HTTPSender:
    """HTTP sender for transmitting event batches."""

    def __init__(self, config: SenderConfig = SenderConfig()):
        """Initialize the HTTP sender.

        Args:
            config: Sender configuration
        """
        self.config = config
        self._auth_token: Optional[AuthToken] = None

        # Statistics
        self._total_batches_sent = 0
        self._total_batches_failed = 0
        self._total_events_sent = 0
        self._total_send_time = 0.0
        self._last_successful_send: Optional[datetime] = None
        self._last_error: Optional[str] = None

    def send_batch(self, batch: EventBatch) -> Tuple[bool, str]:
        """Send a batch of events to the ingest API.

        Args:
            batch: Event batch to send

        Returns:
            Tuple of (success, error_message)
        """
        start_time = time.time()

        try:
            # Ensure we have a valid auth token
            if not self._ensure_auth_token():
                error_msg = "Failed to obtain authentication token"
                self._total_batches_failed += 1
                self._last_error = error_msg
                return False, error_msg

            # Prepare the request
            url = urljoin(self.config.api_base_url, self.config.batch_endpoint)
            payload = batch.to_dict()

            # Send with retries
            success, error_msg = self._send_with_retries(url, payload)

            # Update statistics
            send_time = time.time() - start_time
            self._total_send_time += send_time

            if success:
                self._total_batches_sent += 1
                self._total_events_sent += batch.size()
                self._last_successful_send = datetime.now()
                self._last_error = None

                logger.info(f"Successfully sent batch {batch.batch_id} with {batch.size()} events in {send_time:.2f}s")
            else:
                self._total_batches_failed += 1
                self._last_error = error_msg

                logger.error(f"Failed to send batch {batch.batch_id}: {error_msg}")

            return success, error_msg

        except Exception as e:
            error_msg = f"Unexpected error sending batch: {e}"
            self._total_batches_failed += 1
            self._last_error = error_msg
            logger.error(error_msg)
            return False, error_msg

    def test_connection(self) -> Tuple[bool, str]:
        """Test connection to the ingest API.

        Returns:
            Tuple of (success, message)
        """
        try:
            # Try to authenticate
            success, error_msg = self._authenticate()
            if success:
                return True, "Connection test successful"
            else:
                return False, f"Connection test failed: {error_msg}"

        except Exception as e:
            return False, f"Connection test failed: {e}"

    def get_stats(self) -> Dict[str, Any]:
        """Get sender statistics.

        Returns:
            Dictionary with sender statistics
        """
        avg_send_time = self._total_send_time / max(1, self._total_batches_sent + self._total_batches_failed)

        return {
            "total_batches_sent": self._total_batches_sent,
            "total_batches_failed": self._total_batches_failed,
            "total_events_sent": self._total_events_sent,
            "success_rate": (self._total_batches_sent / max(1, self._total_batches_sent + self._total_batches_failed)),
            "average_send_time_seconds": avg_send_time,
            "last_successful_send": self._last_successful_send.isoformat() if self._last_successful_send else None,
            "last_error": self._last_error,
            "has_auth_token": self._auth_token is not None,
            "auth_token_expired": (self._auth_token.is_expired() if self._auth_token else True),
        }

    def _ensure_auth_token(self) -> bool:
        """Ensure we have a valid authentication token."""
        # Check if we need to get/refresh token
        if self._auth_token is None or self._auth_token.is_expired(self.config.token_refresh_margin):
            success, _ = self._authenticate()
            return success

        return True

    def _authenticate(self) -> Tuple[bool, str]:
        """Authenticate with the API and get an access token.

        Returns:
            Tuple of (success, error_message)
        """
        try:
            url = urljoin(self.config.api_base_url, self.config.auth_endpoint)

            # Prepare authentication payload
            auth_payload = {
                "api_key": self.config.api_key,
                "client_id": self.config.client_id,
            }

            # Make authentication request
            req = Request(
                url,
                data=json.dumps(auth_payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": f"DevPulse-Client/{self.config.client_id}",
                },
            )

            with urlopen(req, timeout=self.config.timeout_seconds) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode("utf-8"))

                    # Parse token response
                    token = data.get("access_token")
                    expires_in = data.get("expires_in", 3600)  # Default 1 hour
                    token_type = data.get("token_type", "Bearer")

                    if not token:
                        return False, "No access token in response"

                    # Create auth token
                    expires_at = datetime.now() + timedelta(seconds=expires_in)
                    self._auth_token = AuthToken(token=token, expires_at=expires_at, token_type=token_type)

                    logger.info(f"Successfully authenticated, token expires at {expires_at}")
                    return True, "Authentication successful"
                else:
                    error_msg = f"Authentication failed with status {response.status}"
                    return False, error_msg

        except HTTPError as e:
            error_msg = f"HTTP error during authentication: {e.code} {e.reason}"
            if e.code == 401:
                error_msg += " (invalid API key)"
            return False, error_msg

        except URLError as e:
            error_msg = f"Network error during authentication: {e.reason}"
            return False, error_msg

        except Exception as e:
            error_msg = f"Unexpected error during authentication: {e}"
            return False, error_msg

    def _send_with_retries(self, url: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """Send payload with retry logic.

        Args:
            url: API endpoint URL
            payload: JSON payload to send

        Returns:
            Tuple of (success, error_message)
        """
        last_error = ""

        for attempt in range(self.config.max_retries + 1):
            try:
                success, error_msg = self._send_request(url, payload)
                if success:
                    return True, ""

                last_error = error_msg

                # Don't retry on client errors (4xx)
                if "4" in error_msg and "status" in error_msg:
                    break

                # Calculate backoff delay
                if attempt < self.config.max_retries:
                    delay = min(self.config.retry_backoff_base * (2**attempt), self.config.retry_backoff_max)
                    logger.warning(f"Send attempt {attempt + 1} failed: {error_msg}. Retrying in {delay:.1f}s...")
                    time.sleep(delay)

            except Exception as e:
                last_error = str(e)
                if attempt < self.config.max_retries:
                    delay = min(self.config.retry_backoff_base * (2**attempt), self.config.retry_backoff_max)
                    logger.warning(f"Send attempt {attempt + 1} failed: {e}. Retrying in {delay:.1f}s...")
                    time.sleep(delay)

        return False, f"Failed after {self.config.max_retries + 1} attempts: {last_error}"

    def _send_request(self, url: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """Send a single HTTP request.

        Args:
            url: API endpoint URL
            payload: JSON payload to send

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Prepare request
            req = Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": self._auth_token.to_header(),
                    "User-Agent": f"DevPulse-Client/{self.config.client_id}",
                },
            )

            # Send request
            with urlopen(req, timeout=self.config.timeout_seconds) as response:
                if 200 <= response.status < 300:
                    # Success
                    response_data = response.read().decode("utf-8")
                    logger.debug(f"Successful response: {response.status}")
                    return True, ""
                else:
                    # HTTP error
                    error_msg = f"HTTP {response.status}: {response.reason}"
                    return False, error_msg

        except HTTPError as e:
            error_msg = f"HTTP error: {e.code} {e.reason}"

            # Handle authentication errors
            if e.code == 401:
                logger.warning("Authentication token expired, clearing token")
                self._auth_token = None
                error_msg += " (token expired)"

            return False, error_msg

        except URLError as e:
            error_msg = f"Network error: {e.reason}"
            return False, error_msg

        except Exception as e:
            error_msg = f"Request error: {e}"
            return False, error_msg


def create_default_sender(
    api_base_url: str,
    api_key: str,
    client_id: str,
) -> HTTPSender:
    """Create an HTTP sender with default configuration.

    Args:
        api_base_url: Base URL of the ingest API
        api_key: Bootstrap API key for authentication
        client_id: Client identifier

    Returns:
        Configured HTTP sender
    """
    config = SenderConfig(
        api_base_url=api_base_url,
        api_key=api_key,
        client_id=client_id,
    )

    return HTTPSender(config)
