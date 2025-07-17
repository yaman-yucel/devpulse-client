"""Credential validation enums and constants."""

from __future__ import annotations

from enum import Enum


class ValidationResult(Enum):
    """Enumeration of credential validation outcomes."""

    VALID = "valid"
    INVALID = "invalid"  # Server says credentials are bad (401/403)
    DEVICE_MISMATCH = "device_mismatch"  # MAC address doesn't match enrolled device
    NETWORK_ERROR = "network_error"  # Cannot reach server
    SERVER_ERROR = "server_error"  # Server error (500, etc.)
    UNKNOWN_ERROR = "unknown_error"  # Other unexpected errors
