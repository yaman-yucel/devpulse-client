"""Pydantic models for enrollment data structures."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class DeviceFingerprint(BaseModel):
    """Hardware fingerprint information for device identification."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    mac_address: str = Field(..., pattern=r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$", description="MAC address in standard format")
    serial_number: str | None = Field(None, min_length=1, description="Device serial number")
    cpu_info: str | None = Field(None, min_length=1, description="CPU information")
    memory_gb: float | None = Field(None, gt=0, description="Total system memory in GB")
    architecture: str | None = Field(None, min_length=1, description="CPU architecture")
    processor: str | None = Field(None, min_length=1, description="Processor name")


class EnrollmentRequest(BaseModel):
    """Enrollment request data sent to the server."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    username: str = Field(..., min_length=1, description="Username for enrollment")
    user_email: str = Field(..., pattern=r"^[^@]+@[^@]+\.[^@]+$", description="User email address")
    hostname: str | None = Field(None, min_length=1, description="Device hostname")
    platform: str | None = Field(None, min_length=1, description="Platform name")
    device_fingerprint: DeviceFingerprint = Field(..., description="Device hardware fingerprint")


class CredentialValidationRequest(BaseModel):
    """Request model for credential validation."""

    user_email: str = Field(..., pattern=r"^[^@]+@[^@]+\.[^@]+$", description="User email address")
    device_fingerprint: DeviceFingerprint = Field(..., description="Device hardware fingerprint (MAC only)")
