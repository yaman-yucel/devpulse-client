"""Pydantic models for enrollment data structures."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class DeviceFingerprint(BaseModel):
    """Hardware fingerprint information for device identification."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    mac_address: str = Field(None, pattern=r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$", description="MAC address in standard format")
    serial_number: Optional[str] = Field(None, min_length=1, description="Device serial number")
    cpu_info: Optional[str] = Field(None, min_length=1, description="CPU information")
    memory_gb: Optional[float] = Field(None, gt=0, description="Total system memory in GB")
    architecture: Optional[str] = Field(None, min_length=1, description="CPU architecture")
    processor: Optional[str] = Field(None, min_length=1, description="Processor name")


class EnrollmentRequest(BaseModel):
    """Enrollment request data sent to the server."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    username: str = Field(..., min_length=1, description="Username for enrollment")
    user_email: str = Field(..., pattern=r"^[^@]+@[^@]+\.[^@]+$", description="User email address")
    hostname: str = Field(..., min_length=1, description="Device hostname")
    platform: str = Field(..., min_length=1, description="Platform name")
    enrollment_secret: str = Field(..., min_length=1, description="Enrollment secret token")
    device_fingerprint: Optional[DeviceFingerprint] = Field(None, description="Device hardware fingerprint")


class EnrollmentConfig(BaseModel):
    """Configuration received from enrollment response."""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    batch_max_events: int = Field(default=100, gt=0, description="Maximum events per batch")
    batch_max_interval_ms: int = Field(default=1000, gt=0, description="Maximum batch interval in milliseconds")
    heartbeat_interval_s: int = Field(default=5, gt=0, description="Heartbeat interval in seconds")

    @model_validator(mode="before")
    @classmethod
    def extract_batch_config(cls, data: Any) -> Dict[str, Any]:
        """Extract batch config from nested structure if present."""
        if isinstance(data, dict) and "batch" in data:
            batch_config = data.get("batch", {})
            return {
                "batch_max_events": batch_config.get("max_events", 100),
                "batch_max_interval_ms": batch_config.get("max_interval_ms", 1000),
                "heartbeat_interval_s": data.get("heartbeat_interval_s", 5),
            }
        return data


class EnrollmentResponse(BaseModel):
    """Response received from successful enrollment."""

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    user_id: str = Field(..., min_length=1, description="Unique user identifier")
    device_id: str = Field(..., min_length=1, description="Unique device identifier")
    api_key: str = Field(..., min_length=1, description="API key for authentication")
    config: EnrollmentConfig = Field(..., description="Configuration settings for the device")
    issued_at: datetime = Field(default_factory=datetime.now, description="Timestamp when credentials were issued")
    expires_at: Optional[datetime] = Field(None, description="Timestamp when credentials expire")

    @field_validator("expires_at", mode="before")
    @classmethod
    def parse_expires_at(cls, v: Any) -> Optional[datetime]:
        """Parse expires_at from various formats."""
        if v is None or v == "":
            return None
        if isinstance(v, str):
            # Handle ISO format with Z suffix
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v

    def to_storage_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage with proper datetime serialization."""
        data = self.model_dump()
        # Convert datetime objects to ISO format strings for storage
        data["issued_at"] = self.issued_at.isoformat()
        data["expires_at"] = self.expires_at.isoformat() if self.expires_at else None
        return data
