"""DevPulse Client - Clean activity tracking with enrollment and pipeline architecture."""

from .config import get_config_manager
from .core import DevPulseClient, create_devpulse_client
from .enroll import get_default_credential_manager

__version__ = "2.0.0"

__all__ = ["DevPulseClient", "create_devpulse_client", "get_default_credential_manager", "get_config_manager"]
