"""DevPulse Client - Clean activity tracking with enrollment and pipeline architecture."""

from .config import get_config_manager
from .core import DevPulseClient

__version__ = "2.0.0"

__all__ = ["DevPulseClient", "get_config_manager"]
