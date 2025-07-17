import getpass
import sys
from datetime import datetime
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class TrackerSettings(BaseSettings):
    """Settings controlling the behaviour of the activity tracker client."""

    IDLE_THRESHOLD: int = Field(10, description="seconds before user considered idle")
    HEARTBEAT_EVERY: int = Field(10, description="seconds between heartbeat writes")
    SCREENSHOT_INTERVAL: int = Field(86400, description="seconds between automatic screenshots (0 = disabled)")
    IMAGE_FORMAT: str = Field("png", description="png | jpeg")
    IMAGE_QUALITY: int = Field(85, description="JPEG quality when IMAGE_FORMAT == 'jpeg'")
    WINDOW_EVENT_INTERVAL: int = Field(0, description="seconds a window must remain active before it is logged")
    SYSTEM_RUN_DELAY: float = Field(0.1, description="seconds to wait after running tasks again")

    # File logging settings

    LOG_TO_CONSOLE: bool = Field(True, description="Enable console logging")
    LOG_TO_FILE: bool = Field(True, description="Enable file logging")
    LOG_LEVEL: str = Field("INFO", description="Minimum log level (DEBUG, INFO, WARNING, ERROR)")
    LOG_RETENTION: str = Field("7 days", description="How long to keep log files")
    LOG_ROTATION: str = Field("10 MB", description="Log file rotation size")

    BASE_DIR: Path = Path.cwd() / "tracker"

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

    @property
    def screenshot_dir(self) -> Path:
        path = self.BASE_DIR / "screenshots"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def system(self) -> str:
        return sys.platform

    @property
    def user(self) -> str:
        return getpass.getuser()

    @property
    def log_dir(self) -> Path:
        """Directory for log files."""
        path = self.BASE_DIR / "logs"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def log_file_path(self) -> Path:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return self.log_dir / f"devpulse_{timestamp}.log"


tracker_settings = TrackerSettings()
