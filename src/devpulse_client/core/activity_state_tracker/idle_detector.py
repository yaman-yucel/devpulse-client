from __future__ import annotations

import ctypes
import ctypes.wintypes
import subprocess

from tracker.config.tracker_settings import tracker_settings


class IdleDetector:
    """Implementation relying on platform-specific utilities."""

    @staticmethod
    def _seconds_idle_win32() -> float:
        class LASTINPUTINFO(ctypes.Structure):
            _fields_ = [
                ("cbSize", ctypes.wintypes.UINT),
                ("dwTime", ctypes.wintypes.DWORD),
            ]

        lii = LASTINPUTINFO()
        lii.cbSize = ctypes.sizeof(lii)

        if ctypes.windll.user32.GetLastInputInfo(ctypes.byref(lii)):
            millis = ctypes.windll.kernel32.GetTickCount() - lii.dwTime
            return millis / 1000.0
        return -1

    @staticmethod
    def _seconds_idle_darwin() -> float:
        try:
            res = subprocess.check_output(["ioreg", "-c", "IOHIDSystem"])
            for line in res.decode().splitlines():
                if "HIDIdleTime" in line:
                    nanos = int(line.split()[-1])
                    return nanos / 1e9
        except Exception:
            return -1

    @staticmethod
    def _seconds_idle_linux() -> float:
        for cmd in (["xprintidle"], ["xssstate", "-i"]):
            try:
                return int(subprocess.check_output(cmd)) / 1000.0
            except (FileNotFoundError, subprocess.CalledProcessError):
                continue
        return -1

    @staticmethod
    def seconds_idle() -> float:  # noqa: D401 – imperative form more readable here
        match tracker_settings.system:
            case "win32":
                return IdleDetector._seconds_idle_win32()
            case "darwin":
                return IdleDetector._seconds_idle_darwin()
            case "linux":
                return IdleDetector._seconds_idle_linux()
            case _:
                return -1
