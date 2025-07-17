from __future__ import annotations

import ctypes
import ctypes.wintypes
import os
import subprocess

from tracker.config.tracker_settings import tracker_settings


class ScreenLockDetector:
    """Naïve cross-platform screen-lock detection."""

    @staticmethod
    def _get_current_session_id() -> str | None:
        """Helper for Linux: get the current session id for loginctl."""
        try:
            uid = os.getuid()
            res = subprocess.run(["loginctl", "list-sessions", "--no-legend"], capture_output=True, check=False)
            for line in res.stdout.decode().splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[2] == str(uid):
                    return parts[0]
        except Exception:
            return None

    @staticmethod
    def _is_locked_win32() -> bool:
        user32 = ctypes.windll.User32
        hDesktop = user32.OpenDesktopW("Default", 0, False, 0x100)
        if hDesktop:
            locked = user32.SwitchDesktop(hDesktop) == 0
            user32.CloseDesktop(hDesktop)
            return locked
        return False

    @staticmethod
    def _is_locked_darwin() -> bool:
        try:
            cg_path = "/System/Library/CoreServices/Menu Extras/User.menu/Contents/Resources/CGSession"
            res = subprocess.run([cg_path, "-s"], capture_output=True, check=False)
            out = res.stdout.decode()
            return "kCGSSessionScreenIsLocked = 1" in out or "ScreenIsLocked=1" in out
        except Exception:
            return False

    @staticmethod
    def _is_locked_linux() -> bool:
        # Try gnome-screensaver-command first
        try:
            res = subprocess.run(["gnome-screensaver-command", "-q"], capture_output=True, check=False)
            if b"is active" in res.stdout:
                return True
        except Exception:
            pass

        # Try loginctl as fallback
        try:
            session_id = ScreenLockDetector._get_current_session_id()
            if session_id is not None:
                res = subprocess.run(
                    [
                        "loginctl",
                        "show-session",
                        str(session_id),
                        "-p",
                        "LockedHint",
                    ],
                    capture_output=True,
                    check=False,
                )
                return b"LockedHint=yes" in res.stdout
        except Exception:
            pass

        return False

    @staticmethod
    def is_locked() -> bool:  # noqa: D401
        sys_platform = tracker_settings.system
        if sys_platform == "win32":
            return ScreenLockDetector._is_locked_win32()
        if sys_platform == "darwin":
            return ScreenLockDetector._is_locked_darwin()
        if sys_platform == "linux":
            return ScreenLockDetector._is_locked_linux()
        # Unsupported platform – be conservative
        return True
