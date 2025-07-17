from __future__ import annotations

import ctypes
import ctypes.wintypes
import subprocess

from tracker.config.tracker_settings import tracker_settings


class WindowTitleProvider:
    @staticmethod
    def _current_title_win32() -> str:
        try:
            user32 = ctypes.windll.user32
            hwnd = user32.GetForegroundWindow()
            if hwnd:
                length = user32.GetWindowTextLengthW(hwnd)
                buff = ctypes.create_unicode_buffer(length + 1)
                user32.GetWindowTextW(hwnd, buff, length + 1)
                return buff.value or "N/A"
        except Exception:
            pass
        return "N/A"

    @staticmethod
    def _current_title_darwin() -> str:
        try:
            title = subprocess.check_output(
                [
                    "osascript",
                    "-e",
                    'tell application "System Events" to get title of (process 1 where frontmost is true)',
                ]
            )
            return title.decode().strip() or "N/A"
        except Exception:
            pass
        return "N/A"

    @staticmethod
    def _current_title_linux() -> str:
        try:
            win_id = subprocess.check_output(["xdotool", "getwindowfocus"]).strip()
            title = subprocess.check_output(["xdotool", "getwindowname", win_id]).decode().strip()
            return title or "N/A"
        except Exception:
            pass
        return "N/A"

    @staticmethod
    def current_title() -> str:
        match tracker_settings.system:
            case "win32":
                return WindowTitleProvider._current_title_win32()
            case "darwin":
                return WindowTitleProvider._current_title_darwin()
            case "linux":
                return WindowTitleProvider._current_title_linux()
            case _:
                return "Error: Unsupported system, window title cannot be determined."
