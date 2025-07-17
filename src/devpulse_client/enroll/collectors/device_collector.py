"""Device fingerprint collection functionality."""

import platform
import subprocess
import uuid
from typing import Optional

from loguru import logger

from ..models import DeviceFingerprint


class DeviceFingerprintCollector:
    """Collects hardware fingerprint information for device identification."""

    def collect_fingerprint(self, mac_only: bool = False) -> DeviceFingerprint:
        """Collect comprehensive device fingerprint information."""
        try:
            fingerprint = DeviceFingerprint(mac_address=self._get_mac_address())
            if mac_only:
                return fingerprint

            # Collect serial number
            fingerprint.serial_number = self._get_serial_number()

            # Collect CPU information
            fingerprint.processor = platform.processor()
            fingerprint.architecture = platform.machine()
            fingerprint.cpu_info = f"{platform.processor()} ({platform.machine()})"

            # Collect memory information
            fingerprint.memory_gb = self._get_memory_info()

            logger.debug(f"Collected device fingerprint: MAC={fingerprint.mac_address}, Serial={fingerprint.serial_number}, CPU={fingerprint.cpu_info}, Memory={fingerprint.memory_gb}GB")

            return fingerprint

        except Exception as e:
            logger.warning(f"Failed to collect device fingerprint: {e}")
            return DeviceFingerprint()

    def _get_mac_address(self) -> Optional[str]:
        """Get the MAC address of the primary network interface."""
        try:
            mac = uuid.getnode()
            # Check if it's a real MAC (not a random one)
            if (mac >> 40) % 2:
                return None
            return ":".join(f"{(mac >> ele) & 0xFF:02x}" for ele in range(40, -1, -8))
        except Exception as e:
            logger.debug(f"Failed to get MAC address: {e}")
            return None

    def _get_serial_number(self) -> Optional[str]:
        """Get the device serial number (platform-specific)."""
        try:
            system = platform.system().lower()

            if system == "linux":
                return self._get_linux_serial()
            elif system == "darwin":
                return self._get_macos_serial()
            elif system == "windows":
                return self._get_windows_serial()
            else:
                return None

        except Exception as e:
            logger.debug(f"Failed to get serial number: {e}")
            return None

    def _get_linux_serial(self) -> Optional[str]:
        """Get serial number on Linux."""
        try:
            # Try reading from DMI
            with open("/sys/class/dmi/id/product_serial", "r") as f:
                serial = f.read().strip()
                return serial if serial and serial != "To Be Filled By O.E.M." else None
        except Exception:
            try:
                # Try using dmidecode as fallback
                result = subprocess.run(["dmidecode", "-s", "system-serial-number"], capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    serial = result.stdout.strip()
                    return serial if serial and serial != "To Be Filled By O.E.M." else None
            except Exception:
                pass
        return None

    def _get_macos_serial(self) -> Optional[str]:
        """Get serial number on macOS."""
        try:
            result = subprocess.run(["system_profiler", "SPHardwareDataType"], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                for line in result.stdout.split("\n"):
                    if "Serial Number" in line:
                        return line.split(":")[1].strip()
        except Exception:
            try:
                # Alternative method using ioreg
                result = subprocess.run(["ioreg", "-c", "IOPlatformExpertDevice", "-d", "2"], capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    for line in result.stdout.split("\n"):
                        if "IOPlatformSerialNumber" in line:
                            return line.split("=")[1].strip().strip('"')
            except Exception:
                pass
        return None

    def _get_windows_serial(self) -> Optional[str]:
        """Get serial number on Windows."""
        try:
            result = subprocess.run(["wmic", "bios", "get", "serialnumber"], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")
                if len(lines) >= 2:
                    serial = lines[1].strip()
                    return serial if serial and serial != "To Be Filled By O.E.M." else None
        except Exception:
            pass
        return None

    def _get_memory_info(self) -> Optional[float]:
        """Get total system memory in GB."""
        try:
            import psutil

            memory_bytes = psutil.virtual_memory().total
            memory_gb = memory_bytes / (1024**3)
            return round(memory_gb, 2)
        except ImportError:
            # Fallback without psutil
            try:
                system = platform.system().lower()
                if system == "linux":
                    with open("/proc/meminfo", "r") as f:
                        for line in f:
                            if line.startswith("MemTotal:"):
                                kb = int(line.split()[1])
                                return round(kb / (1024**2), 2)
                elif system == "darwin":
                    result = subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        bytes_mem = int(result.stdout.strip())
                        return round(bytes_mem / (1024**3), 2)
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"Failed to get memory info: {e}")
        return None
