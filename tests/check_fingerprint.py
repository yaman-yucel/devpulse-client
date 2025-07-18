from src.devpulse_client.credential.enroll.collectors.device_collector import DeviceFingerprintCollector

collector = DeviceFingerprintCollector()

print(collector.collect_fingerprint())
