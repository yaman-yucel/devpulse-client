# DevPulse Client

A modern, clean-architecture activity tracking client with secure enrollment, dynamic configuration, and robust event pipeline. Designed for reliability, crash recovery, and easy integration with analytics backends.

## Architecture Overview

```
Event Hooks → Queuer → WAL (Write-Ahead Log) → Batcher → Sender → API
```

- **Event Hooks**: Capture activity, window, heartbeat, and screenshot events.
- **Queuer**: In-memory event queue with spillover to WAL on backpressure.
- **WAL**: Durable local storage (SQLite) for crash recovery and offline operation.
- **Batcher**: Groups events for efficient sending.
- **Sender**: Secure HTTPS/gRPC batch upload to server API.

## Key Features

- Device enrollment with admin-issued secret
- Secure credential storage (`~/.devpulse/credentials.json`)
- Dynamic config from server
- Robust event pipeline with WAL-based durability
- Tracks activity, window focus, heartbeats, screenshots
- CLI for enrollment, running, status, and sync
- Extensible, testable, and clean codebase

## Installation

Requires Python 3.13+ and [uv](https://github.com/astral-sh/uv) for package management.

```sh
uv pip install .
```

## Enrollment & Setup

Before running, enroll your device with an admin-provided secret:

```sh
devpulse-client enroll \
  --username "$(whoami)" \
  --server https://devpulse.internal \
  --enrollment-secret "$(cat /etc/devpulse/enroll_token)"
```

This stores credentials at `~/.devpulse/credentials.json` (chmod 600).

## Usage

Run the client (after enrollment):

```sh
devpulse-client run --server https://devpulse.internal
```

Other CLI commands:

- `devpulse-client status` — Show enrollment and pipeline status
- `devpulse-client sync` — Force sync pending events

You can also run as a module:

```sh
uv run -m devpulse_client
```

## Configuration

Most settings are auto-configured, but can be overridden via environment variables or `.env`:

- `DEVPULSE_SERVER_URL` — API endpoint
- `DEVPULSE_USERNAME` — Username
- `DEVPULSE_HEARTBEAT_INTERVAL` — Heartbeat interval (seconds)
- `DEVPULSE_SCREENSHOT_INTERVAL` — Screenshot interval (seconds)

See `src/devpulse_client/config/tracker_settings.py` for all options.

## Event Types Tracked

- **Activity**: Active/inactive, lock/unlock, shutdown, etc.
- **Heartbeat**: Regular and final heartbeats
- **Window**: Window focus changes, durations
- **Screenshot**: Periodic or triggered screenshots

## Security & Credential Storage

- Credentials stored at `~/.devpulse/credentials.json` (chmod 600)
- API key and device ID issued on enrollment
- No sensitive data sent without explicit enrollment

## Development

- Install dev dependencies: `uv pip install -r requirements.txt`
- Run tests: `uv pip install pytest && uv run -m pytest`
- Main code: `src/devpulse_client/`

---

© 2024 Yaman Yucel. MIT License.
