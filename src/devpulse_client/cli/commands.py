"""Command-line interface for DevPulse client.

This module provides CLI commands for enrollment and running the client.
"""

from __future__ import annotations

import typer

from ..core.clean_app import DevPulseClient

app = typer.Typer(help="DevPulse Client - Activity tracking with simplified MAC address enrollment")


def setup_logging():
    """Setup basic logging for CLI."""
    import sys

    from loguru import logger

    logger.remove()  # Remove default logger
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
    )


@app.command(name="enroll")
def enroll(
    server: str = typer.Option("http://localhost:8000", help="DevPulse server URL"),
    username: str = typer.Option(..., help="Username for enrollment"),
    user_email: str = typer.Option(..., help="User email for enrollment"),
):
    """Enroll this device using MAC address."""
    setup_logging()
    from loguru import logger

    client = DevPulseClient(server)
    if client.enroll(username=username, user_email=user_email):
        logger.info("✅ Enrollment completed successfully!")
        logger.info("You can now run the client with: devpulse-client run")
        typer.Exit(code=0)
    else:
        logger.error("❌ Enrollment failed!")
        typer.Exit(code=1)


@app.command()
def run(
    server: str = typer.Option("http://localhost:8000", help="DevPulse server URL"),
    user_email: str = typer.Option(..., help="User email for validation"),
):
    """Run the DevPulse client."""
    setup_logging()
    from loguru import logger

    client = DevPulseClient(server)
    if not client.start(user_email):
        logger.error("❌ Failed to start client")
        raise typer.Exit(code=1)
    else:
        logger.info("✅ Client started successfully with validation")
        raise typer.Exit(code=0)
    # success = client.run()
    # if success:
    #     logger.info("✅ Client completed successfully")
    #     raise typer.Exit(code=0)
    # else:
    #     logger.error("❌ Client encountered errors")
    #     raise typer.Exit(code=1)


# @app.command()
# def status(server: str = typer.Option("http://localhost:8000", help="DevPulse server URL")):
#     """Show client status."""
#     setup_logging()
#     from loguru import logger

#     client = create_devpulse_client(server)
#     status = client.get_status()
#     logger.info("DevPulse Client Status")
#     logger.info(f"Server: {server}")
#     logger.info(f"Enrolled: {status['enrolled']}")
#     if status["enrolled"]:
#         logger.info(f"Device ID: {status.get('device_id', 'Unknown')}")
#         logger.info(f"User ID: {status.get('user_id', 'Unknown')}")
#         logger.info(f"Running: {status['running']}")
#         if "pipeline_stats" in status:
#             stats = status["pipeline_stats"]
#             pipeline_stats = stats.get("pipeline", {})
#             queue_stats = stats.get("queue", {})
#             sender_stats = stats.get("sender", {})
#             logger.info(f"Uptime: {pipeline_stats.get('uptime_seconds', 0):.1f} seconds")
#             logger.info(f"Queue: {queue_stats.get('current_size', 0)}/{queue_stats.get('max_size', 0)}")
#             logger.info(f"Events sent: {sender_stats.get('total_events_sent', 0)}")
#             logger.info(f"Success rate: {sender_stats.get('success_rate', 0):.1%}")


# @app.command()
# def sync(server: str = typer.Option("http://localhost:8000", help="DevPulse server URL")):
#     """Force sync pending events."""
#     setup_logging()
#     from loguru import logger

#     logger.info("Forcing synchronization of pending events...")
#     client = create_devpulse_client(server)
#     success = client.force_sync()
#     if success:
#         logger.info("✅ Synchronization completed")
#         raise typer.Exit(code=0)
#     else:
#         logger.error("❌ Synchronization failed")
#         raise typer.Exit(code=1)


# @app.command()
# def clear():
#     """Clear stored credentials."""
#     setup_logging()
#     from loguru import logger

#     from ..enroll.client.enrollment_client import CredentialClient

#     credential_client = CredentialClient(server)

#     if not credential_client.is_enrolled():
#         logger.info("No credentials found to clear")
#         raise typer.Exit(code=0)

#     success = credential_client.clear_credentials()
#     if success:
#         logger.info("✅ Credentials cleared successfully")
#         raise typer.Exit(code=0)
#     else:
#         logger.error("❌ Failed to clear credentials")
#         raise typer.Exit(code=1)
