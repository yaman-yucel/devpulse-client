#!/usr/bin/env python3
"""Main entry point for DevPulse client when run as a module."""

from .cli.commands import app


def main():
    app()


if __name__ == "__main__":
    main()
