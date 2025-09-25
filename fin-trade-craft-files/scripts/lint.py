#!/usr/bin/env python3
"""
Linting script for the fin-trade-craft project.
Runs ruff linter and black formatter on the codebase.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return success status."""

    try:
        result = subprocess.run(cmd, check=False, capture_output=False)
        return result.returncode == 0
    except Exception:
        return False


def main():
    """Run linting and formatting checks."""
    project_root = Path(__file__).parent.parent


    # Change to project directory
    import os

    os.chdir(project_root)

    success = True

    # Run ruff linter
    success &= run_command(["uv", "run", "ruff", "check", "."], "Ruff linting")

    # Run black formatter check
    success &= run_command(
        ["uv", "run", "black", "--check", "."], "Black formatting check"
    )

    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
