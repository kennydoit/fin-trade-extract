#!/usr/bin/env python3
"""
Auto-fix script for the fin-trade-craft project.
Runs ruff auto-fix and black formatter to clean up code issues.
"""

import subprocess
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return success status."""

    try:
        result = subprocess.run(cmd, check=False, capture_output=False)
        return result.returncode == 0
    except Exception:
        return False


def main():
    """Run auto-fixing tools."""
    project_root = Path(__file__).parent.parent


    # Change to project directory
    import os

    os.chdir(project_root)

    # Run ruff auto-fix
    run_command(["uv", "run", "ruff", "check", "--fix", "."], "Ruff auto-fix")

    # Run black formatter
    run_command(["uv", "run", "black", "."], "Black formatting")



if __name__ == "__main__":
    main()
