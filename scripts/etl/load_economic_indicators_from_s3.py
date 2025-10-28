"""
Run a Snowflake SQL runbook for economic indicators load
"""
import sys
import subprocess
from pathlib import Path

def main():
    runbook = Path(__file__).parent.parent.parent / "snowflake" / "runbooks" / "load_economic_indicators_from_s3.sql"
    runner = Path(__file__).parent.parent / "github_actions" / "snowflake_run_sql_file.py"
    print(f"🗄️ Loading economic indicators data into Snowflake using {runbook.name}...")
    result = subprocess.run([sys.executable, str(runner), str(runbook)], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Snowflake load failed!")
    print("✅ Economic indicators load complete.")

if __name__ == "__main__":
    main()
