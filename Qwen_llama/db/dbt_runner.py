"""Optional dbt run hook for post-ingestion transformations."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any


def _enabled() -> bool:
    return os.getenv("DBT_AUTO_RUN", "0") == "1"


def run_dbt_models() -> dict[str, Any]:
    if not _enabled():
        return {"enabled": False, "status": "skipped", "reason": "DBT_AUTO_RUN=0"}

    dbt_cmd = shutil.which("dbt")
    if not dbt_cmd:
        return {
            "enabled": True,
            "status": "error",
            "reason": "dbt binary not found in PATH",
        }

    project_root = Path(__file__).resolve().parents[1]
    project_dir = Path(os.getenv("DBT_PROJECT_DIR", str(project_root / "dbt")))
    profiles_dir = Path(os.getenv("DBT_PROFILES_DIR", str(project_dir)))
    target = os.getenv("DBT_TARGET", "dev")

    cmd = [
        dbt_cmd,
        "run",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--target",
        target,
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
    except Exception as exc:
        return {
            "enabled": True,
            "status": "error",
            "reason": f"dbt execution failed: {exc}",
            "command": cmd,
        }

    output_tail = "\n".join((proc.stdout or "").splitlines()[-20:])
    error_tail = "\n".join((proc.stderr or "").splitlines()[-20:])

    return {
        "enabled": True,
        "status": "ok" if proc.returncode == 0 else "error",
        "return_code": proc.returncode,
        "target": target,
        "project_dir": str(project_dir),
        "profiles_dir": str(profiles_dir),
        "stdout_tail": output_tail,
        "stderr_tail": error_tail,
    }
