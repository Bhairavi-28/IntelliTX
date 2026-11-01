"""Runs the compiled C++ engine (itx_cli) and returns its JSON output."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from backend.database import ROOT, load_dotenv


class EngineError(RuntimeError):
    pass


def cli_path() -> Path:
    load_dotenv()
    p = Path(os.environ.get("ITX_CLI", "build/itx_cli"))
    return p if p.is_absolute() else ROOT / p


def run_cli(command: str, timeout: int = 300, **options) -> dict:
    """run_cli('bench', policy='wait_die', workers=4) -> parsed JSON dict.

    Exit codes: 0 ok, 1 ran but a correctness check failed (JSON is still returned), 2 bad input."""
    exe = cli_path()
    if not exe.exists():
        raise EngineError(f"engine binary not found at {exe}; build it first (cmake --build build)")
    args = [str(exe), command]
    for key, value in options.items():
        if value is None:
            continue
        args += ["--" + key.replace("_", "-"), str(value)]
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise EngineError(f"engine timed out after {timeout}s") from exc
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise EngineError(f"engine produced invalid output (exit {proc.returncode}): {proc.stderr[:300]}") from exc
    if proc.returncode == 2 or "error" in data:
        raise EngineError(data.get("error", "engine rejected the request"))
    data["_exit_code"] = proc.returncode
    return data
