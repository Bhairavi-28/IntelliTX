"""MySQL access layer. Configuration comes from environment variables (see .env.example)."""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import mysql.connector

ROOT = Path(__file__).resolve().parent.parent


def load_dotenv(path: Path | None = None) -> None:
    """Minimal .env loader (does not override variables that are already set)."""
    path = path or ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def db_config() -> dict:
    load_dotenv()
    password = os.environ.get("MYSQL_PASSWORD")
    if password is None:
        raise RuntimeError("MYSQL_PASSWORD is not set. Copy .env.example to .env and edit it.")
    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "intellitx"),
        "password": password,
        "database": os.environ.get("MYSQL_DATABASE", "intellitx"),
    }


def connect():
    return mysql.connector.connect(**db_config())


@contextmanager
def cursor(dictionary: bool = True):
    """Commits on success, rolls back on error."""
    conn = connect()
    cur = conn.cursor(dictionary=dictionary)
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def split_sql(script: str) -> list[str]:
    """Split a script into statements, honouring `DELIMITER` lines (needed for triggers/procs)."""
    statements: list[str] = []
    delimiter = ";"
    buffer: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if stripped.upper().startswith("DELIMITER "):
            delimiter = stripped.split(None, 1)[1]
            continue
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(delimiter):
            stmt = "\n".join(buffer).rstrip()
            stmt = stmt[: -len(delimiter)].rstrip()
            if stmt:
                statements.append(stmt)
            buffer = []
    if "".join(buffer).strip():
        statements.append("\n".join(buffer).strip())
    return statements


def run_script(path: Path) -> int:
    statements = split_sql(path.read_text())
    conn = connect()
    try:
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
            while cur.nextset():  # drain extra result sets (CALL)
                pass
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return len(statements)


def init_schema(with_seed: bool = True) -> None:
    run_script(ROOT / "storage" / "schema.sql")
    if with_seed:
        run_script(ROOT / "storage" / "seed.sql")


def query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    with cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def ping() -> tuple[bool, str]:
    try:
        rows = query("SELECT VERSION() AS v")
        return True, rows[0]["v"]
    except Exception as exc:  # reported to the UI, not swallowed silently
        return False, str(exc)
