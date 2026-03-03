import hashlib
import inspect
import json
import textwrap
from pathlib import Path
from typing import Callable

_HASH_FILE = Path(".depio") / "task_hashes.json"


def compute_hash(func: Callable) -> str:
    """Return a SHA-256 hex digest of the function's dedented source code.

    Returns an empty string when the source is unavailable (e.g. lambdas,
    dynamically generated functions), which is treated as "unchanged" so
    those tasks are never spuriously re-triggered.
    """
    try:
        src = textwrap.dedent(inspect.getsource(func)).strip()
    except OSError:
        return ""
    return hashlib.sha256(src.encode()).hexdigest()


def _load() -> dict:
    if not _HASH_FILE.exists():
        return {}
    return json.loads(_HASH_FILE.read_text())


def _save(hashes: dict):
    _HASH_FILE.parent.mkdir(exist_ok=True)
    _HASH_FILE.write_text(json.dumps(hashes, indent=2))


def has_code_changed(key: str, func: Callable) -> bool:
    """Return True if the stored hash for *key* differs from *func*'s current hash.

    A missing key (first run) is treated as "changed" so the task runs at
    least once and establishes a baseline hash.
    """
    current = compute_hash(func)
    if not current:          # uninspectable function — skip check
        return False
    stored = _load().get(key)
    return stored != current


def record_hash(key: str, func: Callable):
    """Persist the current hash of *func* under *key*."""
    current = compute_hash(func)
    if not current:
        return
    hashes = _load()
    hashes[key] = current
    _save(hashes)
