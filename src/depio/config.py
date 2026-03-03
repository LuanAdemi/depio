import json
from pathlib import Path

_CONFIG_DIR  = Path(".depio")
_CONFIG_FILE = _CONFIG_DIR / "config.json"

_DEFAULTS = {
    "pipeline": {
        "refreshrate": 1.0
    },
    "task": {
        "default_buildmode": "IF_MISSING",
        "max_age_seconds": 86400,
        "code_hash_method": "source"
    },
    "executor": {
        "parallel": {},
        "slurm": {
            "max_jobs_pending": 45,
            "max_jobs_queued": 20,
            "partition": "gpu",
            "time_minutes": 2880,
            "mem_gb": 32,
            "gpus_per_node": 0
        }
    }
}


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base (non-destructive)."""
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def get_config() -> dict:
    """Load config from .depio/config.json, creating it with defaults on first run."""
    if not _CONFIG_FILE.exists():
        _CONFIG_DIR.mkdir(exist_ok=True)
        _CONFIG_FILE.write_text(json.dumps(_DEFAULTS, indent=2))
        print(f"[depio] Created default config at {_CONFIG_FILE}")
        return dict(_DEFAULTS)
    with _CONFIG_FILE.open() as f:
        user_cfg = json.load(f)
    return _deep_merge(_DEFAULTS, user_cfg)
