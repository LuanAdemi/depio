import enum


class BuildMode(enum.Enum):
    """
    Controls when a Task is executed.

    Mode        | Runs when
    ------------|-----------------------------------------------------------
    NEVER       | Never — always skipped
    IF_MISSING  | Any product file is absent
    ALWAYS      | Every run, unconditionally
    IF_NEW      | Any product is missing, OR any upstream task ran
    IF_OLDER    | Any product is missing, OR any product is older than its
                |   path dependencies (make-style timestamp comparison)
    IF_OLD          | Any product is missing, OR any product is older than a
                    |   configurable age threshold (max_age seconds;
                    |   default (24h) set in .depio/config.json → task.max_age_seconds)
    IF_CODE_CHANGED | Any product is missing, OR the task function's source
                    |   code changed since the last successful run
                    |   (hashes stored in .depio/task_hashes.json)
    """
    NEVER = enum.auto()
    IF_MISSING = enum.auto()
    ALWAYS = enum.auto()
    IF_NEW = enum.auto()
    IF_OLDER = enum.auto()
    IF_OLD = enum.auto()
    IF_CODE_CHANGED = enum.auto()
