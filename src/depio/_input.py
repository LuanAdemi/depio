"""Keyboard input handling for the Pipeline TUI.

All functions receive the Pipeline object so they can read and mutate
its interactive state (selection, pause flag, etc.).
They are private to the depio package (_-prefixed module).
"""
from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .Pipeline import Pipeline


def read_key() -> str:
    """Read one logical keypress from stdin at the OS level.

    Uses ``os.read()`` instead of ``sys.stdin.read()`` so that
    ``select.select()`` and the actual reads both operate on the OS buffer.
    Python's ``BufferedReader`` would drain all bytes of a multi-byte escape
    sequence in one call, making subsequent ``select()`` checks return False.
    """
    import select
    import os

    fd = sys.stdin.fileno()
    b = os.read(fd, 1)
    if b == b'\x1b':
        if select.select([fd], [], [], 0.05)[0]:
            b2 = os.read(fd, 1)
            if b2 == b'[' and select.select([fd], [], [], 0.05)[0]:
                b3 = os.read(fd, 1)
                return {b'A': 'up', b'B': 'down'}.get(b3, 'esc')
        return 'esc'
    if b in (b'\n', b'\r'):
        return 'enter'
    return b.decode('utf-8', errors='replace')


def check_for_keypress(p: "Pipeline") -> bool:
    """Check stdin for a keypress and update pipeline interactive state.

    Returns ``True`` if a key was handled so the caller can force an
    immediate TUI redraw; ``False`` if no input was available.
    """
    try:
        import select

        if not (hasattr(select, 'select') and hasattr(sys.stdin, 'fileno')):
            return False
        if not select.select([sys.stdin], [], [], 0.0)[0]:
            return False

        key = read_key()
        current_time = time.time()

        if current_time - p.last_key_press_time > 1.0:
            p.key_sequence = []
        p.last_key_press_time = current_time
        p.key_sequence.append(key)

        if key.lower() == 'p':
            if not p._pipeline_done:
                p.paused = True
                p.last_command_message = "✓ Pipeline paused (press 'r' to resume)"
            p._quit_confirmation_pending = False
            p.key_sequence = []

        elif key.lower() == 'r':
            if not p._pipeline_done:
                p.paused = False
                p.last_command_message = "✓ Pipeline resumed"
            p._quit_confirmation_pending = False
            p.key_sequence = []

        elif p._quit_confirmation_pending and key.lower() == 'y':
            p.last_command_message = "✓ Shutting down..."
            p.exit_with_failed_tasks()

        elif p._quit_confirmation_pending and key.lower() == 'n':
            p._quit_confirmation_pending = False
            p.last_command_message = "✓ Quit cancelled"
            p.key_sequence = []

        elif key.lower() == 'q':
            if p._pipeline_done:
                if p._pipeline_failed:
                    p.exit_with_failed_tasks()
                else:
                    p.exit_successful()
            else:
                if not p._quit_confirmation_pending:
                    p._quit_confirmation_pending = True
                    p.last_command_message = "⚠ Pipeline still running. Press 'y' to confirm quit or 'n' to cancel"
                    p.key_sequence = []
                else:
                    p.last_command_message = "✓ Shutting down..."
                    p.exit_with_failed_tasks()

        elif key == 'up':
            n = len(p.tasks)
            if n:
                start = 0 if p._selected_task_idx is None else p._selected_task_idx
                p._selected_task_idx = (start - 1) % n

        elif key == 'down':
            n = len(p.tasks)
            if n:
                start = -1 if p._selected_task_idx is None else p._selected_task_idx
                p._selected_task_idx = (start + 1) % n

        elif key == 'enter':
            if p._selected_task_idx is not None:
                p._detail_mode = True

        elif key == 'esc':
            if p._detail_mode:
                p._detail_mode = False
            else:
                p._selected_task_idx = None

        return True

    except (ImportError, OSError):
        return False
