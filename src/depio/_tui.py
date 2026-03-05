"""TUI rendering for the Pipeline live display.

All functions receive the Pipeline object and return Rich renderables.
They are private to the depio package (_-prefixed module).
"""
from __future__ import annotations

import shutil
from typing import TYPE_CHECKING, Dict

from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from .TaskStatus import TaskStatus

if TYPE_CHECKING:
    from .Pipeline import Pipeline


_STATUS_DISPLAY = {
    TaskStatus.PENDING:   ("·",  "dim"),
    TaskStatus.WAITING:   ("◉",  "blue"),
    TaskStatus.RUNNING:   ("●",  "bold yellow"),
    TaskStatus.FINISHED:  ("✓",  "bold green"),
    TaskStatus.SKIPPED:   ("✓",  "green"),
    TaskStatus.FAILED:    ("✗",  "bold red"),
    TaskStatus.DEPFAILED: ("✗",  "red"),
    TaskStatus.CANCELED:  ("⊘",  "dim"),
    TaskStatus.HOLD:      ("⏸", "dim"),
    TaskStatus.UNKNOWN:   ("?",  "dim"),
}


def render_task_list(p: "Pipeline") -> Panel:
    has_slurm = any(task.slurmjob is not None for task in p.tasks)

    # Build the display list: tasks that pass the visibility filter.
    display_list = [
        (i, task) for i, task in enumerate(p.tasks)
        if not (p.HIDE_SUCCESSFUL_TERMINATED_TASKS and task.is_in_successful_terminal_state)
    ]
    total_display = len(display_list)

    # Determine how many rows fit in the terminal.
    try:
        term_height = shutil.get_terminal_size().lines
    except Exception:
        term_height = 24
    # Overhead: panel borders (2), table header+separator (2), Rule (1),
    #           footer (2), scroll hints (2) = 9
    max_rows = max(3, term_height - 9)

    # Find where the selected task sits in the display list.
    selected_display_idx = None
    if p._selected_task_idx is not None:
        for di, (i, _) in enumerate(display_list):
            if i == p._selected_task_idx:
                selected_display_idx = di
                break

    # Auto-scroll to keep the selection inside the viewport.
    if selected_display_idx is not None:
        if selected_display_idx < p._scroll_offset:
            p._scroll_offset = selected_display_idx
        elif selected_display_idx >= p._scroll_offset + max_rows:
            p._scroll_offset = selected_display_idx - max_rows + 1
    p._scroll_offset = max(0, min(p._scroll_offset, max(0, total_display - max_rows)))

    visible = display_list[p._scroll_offset : p._scroll_offset + max_rows]
    above   = p._scroll_offset
    below   = total_display - (p._scroll_offset + len(visible))

    has_variants = any(task.description for task in p.tasks)

    table = Table(
        box=box.SIMPLE_HEAD,
        show_edge=False,
        padding=(0, 1),
        expand=True,
        header_style="bold",
    )
    table.add_column("#",    width=4,  style="dim")
    table.add_column("Name", ratio=2 if has_variants else 1)
    if has_variants:
        table.add_column("Description", ratio=1, style="dim")
    if has_slurm:
        table.add_column("Slurm ID",     width=10)
        table.add_column("Cluster State", width=14)
    table.add_column("Status", width=18)
    table.add_column("Time",   width=6,  justify="right")
    table.add_column("Deps",   width=8,  style="dim")

    # Count all tasks for the status summary (including hidden ones).
    status_counts: Dict[str, int] = {}
    for task in p.tasks:
        label = task.status[1].upper()
        status_counts[label] = status_counts.get(label, 0) + 1

    for i, task in visible:
        s, stext, _, slurm_state = task.status
        label = stext.upper()
        sym, style = _STATUS_DISPLAY.get(s, ("?", "dim"))
        badge = Text.assemble((sym + " ", style), (label, style))

        d = task.get_duration()
        duration = f"{d // 60}:{d % 60:02d}" if d else "–"

        deps_str = ",".join(str(t._queue_id) for t in (task.task_dependencies or [])) or "–"
        row_style = "bold reverse" if i == p._selected_task_idx else None

        variant_cells = [task.description or ""] if has_variants else []

        if has_slurm:
            table.add_row(
                str(task.id), task.name, *variant_cells,
                str(task.slurmid or "–"), str(slurm_state or "–"),
                badge, duration, deps_str,
                style=row_style,
            )
        else:
            table.add_row(
                str(task.id), task.name, *variant_cells,
                badge, duration, deps_str,
                style=row_style,
            )

    footer = Text()
    if p._pipeline_done:
        if p._pipeline_failed:
            footer.append("✗ FAILED", style="bold red")
        else:
            footer.append("✓ DONE", style="bold green")
    elif p.paused:
        footer.append("⏸ PAUSED", style="bold yellow")
    else:
        # Pulsating animation: cycle through 4 brightness levels
        frames = ["●", "◐", "○", "◑"]
        frame = frames[(p._animation_frame // 2) % len(frames)]
        p._animation_frame += 1
        footer.append(frame + " RUNNING", style="bold green")
    footer.append("  ")
    for label, count in status_counts.items():
        footer.append(f"{count} {label}  ", style="dim")
    footer.append("\n")
    footer.append("↑↓", style="bold cyan")
    footer.append(" select  ", style="dim")
    footer.append("Enter", style="bold cyan")
    footer.append(" detail  ", style="dim")
    footer.append("Esc", style="bold cyan")
    footer.append(" back  ", style="dim")
    if not p._pipeline_done:
        footer.append("P", style="bold cyan")
        footer.append("/", style="dim")
        footer.append("R", style="bold cyan")
        footer.append(" pause/resume  ", style="dim")
    footer.append("Q", style="bold cyan")
    footer.append(" quit", style="dim")
    
    if p.last_command_message and not p._quit_confirmation_pending:
        from rich.text import Text as RichText
        msg = RichText.from_markup(p.last_command_message) if '[' in p.last_command_message else RichText(p.last_command_message, style="italic dim cyan")
        footer.append("\n")
        footer.append(msg)

    done_tag = ""
    if p._pipeline_done:
        done_tag = "  [bold red]FAILED[/bold red]" if p._pipeline_failed else "  [bold green]DONE[/bold green]"

    group_parts = []
    if above:
        group_parts.append(Text(f"  ▲ {above} more", style="dim"))
    group_parts.append(table)
    if below:
        group_parts.append(Text(f"  ▼ {below} more", style="dim"))
    group_parts.extend([Rule(style="bright_black"), footer])

    if p.last_command_message and p._quit_confirmation_pending:
        from rich.text import Text as RichText
        msg = RichText.from_markup(p.last_command_message)
        msg_panel = Panel(msg, border_style="bold red", padding=(0, 2))
        group_parts.append(msg_panel)

    return Panel(
        Group(*group_parts),
        title=f"[bold]depio[/bold]  [dim]{p.name}[/dim]{done_tag}",
        border_style="bright_black",
        padding=(0, 1),
    )


def render_task_detail(p: "Pipeline") -> Panel:
    task = p.tasks[p._selected_task_idx]
    s, stext, _, _ = task.status
    sym, style = _STATUS_DISPLAY.get(s, ("?", "dim"))

    d = task.get_duration()
    duration = f"{d // 60}:{d % 60:02d}" if d else "–"

    header = Text.assemble(
        ("# ", "dim"),
        (str(task.id), "bold cyan"),
        ("  ", ""),
        (task.name, "bold"),
        ("  ", ""),
        (sym + " ", style),
        (stext.upper(), style),
        ("  ", "dim"),
        (duration, "dim"),
    )

    stdout_content = task.get_stdout()
    stdout_body = Text(stdout_content) if stdout_content else Text("(no output)", style="dim")
    stdout_panel = Panel(stdout_body, title="stdout", border_style="dim", padding=(0, 1))

    parts = [header, Rule(style="bright_black"), stdout_panel]

    stderr_content = task.get_stderr()
    if stderr_content:
        parts.append(Panel(Text(stderr_content), title="stderr", border_style="red dim", padding=(0, 1)))

    n = len(p.tasks)
    pos = (p._selected_task_idx or 0) + 1
    footer = Text.assemble(
        ("↑↓", "bold cyan"), (f"  prev/next task ({pos}/{n})  ", "dim"),
        ("Esc", "bold cyan"), ("  back to overview", "dim"),
    )
    parts.extend([Rule(style="bright_black"), footer])

    return Panel(
        Group(*parts),
        title=f"[bold]Task detail[/bold]  [dim]{task.name}[/dim]",
        border_style="cyan",
        padding=(0, 1),
    )
