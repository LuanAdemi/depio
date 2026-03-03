from typing import Set, Dict, List, Optional, Callable
from pathlib import Path
import queue
import re
import time
import sys

from rich.console import Console
from rich.live import Live

from .hooks import TaskResult, PipelineResult, make_save_hook as _make_save_hook
from .config import get_config as _get_config
from ._tui import render_task_list, render_task_detail
from ._input import check_for_keypress
from .stdio_helpers import enable_proxy
from .Task import Task
from .TaskStatus import TaskStatus
from .Executors import AbstractTaskExecutor
from .exceptions import (
    ProductAlreadyRegisteredException,
    TaskNotInQueueException,
    DependencyNotAvailableException,
)


class Pipeline:
    def __init__(self, depioExecutor: AbstractTaskExecutor, name: str = "NONAME",
                 clear_screen: bool = True,
                 hide_successful_terminated_tasks: bool = False,
                 submit_only_if_runnable: bool = False,
                 quiet: bool = False,
                 refreshrate: float = None,
                 exit_when_done: bool = False,
                 on_task_finished: Optional[Callable[[TaskResult], None]] = None,
                 on_task_failed: Optional[Callable[[TaskResult], None]] = None,
                 on_pipeline_finished: Optional[Callable[[PipelineResult], None]] = None):

        # Flags
        _cfg = _get_config()
        self.CLEAR_SCREEN: bool = clear_screen
        self.QUIET: bool = quiet
        self.REFRESHRATE: float = refreshrate if refreshrate is not None else _cfg["pipeline"]["refreshrate"]
        self.HIDE_SUCCESSFUL_TERMINATED_TASKS: bool = hide_successful_terminated_tasks
        self.SUBMIT_ONLY_IF_RUNNABLE: bool = submit_only_if_runnable
        self.EXIT_WHEN_DONE: bool = exit_when_done
        self.on_task_finished: Optional[Callable[[TaskResult], None]] = on_task_finished
        self.on_task_failed: Optional[Callable[[TaskResult], None]] = on_task_failed
        self.on_pipeline_finished: Optional[Callable[[PipelineResult], None]] = on_pipeline_finished

        self.name: str = name
        self.handled_tasks: Optional[List[Task]] = None
        self.tasks: List[Task] = []
        self._task_set: set = set()          # mirrors self.tasks for O(1) duplicate lookup
        self.depioExecutor: AbstractTaskExecutor = depioExecutor
        self.registered_products: Set[Path] = set()
        self._registered_product_strs: Set[str] = set()

        # Interactive TUI state
        self.paused = False
        self.command_queue = queue.Queue()
        self.last_command_message = ""
        self.last_key_press_time = 0
        self.key_sequence = []
        self._selected_task_idx: Optional[int] = None
        self._detail_mode: bool = False
        self._live: Optional["Live"] = None
        self._scroll_offset: int = 0
        self._pipeline_done: bool = False
        self._pipeline_failed: bool = False
        self._hook_fired_tasks: set = set()

    # ── Task registration ──────────────────────────────────────────────────────

    def add_tasks(self, tasks: List[Task]) -> None:
        for task in tasks:
            self.add_task(task)

    def add_task(self, task: Task) -> None:
        # Already registered — return the existing instance
        if task in self._task_set:
            return self.tasks[self.tasks.index(task)]

        # Reject duplicate products
        products_already_registered: List[str] = [
            str(p) for p in task.products if str(p) in self._registered_product_strs
        ]
        if products_already_registered:
            print(task.cleaned_args)
            for p in products_already_registered:
                t = next(t for t in self.tasks if str(p) in {str(pr) for pr in t.products})
                print(f"Product {p} is already registered by task {t.name}. "
                      f"Now again registered by task {task.name}.")
            raise ProductAlreadyRegisteredException(
                f"The product/s {products_already_registered} is/are already registered. "
                f"Each output can only be registered from one task.")

        # Reject out-of-order task dependencies
        missing_tasks: List[Task] = [
            t for t in task.dependencies if isinstance(t, Task) and t not in self._task_set
        ]
        if missing_tasks:
            raise TaskNotInQueueException(
                f"Add the tasks into the queue in the correct order. "
                f"The following task/s is/are missing: {missing_tasks}.")

        self.registered_products.update(task.products)
        self._registered_product_strs.update(str(p) for p in task.products)
        self.tasks.append(task)
        self._task_set.add(task)
        task._queue_id = len(self.tasks)  # TODO Fix this!
        return task

    # ── DAG resolution ─────────────────────────────────────────────────────────

    def _solve_order(self) -> None:
        product_to_task: Dict[Path, Task] = {}
        for task in self.tasks:
            for product in task.products:
                product_to_task[product] = task

        unavailable_dependencies = []

        for task in self.tasks:
            seen_ids = set()
            task.task_dependencies = []
            task.path_dependencies = []

            for d in task.dependencies:
                if isinstance(d, Task):
                    t_id = id(d)
                    if t_id not in seen_ids:
                        seen_ids.add(t_id)
                        task.task_dependencies.append(d)
                        d.add_dependent_task(task)
                else:
                    producing_task = product_to_task.get(d)
                    if producing_task is not None:
                        t_id = id(producing_task)
                        if t_id not in seen_ids:
                            seen_ids.add(t_id)
                            task.task_dependencies.append(producing_task)
                            producing_task.add_dependent_task(task)
                    else:
                        task.path_dependencies.append(d)
                        if not d.exists():
                            unavailable_dependencies.append(d)

        if unavailable_dependencies:
            dep_list = ', '.join(str(d) for d in unavailable_dependencies)
            raise DependencyNotAvailableException(
                f"The following dependencies do not exist and cannot be produced: {dep_list}")

    def _get_non_terminal_tasks(self) -> List[Task]:
        return [task for task in self.tasks if not task.is_in_terminal_state]

    def _get_pending_tasks(self) -> List[Task]:
        return [task for task in self.tasks
                if task.status[0] in [TaskStatus.PENDING, TaskStatus.UNKNOWN]]

    # ── Execution loop ─────────────────────────────────────────────────────────

    def _setup_keyboard(self) -> bool:
        self._old_terminal_settings = None
        try:
            import termios
            import tty
            self._old_terminal_settings = termios.tcgetattr(sys.stdin)
            tty.setcbreak(sys.stdin.fileno())
            return True
        except Exception:
            if not self.QUIET:
                print("Note: Interactive commands not available on this system")
            return False

    def _submit_ready_tasks(self) -> None:
        for task in self.tasks:
            if task in self.handled_tasks:
                continue
            if task.is_ready_for_execution() or self.depioExecutor.handles_dependencies():
                if task.should_run():
                    if not self.SUBMIT_ONLY_IF_RUNNABLE:
                        self.depioExecutor.submit(task, task.task_dependencies)
                        self.handled_tasks.append(task)
                    elif task.is_ready_for_execution():
                        if self.depioExecutor.has_jobs_queued_limit:
                            if len(self._get_non_terminal_tasks()) >= self.depioExecutor.max_jobs_queued:
                                continue
                        elif self.depioExecutor.has_jobs_pending_limit:
                            if len(self._get_pending_tasks()) >= self.depioExecutor.max_jobs_pending:
                                continue
                        self.depioExecutor.submit(task, task.task_dependencies)
                        self.handled_tasks.append(task)

    def _poll_slurm_statuses(self) -> None:
        # Refresh SLURM task statuses so is_in_terminal_state stays current
        # even in quiet mode (where _render is a no-op and task.status is
        # never called via the TUI).
        for task in self.handled_tasks:
            if task.slurmjob is not None and not task.is_in_terminal_state:
                task._update_by_slurmjob()

    def _fire_task_hooks(self) -> None:
        for task in self.tasks:
            if task not in self._hook_fired_tasks and task.is_in_terminal_state:
                self._hook_fired_tasks.add(task)
                result = TaskResult(
                    name=task.name,
                    status=task.status[0],
                    stdout=task.get_stdout(),
                    stderr=task.get_stderr(),
                    duration=float(task.get_duration()),
                    outputs=list(task.products),
                )
                # on_task_finished — global then per-task
                for hook in filter(None, [self.on_task_finished, task.on_finished]):
                    try:
                        hook(result)
                    except Exception as e:
                        self.last_command_message = f"Hook error: {e}"
                # on_task_failed — fires only on direct failure
                if result.status == TaskStatus.FAILED:
                    for hook in filter(None, [self.on_task_failed, task.on_task_failed]):
                        try:
                            hook(result)
                        except Exception as e:
                            self.last_command_message = f"Hook error: {e}"

    def _check_pipeline_completion(self) -> None:
        # Mark done but stay alive so the user can browse stdouts
        if not self._pipeline_done and all(
                task.is_in_terminal_state for task in self.tasks):
            self._pipeline_done = True
            self._pipeline_failed = any(
                task.is_in_failed_terminal_state for task in self.tasks)
            self.last_command_message = (
                "Pipeline finished with failures. Press Q to quit."
                if self._pipeline_failed else
                "All tasks finished. Press Q to quit."
            )
            # on_pipeline_finished
            if self.on_pipeline_finished is not None:
                pipeline_result = PipelineResult(
                    name=self.name,
                    success=not self._pipeline_failed,
                    task_results=[
                        TaskResult(
                            name=t.name,
                            status=t.status[0],
                            stdout=t.get_stdout(),
                            stderr=t.get_stderr(),
                            duration=float(t.get_duration()),
                            outputs=list(t.products),
                        ) for t in self.tasks
                    ],
                )
                try:
                    self.on_pipeline_finished(pipeline_result)
                except Exception as e:
                    self.last_command_message = f"Pipeline hook error: {e}"

    def run(self) -> None:
        enable_proxy()
        self._solve_order()
        self.handled_tasks = []

        restore_terminal = self._setup_keyboard()

        def _render(live, *, refresh=False):
            if self.QUIET:
                return
            renderable = (render_task_detail(self)
                          if self._detail_mode and self._selected_task_idx is not None
                          else render_task_list(self))
            live.update(renderable, refresh=refresh)

        try:
            with Live(screen=True, refresh_per_second=5,
                      redirect_stdout=False, redirect_stderr=False) as live:
                self._live = live
                while True:
                    try:
                        if self.paused:
                            deadline = time.time() + self.REFRESHRATE
                            while time.time() < deadline:
                                key_handled = restore_terminal and check_for_keypress(self)
                                _render(live, refresh=key_handled)
                                time.sleep(0.05)
                            continue

                        self._submit_ready_tasks()
                        _render(live)
                        self._poll_slurm_statuses()
                        self._fire_task_hooks()
                        self._check_pipeline_completion()

                        if self._pipeline_done and self.EXIT_WHEN_DONE:
                            return

                        # Poll input at 50 ms intervals; redraw immediately on keypress
                        deadline = time.time() + self.REFRESHRATE
                        while time.time() < deadline:
                            key_handled = restore_terminal and check_for_keypress(self)
                            _render(live, refresh=key_handled)
                            time.sleep(0.05)

                    except KeyboardInterrupt:
                        print("\nStopping execution because of keyboard interrupt!")
                        self.exit_with_failed_tasks()

        finally:
            self._restore_terminal()

    # ── Output saving ──────────────────────────────────────────────────────────

    @staticmethod
    def make_save_hook(output_dir: Path) -> Callable[[TaskResult], None]:
        """Convenience alias for :func:`depio.hooks.make_save_hook`."""
        return _make_save_hook(output_dir)

    def save_stdouts(self, output_dir: Optional[Path] = None) -> Path:
        """Immediately save all terminal tasks' outputs to disk.

        Useful for a one-shot manual save.  For continuous per-task saving,
        use :meth:`make_save_hook` instead.

        Args:
            output_dir: Where to write.  Defaults to
                ``depio_output/<pipeline-name>/``.
        """
        if output_dir is None:
            safe_name = re.sub(r'[^\w\-]', '_', self.name).strip('_') or 'pipeline'
            output_dir = Path("depio_output") / safe_name
        hook = _make_save_hook(output_dir)
        for task in self.tasks:
            if task.is_in_terminal_state:
                hook(TaskResult(
                    name=task.name,
                    status=task.status[0],
                    stdout=task.get_stdout(),
                    stderr=task.get_stderr(),
                    duration=float(task.get_duration()),
                    outputs=list(task.products),
                ))
        return Path(output_dir)

    # ── Terminal / exit helpers ────────────────────────────────────────────────

    def _restore_terminal(self):
        if hasattr(self, '_old_terminal_settings') and self._old_terminal_settings is not None:
            try:
                import termios
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self._old_terminal_settings)
            except Exception:
                pass

    def exit_with_failed_tasks(self) -> None:
        if self._live is not None:
            self._live.stop()
        self._restore_terminal()

        print()
        for task in self.tasks:
            task.is_ready_for_execution()
        if not self.QUIET:
            Console().print(render_task_list(self))

        failed_tasks = [task for task in self.tasks if task.status[0] == TaskStatus.FAILED]
        if failed_tasks:
            print("---> Summary of Failed Tasks:")
            print()
            for task in failed_tasks:
                print(f"Details for Task ID: {task.id} - Name: {task.name}")
                print("STDOUT")
                print(task.get_stdout())
                print()
                print("STDERR")
                print(task.get_stderr())

        print("Canceling running jobs...")
        self.depioExecutor.cancel_all_jobs()
        print("Exit.")
        exit(1)

    def exit_successful(self) -> None:
        if self._live is not None:
            self._live.stop()
        self._restore_terminal()

        for task in self.tasks:
            task.is_ready_for_execution()
        if not self.QUIET:
            Console().print(render_task_list(self))

        print("All jobs done! Exit.")
        exit(0)


__all__ = ["Pipeline", "TaskResult", "PipelineResult"]
