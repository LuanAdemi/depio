"""
demo_hook.py — global and per-task on_finished hooks
=====================================================
Demonstrates:
  - Global hook via Pipeline(on_task_finished=...)  — fires for every task
  - Per-task hook via Task(on_finished=...)          — fires only for that task
  - Pipeline.make_save_hook(...)  — auto-save stdout/stderr for all tasks
  - Desktop notification via notify-send (Fedora / Ubuntu) on the final task only

After the pipeline finishes, inspect the saved files:

    ls -R build/hook_demo/outputs/

Requirements (notification support):
    Ubuntu:  sudo apt install libnotify-bin
    Fedora:  sudo dnf install libnotify
"""

import subprocess
import time
from pathlib import Path

from depio.BuildMode import BuildMode
from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline, TaskResult
from depio.Task import Task

BLD     = Path("build") / "hook_demo"
OUTPUTS = BLD / "outputs"
BLD.mkdir(parents=True, exist_ok=True)


# ── Hook: desktop notification via notify-send ────────────────────────────────

def notify_hook(result: TaskResult) -> None:
    """Send a desktop notification for each finished task.

    Uses notify-send (libnotify). Silently skips if not installed.
    """
    success = result.status.name in ("FINISHED", "SKIPPED")
    summary = f"{'✓' if success else '✗'} {result.name}"
    body    = f"{result.status.name}  ({result.duration:.1f}s)"
    if not success and result.stderr:
        first_line = result.stderr.strip().splitlines()[0][:80]
        body += f"\n{first_line}"

    try:
        subprocess.run(
            [
                "notify-send",
                "--app-name", "depio",
                "--icon",     "emblem-ok-symbolic" if success else "dialog-error",
                "--urgency",  "normal" if success else "critical",
                "--expire-time", "4000",
                summary, body,
            ],
            check=False,
            capture_output=True,
        )
    except FileNotFoundError:
        pass  # notify-send not installed — skip silently


# ── Global hook: save stdout/stderr for every task ───────────────────────────

_save_hook = Pipeline.make_save_hook(OUTPUTS)


# ── Task functions ─────────────────────────────────────────────────────────────

def run_prepare(out: Path) -> None:
    time.sleep(0.2)
    out.write_text("dataset ready\n")
    print("Dataset prepared.")
    print(f"Written to: {out.name}")


def run_train(src: Path, out: Path, model_id: int) -> None:
    n_records = len(src.read_text().splitlines())
    time.sleep(0.3 + model_id * 0.1)
    loss = round(1.0 / (model_id + 1), 4)
    out.write_text(f"model_id={model_id}\nloss={loss}\n")
    print(f"Model {model_id} trained on {n_records} records.  loss={loss}")
    print(f"Checkpoint: {out.name}")


def run_evaluate(checkpoint: Path, out: Path, model_id: int) -> None:
    time.sleep(0.15)
    data = dict(line.split("=") for line in checkpoint.read_text().splitlines() if "=" in line)
    acc = round(1.0 - float(data["loss"]) * 0.8, 4)
    out.write_text(f"model_id={model_id}\naccuracy={acc}\n")
    print(f"Model {model_id} evaluated.  accuracy={acc}")


def run_report(out: Path) -> None:
    time.sleep(0.1)
    evals = sorted(BLD.glob("eval_*.txt"))
    lines = ["=== Evaluation Report ===", ""]
    for p in evals:
        lines.append(f"[{p.stem}]")
        lines.extend(f"  {l}" for l in p.read_text().splitlines())
        lines.append("")
    out.write_text("\n".join(lines))
    print("Report compiled.")
    for line in lines:
        print(f"  {line}")


# ── Assemble pipeline ──────────────────────────────────────────────────────────

executor = ParallelExecutor()
pipeline = Pipeline(
    depioExecutor=executor,
    name="Hook Demo",
    on_task_finished=_save_hook,   # save outputs for every task
)

t_prep = pipeline.add_task(Task(
    "prepare_dataset",
    run_prepare,
    func_args=[BLD / "dataset.txt"],
    produces=[BLD / "dataset.txt"],
    buildmode=BuildMode.ALWAYS,
))

N_MODELS = 4
eval_tasks = []
for i in range(N_MODELS):
    ckpt = BLD / f"model_{i}.ckpt"
    t_train = pipeline.add_task(Task(
        f"train_model_{i}",
        run_train,
        func_args=[BLD / "dataset.txt", ckpt, i],
        produces=[ckpt],
        depends_on=[t_prep],
        buildmode=BuildMode.ALWAYS,
    ))

    eval_out = BLD / f"eval_{i}.txt"
    t_eval = pipeline.add_task(Task(
        f"evaluate_model_{i}",
        run_evaluate,
        func_args=[ckpt, eval_out, i],
        produces=[eval_out],
        depends_on=[t_train],
        buildmode=BuildMode.ALWAYS,
    ))
    eval_tasks.append(t_eval)

pipeline.add_task(Task(
    "compile_report",
    run_report,
    func_args=[BLD / "report.txt"],
    produces=[BLD / "report.txt"],
    depends_on=[BLD / f"eval_{i}.txt" for i in range(N_MODELS)],
    buildmode=BuildMode.ALWAYS,
    on_finished=notify_hook,   # desktop notification only for the final task
))

# ── Run ────────────────────────────────────────────────────────────────────────
print(f"\nOutputs will be saved to: {OUTPUTS}/\n")
pipeline.run()
