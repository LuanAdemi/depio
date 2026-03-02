"""
demo.py — decorator API with Product/Dependency annotations
===========================================================
Demonstrates:
  - @task decorator with Annotated[Path, Product] / Annotated[Path, Dependency]
  - Annotated[List[Path], Product] — a task that writes multiple output files
  - BuildMode.IF_MISSING (default) — tasks are skipped when outputs are fresh
  - BuildMode.ALWAYS — tasks always re-run regardless of existing outputs
  - Automatic DAG resolution: depio links file paths to the task that produces them
  - ParallelExecutor — independent branches run concurrently

DAG (files in build/decorator_demo/)
--------------------------------------
  [generate_input] ──► input.txt
        │
        ├──► [process_words]  ──► words.txt  ──┐
        │                                       ├──► [build_report] ──► report.txt
        ├──► [count_lines]    ──► stats.txt  ──┘
        │
        └──► [split_halves]   ──► [first_half.txt, second_half.txt]  (List[Path] product)
"""

from typing import Annotated, List
import pathlib
import time

from depio.BuildMode import BuildMode
from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency

# ── Output directory ──────────────────────────────────────────────────────────
BLD = pathlib.Path("build") / "decorator_demo"
BLD.mkdir(parents=True, exist_ok=True)

# ── Executor and pipeline ─────────────────────────────────────────────────────
executor = ParallelExecutor()
pipeline = Pipeline(depioExecutor=executor, name="Decorator Demo", clear_screen=False)

# ── Task definitions ──────────────────────────────────────────────────────────
# The @task decorator wraps a function so that calling it returns a Task object
# instead of running the function immediately. Pass pipeline= to auto-register.

@task("generate_input", buildmode=BuildMode.ALWAYS)
def generate_input(output: Annotated[pathlib.Path, Product]):
    """Create a synthetic text file used as input for all downstream tasks."""
    lines = [f"word_{i}: value={i * 0.42:.3f}" for i in range(40)]
    output.write_text("\n".join(lines))
    print(f"  [generate_input] wrote {len(lines)} lines → {output.name}")


@task("process_words", buildmode=BuildMode.IF_MISSING)
def process_words(
    src: Annotated[pathlib.Path, Dependency],
    dst: Annotated[pathlib.Path, Product],
):
    """Extract and sort the word tokens from the input."""
    time.sleep(0.15)
    words = sorted({line.split(":")[0] for line in src.read_text().splitlines() if line})
    dst.write_text("\n".join(words))
    print(f"  [process_words] {len(words)} unique words → {dst.name}")


@task("count_lines", buildmode=BuildMode.IF_MISSING)
def count_lines(
    src: Annotated[pathlib.Path, Dependency],
    dst: Annotated[pathlib.Path, Product],
):
    """Compute basic statistics (line count, non-empty lines) for the input."""
    time.sleep(0.1)
    lines = src.read_text().splitlines()
    non_empty = [l for l in lines if l.strip()]
    stats = f"total_lines: {len(lines)}\nnon_empty: {len(non_empty)}\n"
    dst.write_text(stats)
    print(f"  [count_lines] stats written → {dst.name}")


@task("split_halves", buildmode=BuildMode.IF_MISSING)
def split_halves(
    src: Annotated[pathlib.Path, Dependency],
    outputs: Annotated[List[pathlib.Path], Product],
):
    """Split the input into two halves and write each to a separate file.

    Using Annotated[List[Path], Product] registers *each* list element as a
    product, so depio can track them individually.
    """
    time.sleep(0.1)
    if not isinstance(outputs, list):
        outputs = [outputs]
    lines = src.read_text().splitlines()
    mid = len(lines) // 2
    halves = [lines[:mid], lines[mid:]]
    for out, half in zip(outputs, halves):
        out.write_text("\n".join(half))
        print(f"  [split_halves] {len(half)} lines → {out.name}")


@task("build_report", buildmode=BuildMode.ALWAYS)
def build_report(
    words_file: Annotated[pathlib.Path, Dependency],
    stats_file: Annotated[pathlib.Path, Dependency],
    report: Annotated[pathlib.Path, Product],
):
    """Merge word list and statistics into a single summary report."""
    time.sleep(0.05)
    report.write_text(
        "=== Pipeline Report ===\n\n"
        f"--- Word List ({words_file.name}) ---\n{words_file.read_text()}\n\n"
        f"--- Statistics ({stats_file.name}) ---\n{stats_file.read_text()}\n"
    )
    print(f"  [build_report] report written → {report.name}")


# ── Register tasks and wire up the DAG ───────────────────────────────────────
# depio resolves file paths to producing tasks automatically in _solve_order(),
# so the order of add_task() calls only needs to respect task-object dependencies.

pipeline.add_task(generate_input(BLD / "input.txt"))

pipeline.add_task(process_words(BLD / "input.txt", BLD / "words.txt"))
pipeline.add_task(count_lines(BLD / "input.txt",   BLD / "stats.txt"))
pipeline.add_task(split_halves(BLD / "input.txt",  [BLD / "first_half.txt", BLD / "second_half.txt"]))

pipeline.add_task(build_report(BLD / "words.txt", BLD / "stats.txt", BLD / "report.txt"))

# ── Run ───────────────────────────────────────────────────────────────────────
# pipeline.run() starts the execution loop and calls exit(0) on success or
# exit(1) if any task fails. It never returns normally.
pipeline.run()
