"""
demo_slurm.py — SLURM cluster execution via SubmitItExecutor
=============================================================
Demonstrates:
  - SubmitItExecutor for submitting tasks as SLURM batch jobs via submitit
  - Per-pipeline default SLURM parameters (partition, memory, time limit)
  - Per-task SLURM parameter overrides via Task(slurm_parameters={...})
  - Automatic afterok dependency injection between chained SLURM jobs
  - BuildMode.IF_MISSING — tasks already on disk are skipped without resubmission

Requires a machine with a working SLURM installation.  Adjust the partition
name, memory, and time limits for your cluster before running.

DAG (files in build/slurm_demo/)
----------------------------------
  [create_input]
        │
        ├──► input.txt ──► [stage_a] ──► output_a.txt ──► [merge] ──► final.txt
        │
        └──► input.txt ──► [stage_b] ──► output_b.txt ──► [merge]
"""

from typing import Annotated
import pathlib
import time

from depio.Executors import SubmitItExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency
from depio.BuildMode import BuildMode

# ── Output directories ────────────────────────────────────────────────────────
BLD  = pathlib.Path("build") / "slurm_demo"
LOGS = pathlib.Path("slurm") / "slurm_demo"
BLD.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

# ── SLURM configuration ───────────────────────────────────────────────────────
# These parameters are passed to submitit and forwarded to sbatch.
# See https://slurm.schedmd.com/sbatch.html for all available options.
DEFAULT_SLURM_PARAMS = {
    "slurm_partition": "cpu",       # partition / queue name
    "slurm_mem":       "4GB",       # memory per job
    "slurm_time":      "00:30:00",  # wall-clock time limit (HH:MM:SS)
    "gpus_per_node":   0,
}

# Resource-intensive tasks can override defaults on a per-task basis.
HEAVY_SLURM_PARAMS = {
    **DEFAULT_SLURM_PARAMS,
    "slurm_mem":  "16GB",
    "slurm_time": "02:00:00",
}

executor = SubmitItExecutor(
    folder=LOGS,
    parameters=DEFAULT_SLURM_PARAMS,
    # max_jobs_queued limits how many jobs sit in the SLURM queue at once.
    # max_jobs_pending limits how many are in PENDING state (throttles submission).
    max_jobs_queued=50,
    max_jobs_pending=20,
)

pipeline = Pipeline(
    depioExecutor=executor,
    name="SLURM Demo",
    clear_screen=True,
    submit_only_if_runnable=True,   # only submit when dependencies are met
)

# ── Task definitions ──────────────────────────────────────────────────────────

@task("create_input", buildmode=BuildMode.IF_MISSING)
def create_input(output: Annotated[pathlib.Path, Product]):
    """Generate a synthetic input file on the compute node."""
    lines = [f"sample_{i}: {i ** 2}" for i in range(100)]
    output.write_text("\n".join(lines))
    print(f"Input created: {output}")


@task("stage_a", buildmode=BuildMode.IF_MISSING)
def stage_a(
    src: Annotated[pathlib.Path, Dependency],
    dst: Annotated[pathlib.Path, Product],
    delay: int = 5,
):
    """First processing stage — simulates a short compute task."""
    time.sleep(delay)
    text = src.read_text()
    dst.write_text(f"# stage_a output\n{text.upper()}")
    print(f"stage_a: {src.name} → {dst.name}")


@task("stage_b", buildmode=BuildMode.IF_MISSING)
def stage_b(
    src: Annotated[pathlib.Path, Dependency],
    dst: Annotated[pathlib.Path, Product],
    delay: int = 8,
):
    """Second processing stage — simulates a longer compute task."""
    time.sleep(delay)
    lines = src.read_text().splitlines()
    reversed_lines = lines[::-1]
    dst.write_text(f"# stage_b output\n" + "\n".join(reversed_lines))
    print(f"stage_b: {src.name} → {dst.name}")


@task("merge", buildmode=BuildMode.IF_MISSING)
def merge(
    part_a: Annotated[pathlib.Path, Dependency],
    part_b: Annotated[pathlib.Path, Dependency],
    output: Annotated[pathlib.Path, Product],
):
    """Merge stage_a and stage_b outputs into a final file."""
    output.write_text(
        f"=== Merged Output ===\n\n"
        f"--- {part_a.name} ---\n{part_a.read_text()}\n\n"
        f"--- {part_b.name} ---\n{part_b.read_text()}\n"
    )
    print(f"merged → {output.name}")


# ── Wire up the DAG ───────────────────────────────────────────────────────────
pipeline.add_task(create_input(BLD / "input.txt"))

# stage_a and stage_b both consume input.txt — depio detects the shared
# producer (create_input) and adds afterok: dependencies automatically.
pipeline.add_task(stage_a(BLD / "input.txt", BLD / "output_a.txt", delay=5))

# Override SLURM parameters for this heavier task.
t_b = pipeline.add_task(
    stage_b(BLD / "input.txt", BLD / "output_b.txt", delay=10)
)
t_b.slurm_parameters = HEAVY_SLURM_PARAMS

pipeline.add_task(merge(BLD / "output_a.txt", BLD / "output_b.txt", BLD / "final.txt"))

# ── Run ───────────────────────────────────────────────────────────────────────
pipeline.run()
