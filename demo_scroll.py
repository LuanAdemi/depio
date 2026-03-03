"""
demo_scroll.py — Large pipeline showcasing the scrollable TUI
=============================================================
Creates 44 tasks across 4 stages to demonstrate:
  - Auto-scrolling viewport: table height adapts to your terminal
  - Arrow-key navigation: ↑↓ moves the selection; the viewport follows
  - ▲/▼ indicators show how many rows are hidden above/below
  - Enter to inspect any task's stdout   Esc to return to the list

Pipeline structure (44 tasks total)
─────────────────────────────────────────────────────────────────
  [setup]
     │
     ├── [process_shard_00] ──┐
     ├── [process_shard_01]   │ 6 shards per group
     │    …                   ├──► [aggregate_group_0] ──┐
     ├── [process_shard_05] ──┘                          │
     │                                                   │
     ├── [process_shard_06] ──┐                          │
     │    …                   ├──► [aggregate_group_1] ──┤
     ├── [process_shard_11] ──┘                          │  …6 groups…
     │    …                                              │
     └── [process_shard_35] ──► [aggregate_group_5] ───►┤
                                                         │
                                                 [compile_report]
─────────────────────────────────────────────────────────────────
"""

import random
import time
from pathlib import Path

from depio.BuildMode import BuildMode
from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline
from depio.Task import Task

N_SHARDS         = 36
N_GROUPS         = 6
SHARDS_PER_GROUP = N_SHARDS // N_GROUPS   # 6

BLD = Path("build") / "scroll_demo"
BLD.mkdir(parents=True, exist_ok=True)


# ── Stage 1: setup ────────────────────────────────────────────────────────────

def run_setup(out: Path) -> None:
    time.sleep(0.3)
    out.write_text("\n".join(f"shard_{i:02d}" for i in range(N_SHARDS)))
    print(f"Registered {N_SHARDS} shards.")
    print(f"Manifest written to {out.name}")


# ── Stage 2: process shards (run in parallel) ─────────────────────────────────

def make_shard_fn(i: int):
    def run(out: Path) -> None:
        rng = random.Random(i)
        delay = rng.uniform(0.4, 1.8)
        time.sleep(delay)
        n = rng.randint(8, 20)
        records = [f"record_{j:02d}: score={rng.random():.4f}" for j in range(n)]
        out.write_text("\n".join(records))
        print(f"Shard {i:02d} finished in {delay:.2f}s  ({n} records)")
        print(f"Output: {out.name}")
        for r in records:
            print(f"  {r}")
    return run


# ── Stage 3: aggregate groups ─────────────────────────────────────────────────

def make_aggregate_fn(group: int, shard_paths: list):
    def run(out: Path) -> None:
        time.sleep(0.25)
        lines = []
        for p in shard_paths:
            lines += p.read_text().splitlines()
        scores = [float(ln.split("=")[1]) for ln in lines if "=" in ln]
        mean   = sum(scores) / max(len(scores), 1)
        hi, lo = max(scores), min(scores)
        out.write_text(
            f"group={group}\n"
            f"shards={len(shard_paths)}\n"
            f"records={len(lines)}\n"
            f"mean={mean:.4f}\n"
            f"max={hi:.4f}\n"
            f"min={lo:.4f}\n"
        )
        print(f"Group {group}: {len(lines)} records  mean={mean:.4f}  max={hi:.4f}  min={lo:.4f}")
    return run


# ── Stage 4: compile report ───────────────────────────────────────────────────

def run_report(out: Path) -> None:
    time.sleep(0.2)
    sections = []
    for g in range(N_GROUPS):
        sections.append((BLD / f"group_{g}.txt").read_text())
    out.write_text("=== Final Report ===\n\n" + "\n---\n".join(sections))
    print(f"Report compiled from {N_GROUPS} groups.")
    print(f"Output: {out.name}")


# ── Assemble pipeline ─────────────────────────────────────────────────────────

executor = ParallelExecutor()
pipeline = Pipeline(
    depioExecutor=executor,
    name="Scroll Demo  (↑↓ navigate · Enter detail · Esc back)",
    clear_screen=False,
)

# Stage 1
t_setup = pipeline.add_task(Task(
    "setup",
    run_setup,
    func_args=[BLD / "manifest.txt"],
    produces=[BLD / "manifest.txt"],
    buildmode=BuildMode.ALWAYS,
))

# Stage 2 — one task per shard
shard_tasks = []
for i in range(N_SHARDS):
    t = pipeline.add_task(Task(
        f"process_shard_{i:02d}",
        make_shard_fn(i),
        func_args=[BLD / f"shard_{i:02d}.txt"],
        produces=[BLD / f"shard_{i:02d}.txt"],
        depends_on=[t_setup],
        buildmode=BuildMode.ALWAYS,
    ))
    shard_tasks.append(t)

# Stage 3 — one aggregate per group of 6 shards
group_tasks = []
for g in range(N_GROUPS):
    shard_paths = [BLD / f"shard_{(g * SHARDS_PER_GROUP + j):02d}.txt"
                   for j in range(SHARDS_PER_GROUP)]
    t = pipeline.add_task(Task(
        f"aggregate_group_{g}",
        make_aggregate_fn(g, shard_paths),
        func_args=[BLD / f"group_{g}.txt"],
        produces=[BLD / f"group_{g}.txt"],
        depends_on=shard_paths,
        buildmode=BuildMode.ALWAYS,
    ))
    group_tasks.append(t)

# Stage 4
pipeline.add_task(Task(
    "compile_report",
    run_report,
    func_args=[BLD / "report.txt"],
    produces=[BLD / "report.txt"],
    depends_on=[BLD / f"group_{g}.txt" for g in range(N_GROUPS)],
    buildmode=BuildMode.ALWAYS,
))

# ── Run ───────────────────────────────────────────────────────────────────────
pipeline.run()
