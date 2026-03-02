"""
demo_functional.py — functional (Task) API without the decorator
================================================================
Demonstrates:
  - Creating Task objects directly using the Task constructor
  - Explicit produces=[...] and depends_on=[...] keyword arguments
  - Task-object dependencies (depends_on=[task_object])
  - BuildMode.ALWAYS to force re-execution every run
  - ParallelExecutor — independent branches (analyse + visualise) run concurrently

DAG (files in build/functional_demo/)
---------------------------------------
  [generate] ──► raw.txt
                   │
                   ├──► [preprocess] ──► processed.txt
                                               │
                               ┌───────────────┤
                               │               │
                          [analyse]       [visualise]
                               │               │
                          analysis.txt     chart.txt
                               │               │
                               └──────┬────────┘
                                 [report] ──► report.txt

Note: analyse and visualise both depend on preprocess — they run in parallel
      once preprocess finishes.
"""

import time
from pathlib import Path

from depio.BuildMode import BuildMode
from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline
from depio.Task import Task

# ── Output directory ──────────────────────────────────────────────────────────
BLD = Path("build") / "functional_demo"
BLD.mkdir(parents=True, exist_ok=True)

# ── Task functions ────────────────────────────────────────────────────────────
# Plain functions — no annotations required. Products and dependencies are
# declared explicitly when constructing the Task objects below.

def generate_data(output: Path, n_records: int = 30):
    """Write synthetic CSV-like data to output."""
    header = "id,category,value"
    rows = [f"{i},{['A','B','C'][i%3]},{i*1.7:.2f}" for i in range(n_records)]
    output.write_text("\n".join([header] + rows))
    print(f"  [generate]    wrote {n_records} records → {output.name}")


def preprocess(src: Path, dst: Path):
    """Validate and normalise the raw data (drop header, sort by value)."""
    time.sleep(0.15)
    lines = src.read_text().splitlines()
    rows = [l.split(",") for l in lines[1:] if l.strip()]  # skip header
    rows.sort(key=lambda r: float(r[2]), reverse=True)
    dst.write_text("\n".join(",".join(r) for r in rows))
    print(f"  [preprocess]  {len(rows)} rows sorted → {dst.name}")


def analyse(src: Path, dst: Path):
    """Compute per-category totals and overall mean from preprocessed data."""
    time.sleep(0.2)
    rows = [l.split(",") for l in src.read_text().splitlines() if l.strip()]
    totals: dict = {}
    for _id, cat, val in rows:
        totals[cat] = totals.get(cat, 0.0) + float(val)
    values = [float(r[2]) for r in rows]
    mean = sum(values) / max(len(values), 1)
    lines = [f"{cat}: {total:.2f}" for cat, total in sorted(totals.items())]
    lines.append(f"mean: {mean:.2f}")
    lines.append(f"records: {len(rows)}")
    dst.write_text("\n".join(lines))
    print(f"  [analyse]     {len(totals)} categories → {dst.name}")


def visualise(src: Path, dst: Path):
    """Build an ASCII bar chart of the top-10 values."""
    time.sleep(0.2)
    rows = [l.split(",") for l in src.read_text().splitlines() if l.strip()][:10]
    max_val = max((float(r[2]) for r in rows), default=1.0)
    chart = ["=== Top-10 values ==="]
    for _id, cat, val in rows:
        bar_len = int(float(val) / max_val * 30)
        chart.append(f"{cat} {float(val):6.2f} | {'█' * bar_len}")
    dst.write_text("\n".join(chart))
    print(f"  [visualise]   chart written → {dst.name}")


def build_report(analysis: Path, chart: Path, report: Path):
    """Merge analysis and chart into a final Markdown-style report."""
    time.sleep(0.05)
    report.write_text(
        "# Pipeline Report\n\n"
        f"## Analysis\n\n```\n{analysis.read_text()}\n```\n\n"
        f"## Visualisation\n\n```\n{chart.read_text()}\n```\n"
    )
    print(f"  [report]      report written → {report.name}")


# ── Build the pipeline ────────────────────────────────────────────────────────
executor = ParallelExecutor()
pipeline = Pipeline(depioExecutor=executor, name="Functional Demo", clear_screen=False)

# Each Task is registered with explicit produces= / depends_on= lists.
# Task-object dependencies (depends_on=[t_gen]) tell depio about execution
# ordering when the file relationship isn't expressed via annotations.

t_gen = pipeline.add_task(Task(
    "generate",
    generate_data,
    func_args=[BLD / "raw.txt"],
    produces=[BLD / "raw.txt"],
    buildmode=BuildMode.ALWAYS,
))

t_pre = pipeline.add_task(Task(
    "preprocess",
    preprocess,
    func_args=[BLD / "raw.txt", BLD / "processed.txt"],
    produces=[BLD / "processed.txt"],
    depends_on=[t_gen],
    buildmode=BuildMode.ALWAYS,
))

# analyse and visualise both depend on t_pre → they run in parallel
t_ana = pipeline.add_task(Task(
    "analyse",
    analyse,
    func_args=[BLD / "processed.txt", BLD / "analysis.txt"],
    produces=[BLD / "analysis.txt"],
    depends_on=[t_pre],
    buildmode=BuildMode.ALWAYS,
))

t_vis = pipeline.add_task(Task(
    "visualise",
    visualise,
    func_args=[BLD / "processed.txt", BLD / "chart.txt"],
    produces=[BLD / "chart.txt"],
    depends_on=[t_pre],
    buildmode=BuildMode.ALWAYS,
))

# report waits for both t_ana and t_vis to finish
pipeline.add_task(Task(
    "report",
    build_report,
    func_args=[BLD / "analysis.txt", BLD / "chart.txt", BLD / "report.txt"],
    produces=[BLD / "report.txt"],
    depends_on=[t_ana, t_vis],
    buildmode=BuildMode.ALWAYS,
))

# ── Run ───────────────────────────────────────────────────────────────────────
pipeline.run()
