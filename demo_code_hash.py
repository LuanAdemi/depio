"""
demo_code_hash.py — IF_CODE_CHANGED + upstream propagation
===========================================================

Run this script, then edit generate() below and run it again.

  generate  — IF_CODE_CHANGED: reruns when its code changes
  summarize — IF_MISSING:      reruns because generate ran  (propagation)
  report    — IF_MISSING:      reruns because summarize ran (propagation)
  metadata  — IF_MISSING:      independent — unaffected by changes to generate

Even though summarize and report use IF_MISSING, the universal upstream
invariant guarantees they rerun whenever their upstream task did.
metadata has no dependency on the generate chain, so it is never triggered
by changes there — it only runs once, when its output file is missing.
"""

import pathlib
import sys
from pathlib import Path
from typing import Annotated

from depio.BuildMode import BuildMode
from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency

BLD = pathlib.Path("build") / "code_hash_demo"
BLD.mkdir(parents=True, exist_ok=True)

executor = ParallelExecutor()
pipeline = Pipeline(depioExecutor=executor, name="Code Hash Demo")


# ── Tasks — try editing generate() and re-running the script ──────────────────

@task("generate", pipeline=pipeline, buildmode=BuildMode.IF_CODE_CHANGED)
def generate(output: Annotated[Path, Product]):
    # ✏  Edit this function (e.g. change the range or formula) and re-run
    lines = [f"{i}: {i ** 2}" for i in range(10)]
    output.write_text("\n".join(lines))


@task("summarize", pipeline=pipeline, buildmode=BuildMode.IF_MISSING)
def summarize(
    data:    Annotated[Path, Dependency],
    summary: Annotated[Path, Product],
):
    # Reruns automatically when generate reruns, even though it uses IF_MISSING
    values = [int(line.split(": ")[1]) for line in data.read_text().splitlines()]
    summary.write_text(f"count: {len(values)}\nsum:   {sum(values)}\n")


@task("report", pipeline=pipeline, buildmode=BuildMode.IF_MISSING)
def report(
    summary: Annotated[Path, Dependency],
    output:  Annotated[Path, Product],
):
    # Reruns automatically when summarize reruns
    output.write_text(f"=== Report ===\n{summary.read_text()}")


@task("metadata", pipeline=pipeline, buildmode=BuildMode.IF_MISSING)
def metadata(output: Annotated[Path, Product]):
    # No dependency on generate, summarize, or report —
    # runs once when the file is missing, never triggered by upstream changes.
    output.write_text(f"python: {sys.version}\nplatform: {sys.platform}\n")


# ── Register tasks ─────────────────────────────────────────────────────────────

generate(BLD / "data.txt")
summarize(BLD / "data.txt",    BLD / "summary.txt")
report(   BLD / "summary.txt", BLD / "report.txt")
metadata( BLD / "metadata.txt")

# ── Run ────────────────────────────────────────────────────────────────────────

pipeline.run()
