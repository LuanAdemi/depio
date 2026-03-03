"""
demo_hydra.py — Hydra + depio integration
==========================================
Demonstrates run_hydra_multirun(), which runs a depio pipeline across
multiple Hydra config variants inside a single Pipeline execution.

Each task is tagged with its config variant ("attack=zhang", etc.) and
the label appears in the TUI's Variant column.

Shared tasks (same output path across variants, annotated with IgnoredForEq)
are deduplicated automatically — they appear once with no Variant label.

Usage:
    poetry run python demo_hydra.py

DAG (files in build/<attack.name>/)
-------------------------------------
                    [generate_input]  ──► input.txt       ← shared across variants
                          │
             ┌────────────┼────────────┐
             ▼            ▼            ▼
     [process_data]  [process_data]  [process_data]       ← one per attack
             │            │            │
         [evaluate]  [evaluate]    [evaluate]
             │            │            │
      [final_report] [final_report] [final_report]
"""

from typing import Annotated
import pathlib
import time

from omegaconf import DictConfig, OmegaConf

from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency, IgnoredForEq
from depio.BuildMode import BuildMode
from depio.integrations.hydra import run_hydra_multirun

BLD_ROOT = pathlib.Path("build")
BLD_ROOT.mkdir(exist_ok=True)

CONFIG = pathlib.Path("config")


# ── Task definitions ──────────────────────────────────────────────────────────
# cfg is annotated IgnoredForEq so that tasks whose file paths match are
# considered equal regardless of config contents — enabling deduplication
# of shared stages (e.g. generate_input) across variants.

@task("generate_input", buildmode=BuildMode.ALWAYS)
def generate_input(
    output: Annotated[pathlib.Path, Product],
    cfg:    Annotated[DictConfig, IgnoredForEq],
):
    """Write synthetic input data using the target label from config."""
    lines = [f"sample_{i}: label={cfg.targetlabel}" for i in range(50)]
    output.write_text("\n".join(lines))
    print(f"  [generate_input] {len(lines)} samples → {output.name}")


@task("process_data", buildmode=BuildMode.ALWAYS)
def process_data(
    src:    Annotated[pathlib.Path, Dependency],
    dst:    Annotated[pathlib.Path, Product],
    cfg:    Annotated[DictConfig, IgnoredForEq],
    delay:  int = 1,
):
    """Apply attack-specific processing."""
    time.sleep(delay)
    attack_name = cfg.attack.name
    dst.write_text(
        f"# Processed with attack: {attack_name}\n"
        f"# Config:\n{OmegaConf.to_yaml(cfg.attack)}\n"
        f"# Data:\n{src.read_text()}\n"
    )
    print(f"  [process_data]  attack={attack_name} → {dst.name}")


@task("evaluate", buildmode=BuildMode.ALWAYS)
def evaluate(
    processed:  Annotated[pathlib.Path, Dependency],
    result:     Annotated[pathlib.Path, Product],
    cfg:        Annotated[DictConfig, IgnoredForEq],
):
    """Score processed data against the target label."""
    time.sleep(0.5)
    lines = processed.read_text().splitlines()
    n_samples = sum(1 for ln in lines if ln.startswith("sample_"))
    score = n_samples * 0.01 * cfg.alpha
    result.write_text(
        f"attack: {cfg.attack.name}\n"
        f"score:  {score:.4f}\n"
    )
    print(f"  [evaluate]      score={score:.4f} → {result.name}")


@task("final_report", buildmode=BuildMode.ALWAYS)
def final_report(
    eval_result:    Annotated[pathlib.Path, Dependency],
    report:         Annotated[pathlib.Path, Product],
    cfg:            Annotated[DictConfig, IgnoredForEq],
):
    """Produce final report from evaluation."""
    report.write_text(
        f"=== Final Report ===\n"
        f"Pipeline: {cfg.bld_path}\n\n"
        f"{eval_result.read_text()}\n"
    )
    print(f"  [final_report]  → {report.name}")


# ── Pipeline builder (called once per config variant) ─────────────────────────

def build_pipeline(cfg: DictConfig, pipeline: Pipeline) -> None:
    attack = cfg.attack.name
    BLD = pathlib.Path(cfg.bld_path) / attack
    BLD.mkdir(parents=True, exist_ok=True)

    # generate_input uses the root build dir and is shared across variants
    # (same output path → deduplicated by depio when IgnoredForEq is used).
    shared_input = BLD_ROOT / "input.txt"

    pipeline.add_task(generate_input(shared_input, cfg))
    pipeline.add_task(process_data(shared_input, BLD / "output.txt", cfg, delay=1))
    pipeline.add_task(evaluate(BLD / "output.txt", BLD / "eval.txt", cfg))
    pipeline.add_task(final_report(BLD / "eval.txt", BLD / "report.txt", cfg))


# ── Run multirun across all attack variants ───────────────────────────────────

run_hydra_multirun(
    build_pipeline,
    overrides_list=[
        ["attack=zhang"],
        ["attack=dombrowski"],
        ["attack=ours"],
    ],
    executor=ParallelExecutor(),
    config_path=str(CONFIG),
    config_name="config",
    pipeline_name="Hydra Multirun Demo",
)
