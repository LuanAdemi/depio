"""
demo_hydra.py — Hydra configuration integration with IgnoredForEq
==================================================================
Demonstrates:
  - Using Hydra to inject configuration (DictConfig) into task functions
  - Annotating the config argument as IgnoredForEq so that two tasks called
    with different configs but the same paths are still considered equal
    (enabling deduplication and caching across config sweeps)
  - A multi-stage pipeline where each stage reads the previous stage's output
  - ParallelExecutor for running independent branches concurrently

Usage:
  poetry run python demo_hydra.py              # uses config/config.yaml defaults
  poetry run python demo_hydra.py attack=zhang # override the attack sub-config

DAG (files in build/<bld_path>/)
----------------------------------
  [generate_input] ──► input.txt
                           │
                     [process_data]  ──► output_<attack.name>.txt
                           │
                     [evaluate]      ──► eval_<attack.name>.txt
                           │
                     [final_report]  ──► final_<attack.name>.txt
"""

from typing import Annotated
import pathlib
import time

from omegaconf import DictConfig, OmegaConf
import hydra

from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency, IgnoredForEq
from depio.BuildMode import BuildMode

SLURM = pathlib.Path("slurm")
SLURM.mkdir(exist_ok=True)

CONFIG = pathlib.Path("config")
CONFIG.mkdir(exist_ok=True)

depioExecutor = ParallelExecutor()
defaultpipeline = Pipeline(depioExecutor=depioExecutor, name="Hydra Demo", clear_screen=False)

# ── Task definitions ──────────────────────────────────────────────────────────
# cfg is annotated with IgnoredForEq so that Task equality is based only on
# the file paths, not on the config contents.  This means running the same
# pipeline twice with a different config sweep will reuse cached outputs
# whenever the output paths are identical.

@task("generate_input", buildmode=BuildMode.IF_MISSING)
def generate_input(
    output: Annotated[pathlib.Path, Product],
    cfg:    Annotated[DictConfig, IgnoredForEq],
):
    """Write synthetic input data, embedding the target label from the config."""
    lines = [f"sample_{i}: label={cfg.targetlabel}" for i in range(50)]
    output.write_text("\n".join(lines))
    print(f"  [generate_input] {len(lines)} samples → {output.name}")


@task("process_data", buildmode=BuildMode.IF_MISSING)
def process_data(
    src:    Annotated[pathlib.Path, Dependency],
    dst:    Annotated[pathlib.Path, Product],
    cfg:    Annotated[DictConfig, IgnoredForEq],
    delay:  int = 1,
):
    """Apply an attack-specific transformation to the input data."""
    time.sleep(delay)
    text = src.read_text()
    attack_name = cfg.attack.name
    dst.write_text(
        f"# Processed with attack: {attack_name}\n"
        f"# Config:\n{OmegaConf.to_yaml(cfg.attack)}\n"
        f"# Data:\n{text}\n"
    )
    print(f"  [process_data]  attack={attack_name} → {dst.name}")


@task("evaluate", buildmode=BuildMode.IF_MISSING)
def evaluate(
    processed:  Annotated[pathlib.Path, Dependency],
    result:     Annotated[pathlib.Path, Product],
    cfg:        Annotated[DictConfig, IgnoredForEq],
):
    """Simulate evaluation of the processed data against the target label."""
    time.sleep(0.5)
    lines = processed.read_text().splitlines()
    n_samples = sum(1 for l in lines if l.startswith("# Data:") or l.startswith("sample_"))
    score = n_samples * 0.01 * cfg.alpha   # dummy score
    result.write_text(
        f"attack: {cfg.attack.name}\n"
        f"target_label: {cfg.targetlabel}\n"
        f"alpha: {cfg.alpha}\n"
        f"score: {score:.4f}\n"
        f"n_samples: {n_samples}\n"
    )
    print(f"  [evaluate]      score={score:.4f} → {result.name}")


@task("final_report", buildmode=BuildMode.ALWAYS)
def final_report(
    eval_result:    Annotated[pathlib.Path, Dependency],
    report:         Annotated[pathlib.Path, Product],
    cfg:            Annotated[DictConfig, IgnoredForEq],
):
    """Produce a human-readable final report from the evaluation results."""
    content = eval_result.read_text()
    report.write_text(
        f"=== Final Report ===\n"
        f"Pipeline: {cfg.bld_path}\n\n"
        f"{content}\n"
    )
    print(f"  [final_report]  report → {report.name}")


# ── Hydra entry point ─────────────────────────────────────────────────────────

@hydra.main(version_base=None, config_path=str(CONFIG), config_name="config")
def main(cfg: DictConfig) -> None:
    BLD = pathlib.Path(cfg.bld_path)
    BLD.mkdir(parents=True, exist_ok=True)

    attack = cfg.attack.name

    # Stage 0: generate input (shared across attacks with the same target label)
    defaultpipeline.add_task(generate_input(BLD / "input.txt", cfg))

    # Stages 1–3: attack-specific chain
    defaultpipeline.add_task(process_data(
        BLD / "input.txt",
        BLD / f"output_{attack}.txt",
        cfg,
        delay=1,
    ))
    defaultpipeline.add_task(evaluate(
        BLD / f"output_{attack}.txt",
        BLD / f"eval_{attack}.txt",
        cfg,
    ))
    defaultpipeline.add_task(final_report(
        BLD / f"eval_{attack}.txt",
        BLD / f"final_{attack}.txt",
        cfg,
    ))


if __name__ == "__main__":
    main()
    defaultpipeline.run()
