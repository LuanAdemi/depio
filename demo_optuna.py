"""demo_optuna.py — Optuna + depio integration

Demonstrates three optimisation strategies, each paired with the right
batch_size and executor:

  1. Random search  — all trials in one parallel burst (batch_size = n_trials)
  2. TPE            — small parallel batches so the sampler can adapt frequently
  3. CMA-ES         — batch_size = popsize so each depio run = one full generation

Run:
    poetry run python demo_optuna.py
"""

import math
from time import sleep
import optuna

optuna.logging.set_verbosity(optuna.logging.WARNING)

from depio.Executors import ParallelExecutor
from depio.optuna_integration import run_optuna_study


# ── Objective functions ───────────────────────────────────────────────────────

def sphere(trial: optuna.Trial) -> float:
    """Sphere — minimum 0 at the origin."""
    xs = [trial.suggest_float(f"x{i}", -5.0, 5.0) for i in range(6)]
    sleep(1)  # simulate expensive evaluation
    return sum(x ** 2 for x in xs)


def rosenbrock(trial: optuna.Trial) -> float:
    """Rosenbrock — minimum 0 at (1, 1, 1, 1)."""
    xs = [trial.suggest_float(f"x{i}", -5.0, 5.0) for i in range(4)]
    sleep(1.5)  # simulate more expensive evaluation
    return sum(
        100 * (xs[i + 1] - xs[i] ** 2) ** 2 + (1 - xs[i]) ** 2
        for i in range(len(xs) - 1)
    )


# ── 1. Random search ──────────────────────────────────────────────────────────
# Random sampling has no dependence between trials, so all 40 can run at once.
# batch_size defaults to n_trials, meaning a single parallel burst.

study_random = optuna.create_study(
    sampler=optuna.samplers.RandomSampler(seed=0),
    direction="minimize",
)
run_optuna_study(
    study_random,
    sphere,
    n_trials=40,
    executor=ParallelExecutor(),
    # batch_size omitted → all 40 trials submit at once
)

# ── 2. TPE ────────────────────────────────────────────────────────────────────
# TPE is an adaptive sampler: each batch benefits from seeing the results of
# previous batches.  batch_size=1 gives maximum adaptation but no parallelism.
# batch_size=5 is a good trade-off: 5-way parallelism per batch, 6 batches.
#
# Note: TPE needs ~n_startup_trials (default 10) random trials before the
# probabilistic model kicks in.  Those first 10 trials will look random.

study_tpe = optuna.create_study(
    sampler=optuna.samplers.TPESampler(seed=0, n_startup_trials=10),
    direction="minimize",
)
run_optuna_study(
    study_tpe,
    rosenbrock,
    n_trials=30,
    executor=ParallelExecutor(),
    batch_size=5,
)


# ── 3. CMA-ES (evolution strategy) ───────────────────────────────────────────
# CMA-ES is a population-based algorithm:
#
#   for each generation:
#     ask()  × popsize  — sample a population from a multivariate Gaussian
#     tell() × popsize  — evaluate; update the covariance matrix
#
# Setting batch_size = popsize maps one depio Pipeline to one generation:
#   - All popsize trials in the batch run in parallel inside the pipeline.
#   - pipeline.run() returns only after every study.tell() has been called.
#   - The next study.ask() calls therefore see the full updated distribution.
#
# Default CMA-ES popsize for n_params parameters:
#   popsize = 4 + floor(3 * log(n_params))
# For 4 parameters: 4 + floor(3 * ln(4)) ≈ 8

N_PARAMS = 4
POPSIZE  = 4 + math.floor(3 * math.log(N_PARAMS))   # = 8
N_GEN    = 6                                          # 6 generations

study_cmaes = optuna.create_study(
    sampler=optuna.samplers.CmaEsSampler(seed=0, popsize=POPSIZE),
    direction="minimize",
)
run_optuna_study(
    study_cmaes,
    rosenbrock,
    n_trials=N_GEN * POPSIZE,
    executor=ParallelExecutor(),
    batch_size=POPSIZE,   # ← one generation per pipeline run
)


# ── 4. CMA-ES on SLURM (illustrative — requires a cluster) ───────────────────
# Swap the executor for SubmitItExecutor to run each generation on the cluster.
# The batch_size logic is identical; depio handles the SLURM job submission.
#
#   from depio.Executors import SubmitItExecutor
#
#   slurm = SubmitItExecutor(
#       folder="slurm_logs/",
#       parameters={"slurm_time": 30, "slurm_partition": "gpu"},
#   )
#   run_optuna_study(
#       study_cmaes, rosenbrock,
#       n_trials=N_GEN * POPSIZE,
#       executor=slurm,
#       batch_size=POPSIZE,
#   )


# ── Summary ───────────────────────────────────────────────────────────────────

print("=" * 60)
print("Summary")
print("=" * 60)
print(f"  Random  (sphere,    40 trials)   best = {study_random.best_value:.6f}")
print(f"  TPE     (rosenbrock, 30 trials)  best = {study_tpe.best_value:.6f}")
print(f"  CMA-ES  (rosenbrock, {N_GEN * POPSIZE} trials)  best = {study_cmaes.best_value:.6f}")
print()
print("batch_size guidance:")
print("  Random / Grid  → batch_size = n_trials   (one big parallel burst)")
print("  TPE / GP-BO    → batch_size = n_workers  (balance adapt vs parallelism)")
print(f"  CMA-ES / NSGA  → batch_size = popsize    (one pipeline = one generation)")
