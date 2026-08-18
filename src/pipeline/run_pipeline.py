"""
Local/orchestration helper for the ShopEase medallion pipeline.

This is a convenience entry point for running the bronze -> silver -> gold
stages in sequence outside of a Databricks Job (e.g. for local testing
against a small sample of data). In production the stages are orchestrated
by the shopease_job Databricks Job defined in resources/shopease_job.job.yml.
"""

import argparse
import importlib
import sys

STAGES = [
    ("bronze", "src.bronze.ingest_bronze"),
    ("silver", "src.silver.transform_silver"),
    ("gold", "src.gold.build_gold"),
]


def run(stages):
    for stage_name, module_path in STAGES:
        if stages and stage_name not in stages:
            continue
        print(f"--- Running {stage_name} stage ({module_path}) ---")
        module = importlib.import_module(module_path)
        module.main()


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Run the ShopEase medallion pipeline stages.")
    parser.add_argument(
        "--stage",
        action="append",
        dest="stages",
        choices=[name for name, _ in STAGES],
        help="Limit the run to a specific stage. Can be repeated. Defaults to all stages.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    run(args.stages or [])
