import argparse


def get_runtime_config():
    """Read environment-specific parameters passed by the Databricks job."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog used by the deployment target",
    )

    parser.add_argument(
        "--environment",
        required=True,
        help="Deployment environment such as dev or staging",
    )

    args, _ = parser.parse_known_args()

    return args.catalog, args.environment