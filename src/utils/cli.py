import argparse


def ingest_parse_arguments():
    parser = argparse.ArgumentParser(description="Ingest data into DuckDB.")
    parser.add_argument(
        "--db",
        type=str,
        default="dev",
        help="Specify the database environment ('dev' or 'prod' default: dev).",
    )
    return parser.parse_args()
