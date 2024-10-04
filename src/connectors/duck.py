"""DuckDB connector module."""

import json

import duckdb
import pandas as pd
from loguru import logger

from src.utils.file.read import read_yaml_to_dict


def get_db_file(db: str, dbt_profiles_path: str = "profiles.yml") -> str:
    """
    Get the DuckDB file path based on the environment.

    Args:
        db (str): The database environment ('dev' or 'prod').
        dbt_profiles_path (str): The path to the DBT profiles file.

    Returns:
        str: The path to the DuckDB file.
    """
    try:
        logger.info(f"Fetching {db} file value from {dbt_profiles_path}")
        db_file = read_yaml_to_dict(dbt_profiles_path)["main"]["outputs"][db]["path"]
        logger.info(f"Using {db} environment with database file: {db_file}")
        return db_file
    except KeyError:
        logger.error(f"Unknown database environment: {db}")
        raise ValueError(f"Unsupported database environment: {db}")


def connect_duckdb(db_file: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """
    Returns a connection object to the DuckDB database.

    Args:
        db_file (str): The file path to the DuckDB database. Defaults to in-memory (":memory:").

    Returns:
        duckdb.DuckDBPyConnection: The connection object.
    """
    logger.info(f"Connecting to DuckDB database in {db_file}")
    return duckdb.connect(db_file)


def create_table(
    con: duckdb.DuckDBPyConnection,
    data: pd.DataFrame | dict,
    table_name: str,
) -> None:
    """
    Creates a table from a Pandas DataFrame or a dictionary in DuckDB.

    Args:
        con (duckdb.DuckDBPyConnection): Connection object to the DuckDB database.
        data (pd.DataFrame or dict): The data to be stored in the table.
        table_name (str): The name of the table to be created.

    Returns:
        None
    """

    if isinstance(data, pd.DataFrame):
        logger.info(f"Creating table '{table_name}' from DataFrame.")
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM data")
    elif isinstance(data, dict):
        logger.info(f"Creating table '{table_name}' from dictionary.")
        con.execute(f"CREATE OR REPLACE TABLE {table_name} (data JSON)")
        con.execute(f"INSERT INTO {table_name} VALUES ('{json.dumps(data)}')")
    else:
        logger.error("Unsupported data type. Only pd.DataFrame and dict are supported.")
        raise TypeError(
            "Unsupported data type. Only pd.DataFrame and dict are supported."
        )
    logger.success(f"Table '{table_name}' created successfully.")


def select_table_to_dataframe(
    con: duckdb.DuckDBPyConnection, table_name: str
) -> pd.DataFrame:
    """
    Selects a table or view from the DuckDB database and returns it as a Pandas DataFrame.

    Args:
        table_name (str): The name of the table or view to be selected.
        db_file (str): The file path to the DuckDB database. Defaults to in-memory (":memory:").

    Returns:
        pd.DataFrame: The selected table or view as a Pandas DataFrame.
    """
    query = f"SELECT * FROM {table_name}"
    df = con.execute(query).fetch_df()
    logger.success(f"Table/view '{table_name}' selected successfully.")
    return df
