"""Common DataFrame read functions."""

from io import BytesIO
from typing import Any

import pandas as pd
from loguru import logger

from src.utils.file.read import read_file_to_string


def log_dataframe_info(df: pd.DataFrame) -> None:
    """
    Logs key information about the given pandas DataFrame, such as its shape, columns,
    data types, memory usage, and missing values.

    Args:
        df (pd.DataFrame): The pandas DataFrame to log information about.

    Returns:
        None
    """
    mem_usage = df.memory_usage(deep=True).sum() / (1024**2)  # Convert to MB
    missing_values = df.isnull().sum()
    numeric_stats = df.describe().to_string()

    logger.debug(
        f"""

DataFrame Shape: {df.shape[0]} rows, {df.shape[1]} columns

DataFrame Columns and Data Types:
{df.dtypes}

DataFrame Memory Usage: {mem_usage:.2f} MB

Missing Values:
{missing_values[missing_values > 0]}

Numeric Statistics:
{numeric_stats}
"""
    )


def read_file_object_to_dataframe(
    file_obj: BytesIO, file_format: str, **kwargs
) -> pd.DataFrame:
    """
    Convert a file object into a pandas DataFrame.

    Args:
        file_obj (BytesIO): The file object to read.
        file_format (str): The type of the file ('csv', 'excel').
        **kwargs: Additional arguments to pass to the pandas read function (e.g., sep, encoding).

    Returns:
        pd.DataFrame: A pandas DataFrame containing the file data.

    Raises:
        ValueError: If the file type is not supported.
    """
    logger.debug(
        f"Converting file object to DataFrame as {file_format} with kwargs: {kwargs}"
    )

    if file_format.lower() in ("csv", ".csv"):
        return pd.read_csv(file_obj, **kwargs)

    elif file_format in ("excel", "xlsx", "xls", ".xlsx", ".xls"):
        return pd.read_excel(file_obj, **kwargs)

    else:
        logger.error(f"Unsupported file type: {file_format}")
        raise


def default_read_sql_to_dataframe(
    con: Any, sql_file_path: str, sql_string: str
) -> pd.DataFrame:
    """
    Executes a SQL query using a given connection and returns the result as a DataFrame.

    Args:
        con (Any): The database connection object.
        sql_query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: The result of the SQL query as a DataFrame.
    """
    query = sql_string if sql_string else read_file_to_string(sql_file_path)
    logger.info("Executing SQL query and fetching results into DataFrame.")
    try:
        df = pd.read_sql(query, con=con)
        log_dataframe_info(df)
        logger.success("Successfully executed SQL query and fetched DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Failed to execute SQL query. Error: {e}")
        raise
