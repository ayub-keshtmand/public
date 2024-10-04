"""Common DataFrame write functions."""

from io import BytesIO

import pandas as pd
from loguru import logger


def write_dataframe_to_file_buffer(
    df: pd.DataFrame, file_type: str = "csv", **kwargs
) -> BytesIO:
    """
    Converts a pandas DataFrame into a file buffer (CSV or Excel).

    Args:
        df (pd.DataFrame): The DataFrame to convert.
        file_type (str, optional): The type of the file ('csv' or 'excel'). Defaults to 'csv'.
        **kwargs: Additional arguments to pass to pandas DataFrame writing functions (e.g., index, header).

    Returns:
        BytesIO: An in-memory file-like object containing the DataFrame's content.
    """
    logger.info(f"Converting DataFrame to {file_type} format")

    file_buffer = BytesIO()

    try:
        if file_type == "csv":
            df.to_csv(file_buffer, **kwargs)
        elif file_type == "excel":
            # df.to_excel(file_buffer, engine="xlsxwriter", **kwargs)
            df.to_excel(
                file_buffer, engine=kwargs.pop("engine", "xlsxwriter"), **kwargs
            )
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Ensure the buffer is ready to be read from the beginning
        file_buffer.seek(0)

        logger.success(f"Successfully converted DataFrame to {file_type} format")
        return file_buffer
    except Exception as e:
        logger.error(f"Failed to convert DataFrame to {file_type} format: {e}")
        raise
