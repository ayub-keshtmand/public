import os
import shutil

from loguru import logger


# todo: refactor
# higher order function: write_file_to_{source}_as_{file_format}(connection_object, ...)
def snapshot_file(file_path, snapshot_dir):
    """
    Copies a file to the specified snapshot directory.
    If the directory does not exist, it is created.

    Args:
        file_path (str): The path to the file to be copied.
        snapshot_dir (str): The directory where the file will be copied.

    Returns:
        str: The path to the copied file.
    """

    logger.info(f"Starting snapshot of file: {file_path} to directory: {snapshot_dir}")

    # Check if the file exists
    if not os.path.isfile(file_path):
        logger.warning(f"File not found: {file_path}. Skipping snapshot")
        return None

    logger.info(f"File found: {file_path}")

    # Create the snapshot directory if it doesn't exist
    if not os.path.exists(snapshot_dir):
        logger.info(f"Directory '{snapshot_dir}' does not exist. Creating it.")
    os.makedirs(snapshot_dir, exist_ok=True)

    # Get the base name of the file (e.g., dev.duckdb)
    file_name = os.path.basename(file_path)
    destination_path = os.path.join(snapshot_dir, file_name)

    # Copy the file to the snapshot directory
    shutil.copy2(file_path, destination_path)
    logger.info(f"File copied successfully to {destination_path}")

    return destination_path
