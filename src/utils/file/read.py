import yaml
from dotenv import load_dotenv
from loguru import logger


def read_yaml_to_dict(file_path: str) -> dict:
    """
    Reads a YAML file and returns its contents as a Python dictionary.

    Args:
        file_path (str): Path to the YAML file.

    Returns:
        dict: A dictionary representing the contents of the YAML file, or None if there is an error.

    Raises:
        yaml.YAMLError: If there is an error reading the YAML file.
    """
    with open(file_path, "r") as file:
        try:
            logger.info(f"Reading {file_path} to dictionary")
            return yaml.safe_load(file)
        except yaml.YAMLError as exc:
            logger.error(f"Error reading YAML file: {exc}")
            return None


def read_file_to_string(file_path: str) -> str:
    """
    Reads the content of a file and returns it as a string.

    Args:
        file_path (str): Path to the file to be read.

    Returns:
        str: Content of the file as a string, or None if the file is not found or another error occurs.

    Raises:
        FileNotFoundError: If the file does not exist.
        Exception: For any other errors encountered while reading the file.
    """
    try:
        logger.info(f"Reading {file_path} to string")
        with open(file_path, "r", encoding="utf-8") as file:
            file_content = file.read()
        return file_content
    except FileNotFoundError:
        logger.error(f"Error: The file at {file_path} was not found.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
