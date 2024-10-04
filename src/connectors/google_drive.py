import fnmatch
from io import BytesIO
from typing import Dict, List

import duckdb
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import HttpError, build
from googleapiclient.http import MediaIoBaseUpload
from loguru import logger

from src.connectors.duck import create_table
from src.utils.dataframe.read import (log_dataframe_info,
                                      read_file_object_to_dataframe)


def connect_to_google_drive(service_account_file: str) -> build:
    """
    Authenticates and creates a Google Drive service client using a service account key.

    Args:
        service_account_file (str): Path to the service account key JSON file.

    Returns:
        build: Authenticated Google Drive API client.

    Raises:
        Exception: If there is an error creating the Google Drive service.
    """
    logger.info(f"Authenticating using service account file: {service_account_file}")
    try:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        service = build("drive", "v3", credentials=credentials)
        logger.success("Successfully authenticated and created Google Drive service.")
        return service
    except Exception as e:
        logger.error(f"Error creating Google Drive service: {e}")
        raise


def list_files_in_folder(service: build, folder_id: str) -> List[Dict[str, str]]:
    """
    Fetches all files from a specified Google Drive folder.

    Args:
        service (build): Google Drive API service client.
        folder_id (str): The ID of the Google Drive folder.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing file metadata such as file ID and name.

    Raises:
        HttpError: If there is an error fetching the files.
    """
    logger.info(f"Fetching files from folder {folder_id}")
    query = (
        f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    )

    try:
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])

        if not files:
            logger.warning(f"Folder {folder_id} is empty or does not exist.")
            return []

        logger.info(f"Fetched {len(files)} files from folder {folder_id}.")
        return files

    except HttpError as e:
        logger.error(f"Failed to fetch files from folder {folder_id}: {e}")
        return []


def filter_files_in_list(
    files: List[Dict[str, str]], pattern: str
) -> List[Dict[str, str]]:
    """
    Filters a list of files based on the specified glob pattern.

    Args:
        files (List[Dict[str, str]]): List of file metadata dictionaries, each containing file name and ID.
        pattern (str): Glob pattern to filter file names (e.g., '*PCS*').

    Returns:
        List[Dict[str, str]]: A list of files that match the specified pattern.
    """
    if not pattern:
        logger.info("No pattern provided, returning original file list.")
        return files

    logger.info(f"Filtering files by pattern: {pattern}")

    filtered_files = [file for file in files if fnmatch.fnmatch(file["name"], pattern)]

    if not filtered_files:
        logger.warning(f"No files match the pattern: {pattern}")
        return []

    logger.info(f"{len(filtered_files)} files matched the given pattern.")
    return filtered_files


def download_file_as_bytes(service: build, file_id: str) -> BytesIO:
    """
    Downloads a file from Google Drive as a byte stream.

    Args:
        service (build): Google Drive API service client.
        file_id (str): The ID of the file to download.

    Returns:
        BytesIO: A byte stream containing the file data.

    Raises:
        HttpError: If there is an error downloading the file.
    """
    logger.info(f"Reading file with ID {file_id} as byte stream.")
    try:
        request = service.files().get_media(fileId=file_id)
        return BytesIO(request.execute())
    except HttpError as e:
        logger.error(f"Error downloading file with ID {file_id}: {e}")
        raise


def read_file_to_dataframe(
    service: build, file_id: str, file_format: str, **kwargs
) -> pd.DataFrame:
    """
    Reads a file from Google Drive into a pandas DataFrame.

    Args:
        service (build): Google Drive API service client.
        file_id (str): The ID of the file to download.
        file_format (str): The format of the file (e.g., 'csv', 'excel', 'json').
        **kwargs: Additional arguments passed to the file reader.

    Returns:
        pd.DataFrame: The file data as a pandas DataFrame.
    """
    file_object = download_file_as_bytes(service, file_id)
    return read_file_object_to_dataframe(file_object, file_format, **kwargs)


def read_folder_to_dataframe(
    service: build,
    folder_id: str,
    file_format: str = "csv",
    pattern: str = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Reads all files of a specific format from a Google Drive folder into a single pandas DataFrame.

    Args:
        service (build): Google Drive API service client.
        folder_id (str): The ID of the Google Drive folder.
        file_format (str): The format of the files to read (e.g., 'csv', 'excel', 'json'). Defaults to 'csv'.

    Returns:
        pd.DataFrame: A concatenated DataFrame containing the data from all files in the specified format.
    """
    files = list_files_in_folder(service, folder_id)
    if pattern:
        files = filter_files_in_list(files, pattern)
    dataframes = [
        read_file_to_dataframe(service, file["id"], file_format, **kwargs)
        for file in files
    ]

    if not dataframes:
        logger.warning(
            f"No files found in folder {folder_id} based on passed parameters"
        )
        logger.debug(f"fil_format: {file_format}, pattern: {pattern}, kwargs: {kwargs}")
        logger.warning("Returning empty DataFrame")
        return pd.DataFrame()

    df = pd.concat(dataframes, ignore_index=True)
    logger.success(
        f"Successfully read {len(dataframes)} files into a single DataFrame."
    )
    log_dataframe_info(df)
    return df


# Higher order function
def ingest(
    settings: dict,
    service: build,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    try:
        ingest_settings = settings["ingest"]["google_drive"]
    except KeyError as e:
        logger.warning(f"Missing key in settings dictionary: {e}")

    folders = ingest_settings.get("folders")
    files = ingest_settings.get("files")

    if not folders:
        logger.warning("No Google Drive folder specified in settings")
    else:
        for folder in folders:
            df = read_folder_to_dataframe(
                service,
                folder["id"],
                folder["file_format"],
                **folder.get("config", {}),
            )
            create_table(duckdb_conn, df, folder["table_name"])

    if not files:
        logger.warning("No Google Drive folder specified in settings")
    else:
        for file in files:
            df = read_file_to_dataframe(
                service,
                file["id"],
                file["file_format"],
                **file.get("config", {}),
            )
            create_table(duckdb_conn, df, file["table_name"])
