import os
import json
import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


def get_credential():
    """
    Gets Azure AD auth credential.
    """
    return DefaultAzureCredential()


def get_adls_gen2_service_client(credential, storage_account_name):
    return DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=credential)


def upload_file(service_client, container_name, file_path, body):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=container_name)
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(body, overwrite=True)
    except Exception as e:
        logging.exception(e)
        raise


def main(req: func.HttpRequest) -> func.HttpResponse:

    storage_account_name = os.environ['STORAGE_ACCOUNT_NAME']
    container_name = os.environ['CONTAINER_NAME']
    file_path = req.params.get('filePath')

    try:
        req_body = req.get_json()
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)

    if not (file_path):
        file_path = req_body.get('filePath')
    file_content = req_body.get('fileContent')

    if not (file_path or file_content):
        if not file_path:
            logging.info("File path is missing")
        if not file_content:
            logging.info("File content is missing")
        return func.HttpResponse("One or more of required parameters are missing", status_code=400)

    logging.info(f"Getting credential")
    credential = get_credential()

    logging.info(f"Creating ADLS Gen2 service client")
    service_client = get_adls_gen2_service_client(credential, storage_account_name)

    logging.info(f'Creating file "{file_path}"')
    try:
        upload_file(service_client, container_name, file_path,
                body=json.dumps(file_content))
        return func.HttpResponse("Successfully created file", status_code=200)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
