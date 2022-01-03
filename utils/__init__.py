import os
import json
import logging
import requests
import pandas as pd
import azure.functions as func
from io import StringIO
from urllib.parse import parse_qs
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

def execute_dax_query(credential, dataset_id, daxQuery):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"        
    try: 
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
        headers = { "Content-Type" : "application/json", "Authorization": f"Bearer {token}"}
        r = requests.post(url, headers=headers, data=daxQuery)
        r.encoding='utf-8-sig'
        return r
    except Exception as e:
        logging.exception(e)
        raise

def convert_to_csv(data): 
    df = pd.read_json(StringIO(data), orient='records')
    return df.to_csv(index = False)

def upload_file(service_client, container_name, file_path, data):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=container_name)
        file_client = file_system_client.get_file_client(file_path)
        r = file_client.upload_data(data, overwrite=True)
        return { 'filePath': f"{container_name}{file_path}", "request_id": r['request_id'] } 
    except Exception as e:
        logging.exception(e)
        raise