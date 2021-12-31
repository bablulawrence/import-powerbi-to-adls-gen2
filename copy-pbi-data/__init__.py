import os
import json
import logging
import requests
import azure.functions as func
import codecs
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

def query_dataset(credential, datasetId, tableName): 
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/executeQueries"    
    daxQuery = json.dumps({
                "queries": [{ "query": f"EVALUATE('{tableName}')"}],
                "serializerSettings": { "incudeNulls": True }
        })
    try: 
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
        headers = { "Content-Type" : "application/json", "Authorization": f"Bearer {token}"}
        r = requests.post(url, headers=headers, data=daxQuery)
        r.encoding='utf-8-sig'        
    except Exception as e:
        logging.exception(e)
        raise
    return r.json()

def upload_file(service_client, container_name, file_path, data):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=container_name)
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(data, overwrite=True)
    except Exception as e:
        logging.exception(e)
        raise

def parse_agruments(req):
    table_name = req.params.get('tableName')
    dataset_id = req.params.get('datasetId')
    file_path = req.params.get('filePath')
    
    if not (dataset_id or table_name or file_path):
        try:
            req_body = req.get_json()
        except Exception as e:
            logging.exception(e)
            raise
        if not (dataset_id):
            dataset_id = req_body.get('datasetId')
        if not (table_name):
            table_name = req_body.get('tableName')
        if not (file_path):
            file_path = req_body.get('filePath')    
    
    if not (dataset_id or table_name or file_path):
        if not dataset_id:
            logging.exception("Dataset id is missing")
        if not table_name:
            logging.exception("Table name is missing")
        if not file_path:
            logging.exception("File path is missing")
        raise Exception("One or more of required parameters are missing")

    return { "datasetId": dataset_id, "tableName": table_name, "filePath" : file_path }



def main(req: func.HttpRequest) -> func.HttpResponse:

    storage_account_name = os.environ['STORAGE_ACCOUNT_NAME']
    container_name = os.environ['CONTAINER_NAME']

    try: 
        args = parse_agruments(req)
        logging.info(f"Getting credential")
        credential = get_credential()

        logging.info(f"Creating ADLS Gen2 service client")
        service_client = get_adls_gen2_service_client(credential, storage_account_name)

        logging.info(f"Quering table {args['tableName']} in dataset {args['datasetId']}")    
        data = query_dataset(credential, args['datasetId'], args['tableName'])        

        logging.info(f"Copying data to file {args['filePath']}")    
        upload_file(service_client, container_name, args['filePath'], json.dumps(data))

        return func.HttpResponse("Ok", status_code=200)
    except:
        return func.HttpResponse("One or more errors occured", status_code=500)
