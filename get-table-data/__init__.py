import os
import json
import logging
import requests
import pandas as pd
from io import StringIO
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

def query_dataset(credential, dataset_id, table_name, top_n_rows):    
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"        
    try: 
        daxQuery = json.dumps({
                "queries": [{ "query": f"EVALUATE TOPN({top_n_rows}, '{table_name}')"}],
                "serializerSettings": { "incudeNulls": True }
        })
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token
        headers = { "Content-Type" : "application/json", "Authorization": f"Bearer {token}"}
        r = requests.post(url, headers=headers, data=daxQuery)
        r.encoding='utf-8-sig'
        data = json.dumps((r.json())['results'][0]['tables'][0]['rows'])
    except Exception as e:
        logging.exception(e)
        raise
    return data

def convert_to_csv(data): 
    df = pd.read_json(StringIO(data), orient='records')
    return df.to_csv(index = False)

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
    dataset_id = req.params.get('datasetId')
    table_name = req.params.get('tableName')
    top_n_rows = req.params.get('tableNRows')
    convert_to_csv = req.params.get('convertToCsv')
    file_path = req.params.get('filePath')
    
    if not (dataset_id or table_name or top_n_rows or file_path):
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
        if not (top_n_rows):
            top_n_rows = req_body.get('topNRows')    
        if not (convert_to_csv):
            convert_to_csv = req_body.get('convertToCsv')    
    

    if not (dataset_id or table_name or file_path):
        if not dataset_id:
            logging.exception("Dataset id is missing")
        if not table_name:
            logging.exception("Table name is missing")
        if not file_path:
            logging.exception("File path is missing")
        raise Exception("One or more of required parameters are missing")

    if (top_n_rows is None): 
        top_n_rows = 100000 #Set default to 100000 rows

    if (convert_to_csv is None): 
        convert_to_csv = True #Convert to CSV by default

    return { "datasetId": dataset_id, 
            "tableName": table_name, 
            "topNRows": top_n_rows, 
            "convertToCsv": convert_to_csv, 
            "filePath" : file_path }


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

        data = query_dataset(credential, args['datasetId'], 
                                args['tableName'],  args['topNRows'])        

        logging.info(f"Copying data to file {args['filePath']}")    
        if (args['convertToCsv']):
            upload_file(service_client, container_name, args['filePath'], convert_to_csv(data))
        else: 
            upload_file(service_client, container_name, args['filePath'],data)

        return func.HttpResponse("Ok", status_code=200)
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse("One or more errors occured", status_code=500)
