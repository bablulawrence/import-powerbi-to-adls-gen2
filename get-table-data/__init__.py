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
        return data
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
        file_client.upload_data(data, overwrite=True)
    except Exception as e:
        logging.exception(e)
        raise

def parse_agruments(req):
    
    try:
        args = {
            'datasetId': req.route_params.get('datasetId'), 
            'tableName': req.route_params.get('tableName')
        }
        req_body = req.get_json()

        if ('topNRows' not in req_body): 
            args['topNRows'] = 100000 #Set default to the maximum 100,000 rows allowed for a Power BI Tables
        else:
            args['topNRows'] = req_body['topNRows']

        if ('convertToCsv' not in req_body): 
            args['convertToCsv'] = True #Convert to CSV by default
        else:
            args['convertToCsv'] = req_body['convertToCsv']
                
        if ('filePath' not in req_body): #Default file path is /<container name>/<dataset id>/<table name>.csv or .json
            if (args['convertToCsv']):
                args['filePath'] = f"/{args['datasetId']}/{args['tableName']}.csv" 
            else:
                args['filePath'] = f"/{args['datasetId']}/{args['tableName']}.json" 
        else:
            args['filePath'] = req_body['filePath']

        logging.error(json.dumps(args))
        return args        
    except Exception as e:
        logging.exception(e)
        raise


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
        logging.error(json.dumps(args))
        logging.info(f"Copying data to file {args['filePath']}")    
        if (args['convertToCsv']):
            upload_file(service_client, container_name, args['filePath'], convert_to_csv(data))
        else: 
            upload_file(service_client, container_name, args['filePath'],data)

        return func.HttpResponse("Ok", status_code=200)
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse("One or more errors occured", status_code=500)
