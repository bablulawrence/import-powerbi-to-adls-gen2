import os
import json
import logging
import requests
import pandas as pd
import azure.functions as func
from io import StringIO
from urllib.parse import parse_qs
from utils import *

def query_dataset(credential, dataset_id, daxQuery):    
    """Execute DAX query provided in the input"""
    try: 
        r = execute_dax_query(credential, dataset_id, json.dumps(daxQuery))
        if (r.status_code == 200):
            return { 
                'statusCode': r.status_code, 
                'data': (r.json())['results'][0]['tables'][0]['rows'] 
            }
        else:
            c = r.status_code
            e = r.json()['error']           
            if (c == 400):            
                if ('errorCode' in e and 'Cannot find table' in e['message']):
                    return { 'statusCode': 404, 'errorMessage': f'Table is not found' }            
                elif ('code' in e and e['code'] == 'StorageInvalidData'):
                    return { 'statusCode': 400, 'errorMessage': f'The dataset id {dataset_id} is not a valid GUID' }
                else: 
                    return { 'statusCode': 400, 'errorMessage': f'Power BI Dataset query failed' }                
            elif (c == 404):
                if ('code' in e and e['code'] == 'PowerBIEntityNotFound'):
                        return { 'statusCode': 404, 'errorMessage': f'The dataset id {dataset_id} is not found' }                
                else:
                    return { 'statusCode': c, 'errorMessage': f'Power BI Dataset query failed' }                

    except Exception as e:
        logging.exception(e)
        raise

def parse_agruments(req):
    """Parse Argument from HTTP request"""
    try:
        args = {  'datasetId': req.route_params.get('datasetId')  }
        req_body = req.get_json()
        if ('daxQuery' not in req_body): 
            raise Exception("Query cannot be null")
        else:
            args['daxQuery'] = req_body['daxQuery']

        if ('convertToCsv' not in req_body): 
            args['convertToCsv'] = True #Convert to CSV by default
        else:
            args['convertToCsv'] = req_body['convertToCsv']
                
        if ('filePath' not in req_body): #Default file path is /<container name>/<dataset id>/query_data.csv or .json
            if (args['convertToCsv']):
                args['filePath'] = f"/{args['datasetId']}/query_data.csv" 
            else:
                args['filePath'] = f"/{args['datasetId']}/query_data.json" 
        else:
            args['filePath'] = req_body['filePath']
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
        credential = get_credential() #Get credential from Azure AD

        logging.info(f"Creating ADLS Gen2 service client")
        service_client = get_adls_gen2_service_client(credential, storage_account_name) #Create ADLS Gen2 service client 
        
        logging.info(f"Executive query against dataset {args['datasetId']}")           

        queryRes = query_dataset(credential, args['datasetId'], args['daxQuery']) #Execute DAX query to get the table data in JSON format
        logging.info(f"Uploading data to file {args['filePath']}")    
        if (queryRes['statusCode'] == 200):
            if (args['convertToCsv']): # Covert table data to CSV(optionally) and upload to ADLS Gen 2
                uploadRes = upload_file(service_client, container_name, args['filePath'], 
                            convert_to_csv(json.dumps(queryRes['data'])))
            else: 
                uploadRes = upload_file(service_client, container_name, args['filePath'],
                            json.dumps(queryRes['data']))
            return func.HttpResponse(json.dumps(uploadRes), status_code=200)
        else:
            logging.exception(queryRes['errorMessage'])
            return func.HttpResponse(queryRes['errorMessage'], status_code=queryRes['statusCode'] )
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse("Internal error", status_code=500)