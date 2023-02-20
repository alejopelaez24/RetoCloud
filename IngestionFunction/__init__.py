import logging
import requests
import pandas as pd
import azure.functions as func
import os
from azure.storage.blob import BlobServiceClient
import pyarrow as pa
import pyarrow.parquet as pq  


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python HTTP trigger function processed a request.')
        actionList = ["AAPL", "MSFT", "TSLA", "MSTR", "META", "CRM", "AMZN"]
        print(actionList)
        for x in actionList:
            readAPIActions(x)
            
        logging.info(f'Se leyó correctamente la API de acciones con información de {x}')
        return func.HttpResponse('Proceso completado con exito', status_code=200)
    
    except Exception as e:
        print(str(e))
        logging.error('Ocurrio un error en: ' + str(e))
        return func.HttpResponse('Ocurrio un error en: ' + str(e), status_code=400)


def readAPIActions(action):
    #Apple
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={action}&interval=60min&outputsize=compact&apikey=4EVM3NBBM0PCESWU'
    r = requests.get(url, headers={'open': 'close'})
    df = pd.DataFrame.from_dict(r.json()["Weekly Time Series"], orient='index')
    action_df = df.assign(Symbol = action)
    DLIngest(action, action_df)
    print(action_df)

#Función para conexión al DataLake e insertar json's
def DLIngest(action, action_df):
    account_name = os.environ["account_name"]
    account_key = os.environ["account_key"]
    blob_service_client = BlobServiceClient(account_url=f'https://{account_name}.blob.core.windows.net/', credential=account_key)
    #print(action)
    #print(action_df)
    action_df.to_parquet(f'{action}.parquet', engine='fastparquet')
    #data = action_df.to_parquet(f'{action}.parquet', engine='fastparquet')
    #print(data)
    pd.read_parquet(f'{action}.parquet')
    #file = open(f'{action}.parquet')
    #data = file.read()
    blob_client = blob_service_client.get_blob_client(container='container-alejopelaez', blob=f'{action}.parquet')
    blob_client.upload_blob(f'{action}.parquet', overwrite=True)#, data='PruebaValor')

    #file_client = blob_client.upload_blob(name = f'{action}.parquet', data=data, overwrite=True)
    #file.close
    #readA = downloaderA.readall()

    logging.info('Se leyó correctamente el archivo json AAPL.json')
