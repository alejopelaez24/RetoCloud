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
        actionList = ["AAPL", "MSFT"]#, "TSLA", "MSTR", "META"]#, "CRM", "AMZN"]
        apikey = "4EVM3NBBM0PCESWU"
        interval = "60min"
        print(actionList)
        for x in actionList:
            readAPIActions(x, apikey, interval)
        logging.info(f'Se leyeron e insertaron correctamente los archivos de la API de acciones')
        for x in actionList:
            redAPISentiment(x, apikey)
        logging.info(f'Se leyeron e insertaron correctamente los archivos de la API de sentimientos')
        
        return func.HttpResponse('Proceso completado con exito', status_code=200)
    
    except Exception as e:
        print(str(e))
        logging.error('Ocurrio un error en: ' + str(e))
        return func.HttpResponse('Ocurrio un error en: ' + str(e), status_code=400)


def readAPIActions(action, apikey, interval):
    sufix = "prices"
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={action}&interval={interval}&outputsize=compact&apikey={apikey}'
    r = requests.get(url, headers={'open': 'close'})
    df = pd.DataFrame.from_dict(r.json()["Weekly Time Series"], orient='index')
    action_df = df.assign(Symbol = action)
    js = action_df.to_json(orient='index')
    DLIngest(action, js, sufix)
    #print(action_df)

def redAPISentiment(action, apikey):
    sufix = "sentiment"
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={action}&apikey={apikey}'
    r = requests.get(url)
    sentiment_df = pd.DataFrame.from_dict(r.json()["feed"])
    js = sentiment_df.to_json(orient='index')
    DLIngest(action, js, sufix)
    #print(sentiment_df)

# Función para conexión al DataLake e insertar JSON's
def DLIngest(action, js, sufix):
    account_name = os.environ["account_name"]
    account_key = os.environ["account_key"]
    blob_service_client = BlobServiceClient(account_url=f'https://{account_name}.blob.core.windows.net/', credential=account_key)
    #print(js)
    blob_client = blob_service_client.get_blob_client(container='container-alejopelaez', blob=f'{action}_{sufix}.json')
    blob_client.upload_blob(js, overwrite=True)
    logging.info(f'Se insertó correctamente el archivo {action}_{sufix}.json')


# funcion para conexión al DataLake y leer json's
#def read_file(action):#, sufix):
#    account_name = os.environ["account_name"]
#    account_key = os.environ["account_key"]
#    blob_service_client = BlobServiceClient(account_url=f'https://{account_name}.blob.core.windows.net/', credential=account_key)
#    blob_clientA = blob_service_client.get_blob_client(container='container-alejopelaez', blob=f'{action}_prices.parquet')
#    downloaderA = blob_clientA.download_blob()
#    readA = downloaderA.readall()
#    print(readA)

#    logging.info('Se leyó correctamente el archivo json AAPL.json')
