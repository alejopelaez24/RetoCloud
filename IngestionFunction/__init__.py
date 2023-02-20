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
        apikey = "4EVM3NBBM0PCESWU"
        interval = "60min"
        print(actionList)
        for x in actionList:
            readAPIActions(x, apikey, interval)
        logging.info(f'Se leyeron e insertaron correctamente los archivos de la API de acciones')
        for x in actionList:
            redAPISentiment(x, apikey)
        logging.info(
            f'Se leyeron e insertaron correctamente los archivos de la API de sentimientos')
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
    DLIngest(action, action_df, sufix)
    #print(action_df)

def redAPISentiment(action, apikey):
    sufix = "sentiment"
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={action}&apikey={apikey}'
    r = requests.get(url)
    sentiment_df = pd.DataFrame.from_dict(r.json()["feed"])
    DLIngest(action, sentiment_df, sufix)
    #print(sentiment_df)

# Función para conexión al DataLake e insertar parquet's
def DLIngest(action, action_df, sufix):
    account_name = os.environ["account_name"]
    account_key = os.environ["account_key"]
    blob_service_client = BlobServiceClient(
        account_url=f'https://{account_name}.blob.core.windows.net/', credential=account_key)
    action_df.to_parquet(f'{action}_{sufix}.parquet', engine='fastparquet')
    # data = action_df.to_parquet(f'{action}.parquet', engine='fastparquet')
    # print(data)
    pd.read_parquet(f'{action}_{sufix}.parquet')
    # file = open(f'{action}.parquet')
    # data = file.read()
    blob_client = blob_service_client.get_blob_client(
        container='container-alejopelaez', blob=f'{action}_{sufix}.parquet')
    # , data='PruebaValor')
    blob_client.upload_blob(f'{action}_{sufix}.parquet', overwrite=True)

    # file_client = blob_client.upload_blob(name = f'{action}.parquet', data=data, overwrite=True)
    # file.close
    # readA = downloaderA.readall()

    logging.info(f'Se insertó correctamente el archivo {action}_{sufix}.parquet')


# funcion para conexión al DataLake y leer json's
#def read_file(action, sufix):
#    account_name = os.environ["account_name"]
#    account_key = os.environ["account_key"]
#    blob_service_client = BlobServiceClient(
#        account_url=f'https://{account_name}.blob.core.windows.net/', credential=account_key)
#    blob_clientA = blob_service_client.get_blob_client(
#        container='container-alejopelaez', blob=f'{action}_{sufix}.parquet')
#    downloaderA = blob_clientA.download_blob()
#    readA = downloaderA.readall()

#    logging.info('Se leyó correctamente el archivo json AAPL.json')
