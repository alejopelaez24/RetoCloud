import requests
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
import logging

#Función para leer la API de time series y almacenarla en un JSON
def readAPIActions(action, apikey, interval):
    sufix = "prices"
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={action}&interval={interval}&outputsize=compact&apikey={apikey}'
    r = requests.get(url, headers={'open': 'close'})
    # df = pd.DataFrame.from_dict(r.json()["Weekly Time Series"], orient='index')
    df = pd.DataFrame.from_dict(r.json(), orient='index')
    js = df.to_json(orient='index')
    DLIngest(action, js, sufix)


# Función para leer la API de sentiments y almacenarla en un JSON
def redAPISentiment(action, apikey):
    sufix = "sentiment"
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={action}&apikey={apikey}'
    r = requests.get(url)
    sentiment_df = pd.DataFrame.from_dict(r.json()["feed"])
    # sentiment_df = pd.DataFrame.from_dict(r.json())
    js = sentiment_df.to_json(orient='index')
    DLIngest(action, js, sufix)


# Función para conexión al DataLake e insertar JSON's
def DLIngest(action, js, sufix):
    account_name = os.environ["account_name"]
    account_key = os.environ["account_key"]
    blob_service_client = BlobServiceClient(account_url=f'https://{account_name}.blob.core.windows.net/', credential=account_key)
    blob_client = blob_service_client.get_blob_client(container='container-alejopelaez', blob=f'{action}_{sufix}.json')
    blob_client.upload_blob(js, overwrite=True)
    logging.info(f'Se insertó correctamente el archivo {action}_{sufix}.json')