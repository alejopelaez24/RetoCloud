import logging
import requests
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python HTTP trigger function processed a request.')
        actionList = ["AAPL", "MSFT", "TSLA", "MSTR", "META", "CRM", "AMZN"]
        print(actionList)
        for x in actionList:
            readAPIActions(x)
        logging.info('Se leyó correctamente la API de acciones con información de AAPL')
        return func.HttpResponse('Proceso completado con exito', status_code=200)
    
    except Exception as e:
        print(str(e))
        logging.error('Ocurrio un error en: ' + str(e))
        return func.HttpResponse('Ocurrio un error en: ' + str(e), status_code=400)


def readAPIActions(action):
    #Apple
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={action}&interval=60min&outputsize=compact&apikey=4EVM3NBBM0PCESWU'
    r = requests.get(url, headers={'open': 'close'})
    df = pd.DataFrame.from_dict(r.json()["Monthly Time Series"],orient='index')
    appl_df = df.assign(Symbol = action)
    print(appl_df)