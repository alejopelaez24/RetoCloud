import logging
import azure.functions as func
from IngestionFunction import functions as rf


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python HTTP trigger function processed a request.')
        actionList = ["AAPL", "MSFT", "TSLA", "MSTR", "META",  "CRM", "AMZN"] 
        apikey = "4EVM3NBBM0PCESWU"
        interval = "60min"
        for x in actionList:
            rf.readAPIActions(x, apikey, interval)
        logging.info(f'Se leyeron e insertaron correctamente los archivos de la API de acciones')
        for x in actionList:
            rf.redAPISentiment(x, apikey)
        logging.info(f'Se leyeron e insertaron correctamente los archivos de la API de sentimientos')
        return func.HttpResponse('Proceso completado con exito', status_code=200)
    
    except Exception as e:
        print(str(e))
        logging.error('Ocurrio un error en: ' + str(e))
        return func.HttpResponse('Ocurrio un error en: ' + str(e), status_code=400)