"""
Project settings and constants for Binance API pipeline.
"""

from config.settings import KEY

#--------------------------------------------------------------------------------
# Seteo de archivo incremental
CONTENIDO_INCREMENTAL = {'valor_previo':0,'ultimo_valor':0}

#--------------------------------------------------------------------------------
# Parametros de la API

BASE_URL = 'https://api.binance.com/api/v3'
ENDPOINT = 'historicalTrades'
SYMBOL = 'SOLUSDT'
LIMIT = 1000
FROM_ID = 0
BATCH_QTTY = 10

BINANCE_HIST_TRADES = {
    'base_url':BASE_URL,
    'endpoint':ENDPOINT,
    'params':{
        "symbol": SYMBOL, 
        "limit": LIMIT,
        'fromId':FROM_ID
        },
    'headers':{"X-MBX-APIKEY":KEY},
    'batch_qtty':BATCH_QTTY
}

# Configuración de parámetros

#--------------------------------------------------------------------------------
# Data Processing
DATA_PROCESING = {
    'default_partition_cols':['date'],
    'max_retires':3,
    'retry_delay':5 # seconds
}

#--------------------------------------------------------------------------------
# File Formats
FILE_FORMATS = {
    'delta_format':'delta',
    'parquet_format':'parquet',
    'engine':'pyarrow'
}

NOMBRE_COLUMNAS_DESEADAS = {
    'time': 'miliseconds',
    'qty': 'quantity',
    'quoteQty': 'quote_qty',
    'isBuyerMaker': 'is_buyer_maker'
}

CONVERSION_MAPPING = {
    'price':'float32',
    'quantity':'float32',
    'quote_qty':'float32'
}