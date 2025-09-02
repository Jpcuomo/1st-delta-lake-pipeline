"""
Project settings and constants for Binance API pipeline.
"""

from config.settings import KEY

#--------------------------------------------------------------------------------
# Seteo de archivo incremental
CONTENIDO_INCREMENTAL = {'valor_previo':0,'ultimo_valor':0}

#--------------------------------------------------------------------------------
# Parametros de la API
BINANCE_HIST_TRADES = {
    'base_url':'https://api.binance.com/api/v3',
    'endpoint':"historicalTrades",
    'params':{
        "symbol": "symbol", 
        "limit": 'limit',
        'fromId':'fromId'
        },
    'headers':{"X-MBX-APIKEY":KEY}
}

# Configuración de parámetros
BINANCE_HIST_TRADES['params']['symbol'] = 'SOLUSDT'
BINANCE_HIST_TRADES['params']['limit'] = 1000
BINANCE_HIST_TRADES['params']['fromId'] = 0

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