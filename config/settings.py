"""
Project settings and constants for Binance API pipeline.
"""

import os
import logging
from dotenv import load_dotenv
from datetime import datetime

logger = logging.getLogger('pipeline')

#---------------------------------------------------------
# Carga de variables de entorno
#---------------------------------------------------------
load_dotenv()

#---------------------------------------------------------
# Validaciones y casteos de variables de entorno
#---------------------------------------------------------
def get_env_int(var_name:str, default:int) -> int:
    """Retorna variable de entorno int con validacion"""
    try:
        valor = os.getenv(var_name)
        return int(valor) if valor is not None else default
    except ValueError:
        logger.warning(f'La variable de entorno debe ser un entero. Usando {default} por defecto')
        return default
    
def get_env_datetime(var_name:str, default:datetime) -> datetime:
    """Retorna variable de entorno datetime con validacion"""
    valor = os.getenv(var_name)
    if valor:
        try:
            if valor.endswith('Z'):
                valor = valor.replace('Z', '+00:00')
            return datetime.fromisoformat(valor)
        except ValueError:
            logger.warning(f'Fecha inválida, usando {default} por defecto')
    return default

def get_env_str(var_name: str, default: str) -> str:
    """Retorna variable de entorno string con validacion"""
    value = os.getenv(var_name)
    return value if value is not None else default

def datetime_to_millis(dt: datetime) -> int:
    """Conversión de datetime a milisegundos"""
    return int(dt.timestamp() * 1000)
    

#---------------------------------------------------------
# Inicialización de archivo incremental
#---------------------------------------------------------
CONTENIDO_INCREMENTAL = {'valor_previo':0,'ultimo_valor':0}

#---------------------------------------------------------
# Parametros de la API Historical_trades
#---------------------------------------------------------
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
SYMBOL = get_env_str('HIST_TRADES_SYMBOL', 'SOLUSDT')
LIMIT = get_env_int("HIST_TRADES_LIMIT", 1000)
FROM_ID = get_env_int('HIST_TRADES_FROM_ID', 0)
BATCH_QTTY = get_env_int('HIST_TRADES_BATCH_QTTY', 1)

#---------------------------------------------------------
# Parametros de la API Klines
#---------------------------------------------------------
KLINES_SYMBOL = get_env_str('KLINES_SYMBOL', 'SOLUSDT')
KLINES_INTERVAL = get_env_str('KLINES_INTERVAL', '1m')
KLINES_START_TIME = get_env_datetime('KLINES_START_TIME', datetime(2024, 7, 31, 0, 0, 0))
KLINES_END_TIME = get_env_datetime('KLINES_END_TIME', datetime(2025, 7, 31, 23, 59, 59))
KLINES_LIMIT = get_env_int('KLINES_LIMIT', 1000)

BASE_URL = 'https://api.binance.com/api/v3'

BINANCE_API = {
    'historical_trades':{ # Endpoint historicalTrades
        'base_url':BASE_URL,
        'endpoint':'historicalTrades',
        'params':{
            'symbol':SYMBOL, 
            'limit':LIMIT,
            'fromId':FROM_ID
            },
        'headers':{'X-MBX-APIKEY':BINANCE_API_KEY},
        'batch_qtty':BATCH_QTTY
    },
    'klines': { # Endpoint klines
        'base_url':BASE_URL,
        'endpoint':'klines',
        'params': {
            'symbol':KLINES_SYMBOL,
            'startTime':datetime_to_millis(KLINES_START_TIME),
            'endTime':datetime_to_millis(KLINES_END_TIME),
            'interval':KLINES_INTERVAL,
            'limit':KLINES_LIMIT
        },
        'headers':{'X-MBX-APIKEY': BINANCE_API_KEY},
    }
}

#---------------------------------------------------------

#---------------------------------------------------------
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


# Data Processing
DATA_PROCESING = {
    'default_partition_cols':['date'],
    'max_retires':3,
    'retry_delay':5 # seconds
}

# File Formats
FILE_FORMATS = {
    'delta_format':'delta',
    'parquet_format':'parquet',
    'engine':'pyarrow'
}