"""
Constants and settings for the Binance API ETL/ELT pipeline project.
"""

import os
import logging
from dotenv import load_dotenv
from datetime import datetime

logger = logging.getLogger('pipeline')

#---------------------------------------------------------
# Loading environment variables
#---------------------------------------------------------
load_dotenv()

#---------------------------------------------------------
# Validations and casting of environment variables
#---------------------------------------------------------
def get_env_int(var_name:str, default:int) -> int:
    """Returns an int environment variable with validation"""
    try:
        valor = os.getenv(var_name)
        return int(valor) if valor is not None else default
    except ValueError:
        logger.warning(f'The environment variable must be an integer. Using {default} by default.')
        return default
    
def get_env_datetime(var_name:str, default:datetime) -> datetime:
    """Returns a datetime environment variable with validation"""
    valor = os.getenv(var_name)
    if valor:
        try:
            if valor.endswith('Z'):
                valor = valor.replace('Z', '+00:00')
            return datetime.fromisoformat(valor)
        except ValueError:
            logger.warning(f'Invalid date, using {default} by default')
    return default

def get_env_str(var_name: str, default: str) -> str:
    """Returns a string environment variable with validation"""
    value = os.getenv(var_name)
    return value if value is not None else default

def datetime_to_millis(dt: datetime) -> int:
    """Conversion from datetime to milliseconds"""
    return int(dt.timestamp() * 1000)
    

#---------------------------------------------------------
# Initialization of the incremental file (metadata)
# (for Historical_trades API)
#---------------------------------------------------------
INCREMENTAL_CONTENT = {'previous_value':0,'last_value':0}

#---------------------------------------------------------
# Historical_trades API parameters
#---------------------------------------------------------
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
SYMBOL = get_env_str('HIST_TRADES_SYMBOL', 'SOLUSDT')
LIMIT = get_env_int("HIST_TRADES_LIMIT", 1000)
FROM_ID = get_env_int('HIST_TRADES_FROM_ID', 0)
BATCH_QTTY = get_env_int('HIST_TRADES_BATCH_QTTY', 1)

#---------------------------------------------------------
# Klines API parameters
#---------------------------------------------------------
KLINES_SYMBOL = get_env_str('KLINES_SYMBOL', 'SOLUSDT')
KLINES_INTERVAL = get_env_str('KLINES_INTERVAL', '1m')
KLINES_START_TIME = get_env_datetime('KLINES_START_TIME', datetime(2024, 7, 31, 0, 0, 0))
KLINES_END_TIME = get_env_datetime('KLINES_END_TIME', datetime(2025, 7, 31, 23, 59, 59))
KLINES_LIMIT = get_env_int('KLINES_LIMIT', 1000)


#---------------------------------------------------------
# Dictionary with general APIs configurations 
#---------------------------------------------------------
BASE_URL = 'https://api.binance.com/api/v3'

BINANCE_API_CONFIG = {
    'historical_trades':{ # Endpoint historicalTrades
        'base_url':BASE_URL,
        'endpoint':'historicalTrades',
        'params':{
            'symbol':SYMBOL, 
            'limit':LIMIT,
            'fromId':FROM_ID
            },
        'headers':{'X-MBX-APIKEY':BINANCE_API_KEY},
        'batch_qtty':BATCH_QTTY,
        'partition_cols':['date','hr']
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
# Parameters and settings for Binance API/Historical_trades
#---------------------------------------------------------
DESIRED_COLS_HT = {
    'time': 'milliseconds',
    'qty': 'quantity',
    'quoteQty': 'quote_qty',
    'isBuyerMaker': 'is_buyer_maker'
}

CONVERSION_MAPPING_HT = {
    'price':'float32',
    'quantity':'float32',
    'quote_qty':'float32'
}


#---------------------------------------------------------
# Parameters and settings for Binance/Klines API
#---------------------------------------------------------

# Dictionary for renaming columns with desired names
DESIRED_COLS_KL = {
    0:"open_time",
    1:"open",
    2:"high",
    3:"low",
    4:"close",
    5:"volume",
    6:"close_time",
    7:"quote_asset_volume",
    8:"num_trades",
    9:"tb_base_asset_volume",
    10:"tb_quote_asset_volume",
    11:"ignore"}

# Column mapping for data type conversion
CONVERSION_MAPPING_KL = {
    "open":"float32",
    "high":"float32",
    "low":"float32",
    "close":"float32",
    "volume":"float32",
    "quote_asset_volume":"float64",
    "num_trades":"int32",
    "tb_base_asset_volume":"float32",
    "tb_quote_asset_volume":"float64"
    }


#---------------------------------------------------------
# Other general settings
#---------------------------------------------------------
# File format
FILE_FORMATS = {
    'delta_format':'delta',
    'parquet_format':'parquet',
    'engine':'pyarrow'
}