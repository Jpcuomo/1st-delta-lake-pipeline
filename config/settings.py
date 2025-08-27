"""
Project settings and constants for Binance API pipeline.
"""

from pathlib import Path
from src.utils import set_fecha_final, set_fecha_inicial, leer_archivo_conf
from config.paths import API_AUTH_PATH

# Parámetros de acceso a la API
API_SECCION = 'binance'
API_KEY = leer_archivo_conf(API_AUTH_PATH, API_SECCION)
KEY = API_KEY['clave_api']

# API Settings
BINANCE_BASE_URL = "https://api.binance.com"
ENDPOINT = "api/v3/klines"

START_TIME = set_fecha_inicial("2024-07-31 00:00:00Z")
END_TIME = set_fecha_final("2025-07-31 23:59:59Z")

PARAMS = {
        'symbol':'SOLUSDT',
        'interval':'1d',
        'startTime':START_TIME,
        'endTime':END_TIME,
        'limit':1000
    }

HEADERS = {
            "X-MBX-APIKEY":KEY
        }

# Data Processing
DEFAULT_PARTITION_COLS = ["date"]
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# File Formats
DELTA_FORMAT = "delta"
PARQUET_FORMAT = "parquet"
ENGINE = 'pyarrow'

# Symbol specific (podría venir de variable de entorno)
SYMBOL = "SOLUSDT"