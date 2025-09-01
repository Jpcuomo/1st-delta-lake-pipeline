"""
Project settings and constants for Binance API pipeline.
"""

from src.utils import leer_archivo_conf
from config.paths import API_AUTH_PATH

# Parámetros de acceso a la API
API_SECCION = 'binance'
API_KEY = leer_archivo_conf(API_AUTH_PATH, API_SECCION)
KEY = API_KEY['clave_api']


