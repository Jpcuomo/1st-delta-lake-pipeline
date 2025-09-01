import pandas as pd
from config.settings import KEY


#--------------------------------------------------------------------------------
# API Settings
BINANCE_KLINES = {
    'base_url':'https://api.binance.com',
    'endpoint':'api/v3/klines',
    'params':{'symbol':'symbol',
        'interval':'interval',
        'startTime':'startTime',
        'endTime':'endTime',
        'limit':'limit'},
    'headers':{"X-MBX-APIKEY":KEY}
}

# Defino fecha y rango horario a filtrar
fecha_inicial = "2024-07-31 00:00:00Z"
fecha_final = "2025-07-31 23:59:59Z"
    
# Configuración de parámetros
BINANCE_KLINES['params']['symbol'] = 'SOLUSDT'
BINANCE_KLINES['params']['interval'] = '1d'
BINANCE_KLINES['params']['startTime'] = int(pd.Timestamp(fecha_inicial).timestamp() * 1000) # Convierto a milisegundos
BINANCE_KLINES['params']['endTime'] = int(pd.Timestamp(fecha_final).timestamp() * 1000) # Convierto a milisegundos
BINANCE_KLINES['params']['limit'] = 1000