"""
Path configuration for Binance API data pipeline.
Usa pathlib para mejor manejo de rutas entre sistemas.
"""

from pathlib import Path

#----------------------------------------------
# Directorio base del proyecto
#----------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

#----------------------------------------------
# Metadatos
#----------------------------------------------
INCREMENTAL_DIR = Path("metadata") / "incremental.json"
CARPETA_INCREMENTAL = INCREMENTAL_DIR.parent
ARCHIVO_INCREMENTAL = INCREMENTAL_DIR.name

#----------------------------------------------
# Logs
#----------------------------------------------
LOGS_DIR = BASE_DIR / 'logs'
LOGS_INCREMENTAL = LOGS_DIR / 'incremental'
LOGS_FULL = LOGS_DIR / 'full'

#----------------------------------------------
# Reportes
#----------------------------------------------
REPORTS_DIR = BASE_DIR / 'reports'
REPORTS_INCREMENTAL = REPORTS_DIR / 'incremental'
REPORTS_FULL = REPORTS_DIR / 'full'

#----------------------------------------------
# Capa Bronze - Datos crudos
#----------------------------------------------
BRONZE_DIR = BASE_DIR / "data" / "bronze"
PATH_BRONZE_DELTALAKE_FULL = BRONZE_DIR / "api_binance" / "klines" 
PATH_BRONZE_DELTALAKE_INCREMENTAL = BRONZE_DIR / "api_binance" / "historicalTrades" 

#----------------------------------------------
# Capa Silver - Datos limpios
#----------------------------------------------
SILVER_DIR = BASE_DIR / "data" / "silver"
PATH_SILVER_DELTALAKE_FULL = SILVER_DIR / "api_binance" / "klines" 
PATH_SILVER_DELTALAKE_INCREMENTAL = SILVER_DIR / "api_binance" / "historicalTrades" 

#----------------------------------------------
# Capa Gold - Datos enriquecidos
#----------------------------------------------
GOLD_DIR = BASE_DIR / "data" / "gold"
PATH_GOLD_SUMMARIZED_TABLE_INCREMENTAL = GOLD_DIR / "api_binance" / "historicalTrades" / "summarized_table" 
PATH_GOLD_SUMMARIZED_TABLE_FULL = GOLD_DIR / "api_binance" / "klines" / "summarized_table" 
PATH_GOLD_PIVOT_TABLE_FULL = GOLD_DIR / "api_binance" / "klines" / "pivot_table" 
