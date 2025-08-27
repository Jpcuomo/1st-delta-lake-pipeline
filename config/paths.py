"""
Path configuration for Binance API data pipeline.
Usa pathlib para mejor manejo de rutas entre sistemas.
"""

from pathlib import Path


# Directorio base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Configuración
API_AUTH_PATH = BASE_DIR / "config" / "api_auth.conf"

# Metadatos
PATH_ARCHIVO_INCREMENTAL = BASE_DIR / "metadata" / "incremental.json"

# Capa Bronze - Datos crudos
BRONZE_DIR = BASE_DIR / "data" / "bronze"
PATH_BRONZE_DELTALAKE_FULL = BRONZE_DIR / "api_binance" / "klines" / "sol_usdt"
PATH_BRONZE_DELTALAKE_INCREMENTAL = BRONZE_DIR / "api_binance" / "historicalTrades" / "sol_usdt"

# Capa Silver - Datos limpios
SILVER_DIR = BASE_DIR / "data" / "silver"
PATH_SILVER_DELTALAKE_FULL = SILVER_DIR / "api_binance" / "klines" / "sol_usdt"
PATH_SILVER_DELTALAKE_INCREMENTAL = SILVER_DIR / "api_binance" / "historicalTrades" / "sol_usdt"

# Capa Gold - Datos enriquecidos
GOLD_DIR = BASE_DIR / "data" / "gold"
PATH_GOLD_SUMARIZED_TABLE_INCREMENTAL = GOLD_DIR / "api_binance" / "historicalTrades" / "sumarized_table" / "sol_usdt"
PATH_GOLD_SUMARIZED_TABLE_FULL = GOLD_DIR / "api_binance" / "klines" / "sumarized_table" / "sol_usdt"
PATH_GOLD_PIVOT_TABLE_FULL = GOLD_DIR / "api_binance" / "klines" / "pivot_table" / "sol_usdt"

# Crear directorios si no existen
for path in [
    BRONZE_DIR, 
    SILVER_DIR, 
    GOLD_DIR,
    PATH_BRONZE_DELTALAKE_FULL, 
    PATH_BRONZE_DELTALAKE_INCREMENTAL,
    PATH_SILVER_DELTALAKE_FULL, 
    PATH_SILVER_DELTALAKE_INCREMENTAL,
    PATH_GOLD_SUMARIZED_TABLE_INCREMENTAL, 
    PATH_GOLD_SUMARIZED_TABLE_FULL,
    PATH_GOLD_PIVOT_TABLE_FULL
]:
    path.mkdir(parents=True, exist_ok=True)