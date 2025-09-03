import pandas as pd
from src.extract.api_extractor import get_data_incremental
from config.paths import INCREMENTAL_DIR
from config.binance_hist_trading_settings import BINANCE_HIST_TRADES
from src.extract.data_loader import build_table


def extract_from_api(extraction_limit=1000, batch_qtty=None):
    """Extrae la cantidad de lotes pasados como parametro"""
    
    # Trayendo datos desde la API
    BINANCE_HIST_TRADES['params']['limit'] = extraction_limit
    
    batch_list = []
    for _ in range(batch_qtty or 1):
        datos = get_data_incremental(INCREMENTAL_DIR, 
                                    BINANCE_HIST_TRADES['base_url'], 
                                    BINANCE_HIST_TRADES['endpoint'], 
                                    params=BINANCE_HIST_TRADES['params'], 
                                    headers=BINANCE_HIST_TRADES['headers'])
        # Conversión a Data Frame de los datos extraidos
        df = build_table(datos)
        batch_list.append(df)
    df_raw = pd.concat(batch_list, ignore_index=True)
    return df_raw
    