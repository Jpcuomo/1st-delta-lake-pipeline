import pandas as pd
import logging
from src.extract.api_extractor import get_data_incremental
from src.extract.data_loader import build_table
from config.paths import INCREMENTAL_DIR
from config.settings import BINANCE_API


logger = logging.getLogger('pipeline')

def extract_from_api() -> pd.DataFrame:
    """
    Extrae datos incrementales desde la API de Binance en múltiples lotes.
    
    Returns:
        pd.DataFrame: DataFrame con la concatenación de todos los lotes extraídos.
    """
    # Trayendo datos desde la API
    BINANCE_HIST_TRADES = BINANCE_API['historical_trades']
    print(BINANCE_HIST_TRADES)
    batch_list = []
    batch_qtty = BINANCE_HIST_TRADES.get('batch_qtty', 1)
    registros = BINANCE_HIST_TRADES['params']['limit']
    
    for i in range(batch_qtty):
        logger.info(f'Extrayendo lote n°{i+1} con {registros} registros')
        datos = get_data_incremental(INCREMENTAL_DIR, 
                                    BINANCE_HIST_TRADES['base_url'], 
                                    BINANCE_HIST_TRADES['endpoint'], 
                                    params=BINANCE_HIST_TRADES['params'], 
                                    headers=BINANCE_HIST_TRADES['headers'])
        # Conversión a Data Frame de los datos extraidos
        df = build_table(datos)
        batch_list.append(df)
    logger.info(f'Se extrajeron {batch_qtty} lotes')
    df_raw = pd.concat(batch_list, ignore_index=True)
    return df_raw
    