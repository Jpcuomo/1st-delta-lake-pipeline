import pandas as pd
import logging
from src.extract import api_extractor, data_loader
from config.paths import INCREMENTAL_DIR
from config.settings import BINANCE_API_CONFIG


logger = logging.getLogger('pipeline')

def extract_from_api() -> pd.DataFrame:
    """
    Extract incremental data from the Binance API in multiple batches.
    
    Returns:
        pd.DataFrame: DataFrame with the concatenation of all extracted batches.
    """
    
    # Retrieving data from the API
    batch_list = []
    BINANCE_HIST_TRADES = BINANCE_API_CONFIG['historical_trades']
    batch_qtty = BINANCE_HIST_TRADES.get('batch_qtty', 1)
    records = BINANCE_HIST_TRADES['params']['limit']
    
    for i in range(batch_qtty):
        logger.debug(f'Extracting batch n° {i+1} with {records} records')
        datos = api_extractor.get_incremental_extraction(INCREMENTAL_DIR, 
                                    BINANCE_HIST_TRADES['base_url'], 
                                    BINANCE_HIST_TRADES['endpoint'], 
                                    params=BINANCE_HIST_TRADES['params'], 
                                    headers=BINANCE_HIST_TRADES['headers'])
        # Conversion of extracted data to Data Frame
        df = data_loader.build_table(datos)
        batch_list.append(df)
        
    logger.info(f'Batches extracted: {batch_qtty} (total rows: {len(df) * batch_qtty}')
    df_raw = pd.concat(batch_list, ignore_index=True)
    
    return df_raw
    