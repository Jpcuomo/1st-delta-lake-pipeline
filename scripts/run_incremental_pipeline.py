#!user/bin/env python3
"""
Pipeline completo ETL para extracción incremental de API de Binance
Realiza extracción -> transformación -> carga -> quality testing
"""

import logging
from datetime import datetime
from src.utils.helpers import setup_paths
from src.utils.file_utils import crear_archivo_incremental, obtener_archivo_incremental
from config.logging_config import setup_logging
from config.binance_hist_trading_settings import CONTENIDO_INCREMENTAL
from config.paths import ARCHIVO_INCREMENTAL, CARPETA_INCREMENTAL, INCREMENTAL_DIR

# Configuracion de paths para importaciones
setup_paths()

# Congiguracion de logging
setup_logging('incremental')
logger = logging.getLogger('pipeline')

# Creación de archivo json con metadata
if CONTENIDO_INCREMENTAL['valor_previo'] == 0: # Esta línea evita que se reinicie a 0 en cada ejecución
    crear_archivo_incremental(CONTENIDO_INCREMENTAL, ARCHIVO_INCREMENTAL, CARPETA_INCREMENTAL)


def run_incremental_pipeline():
    """Ejecuta el pipeline ETL completo"""
    try:
        logger.info('Iniciando pipeline completo.')
        
        # Inicia conteo del tiempo de ejecución
        start_time = datetime.now()
        metricas = {'start_time':start_time}
        
        #-----------------------------------------------------------------------------------------
        logger.info('Etapa 1: Extracción')
        
        from src.extract.api_extractor import get_data_incremental
        from config.paths import INCREMENTAL_DIR
        from config.binance_hist_trading_settings import BINANCE_HIST_TRADES
        
        # Trayendo datos desde la API
        datos = get_data_incremental(INCREMENTAL_DIR, 
                                     BINANCE_HIST_TRADES['base_url'], 
                                     BINANCE_HIST_TRADES['endpoint'], 
                                     params=BINANCE_HIST_TRADES['params'], 
                                     headers=BINANCE_HIST_TRADES['headers'])
        
        from src.extract.data_loader import build_table
        df_raw = build_table(datos)
        print(df_raw.head())
        
        
        end_time = datetime.now()
        metricas['end_time'] = end_time
        
        duration = (end_time - start_time).total_seconds()
        logger.info(f'Pipeline completado en: {duration:.2f} segundos')
        
    except Exception as e:
        logger.error(f'No se pudo correr el pipeline: {e}')
        
    
    
    
    
if __name__=='__main__':
    run_incremental_pipeline()