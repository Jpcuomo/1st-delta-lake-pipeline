#!user/bin/env python3
"""
Pipeline completo ETL para extracción incremental de API de Binance
Realiza extracción -> transformación -> carga -> quality testing
"""

import logging
from config.logging_config import setup_logging
from src.utils.helpers import setup_paths, set_fecha_inicial, set_fecha_final

# Configuracion de paths para importaciones
setup_paths()

# Congiguracion de logging
setup_logging('incremental')
logger = logging.getLogger('pipeline')


def run_incrmental_pipeline():
    """Ejecuta el pipeline ETL completo"""
    pass
    
    
    
    
if __name__=='__main__':
    run_incrmental_pipeline()