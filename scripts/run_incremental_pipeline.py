#!user/bin/env python3
"""
Pipeline completo ETL para extracción incremental de API de Binance
Realiza extracción -> transformación -> carga -> quality testing
"""

import logging
import pandas as pd
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
        
        # Conversión a Data Frame de los datos extraidos
        df_raw = build_table(datos)
        metricas['filas_extraidas'] = len(df_raw)
        logger.info(f'Filas extraidas: {metricas['filas_extraidas']}')
        
        
        #-----------------------------------------------------------------------------------------
        logger.info('Etapa 2: Transformación')
        
        from src.transform import contar_registros_nulos, ordenar_dataframe, eliminar_duplicados, renombrar_columnas, convertir_milisegundos_a_datetime, castear_tipos_de_dato, cambiar_posicion_de_columna
        from src.utils.memory_utils import mostrar_espacio_en_memoria_df
        from config.binance_hist_trading_settings import NOMBRE_COLUMNAS_DESEADAS, CONVERSION_MAPPING
        
        # Verificación de tipos de datos y espacio en memoria antes de iniciar transformaciones.
        mostrar_espacio_en_memoria_df(df_raw)
        
        # Contar registros nulos por columna
        cols = df_raw.columns
        contar_registros_nulos(df_raw, cols)
        
        #Ordeno por 'id' y elimino duplicados de haberlos
        df_clean = ordenar_dataframe(df_raw, sort_by='id')
        
        # Eliminación de registros duplicados
        df_clean = eliminar_duplicados(df_clean)
        print(f'Cantidad de filas: {df_clean.shape[0]}')
        
        # Renombro columnas
        df_clean = renombrar_columnas(df_clean, NOMBRE_COLUMNAS_DESEADAS)
        
        # Convierto los 'milisegundos' a datetime y los asigno a la columna auxiliar 'time'
        df_clean['time'] = convertir_milisegundos_a_datetime(df_clean, ['miliseconds'])
        
        # Creo la columna 'date' con formato dd/mm/yy a partir de 'time'
        df_clean["date"] = pd.to_datetime(df_clean["time"], unit="ms").dt.date

        # Creo la columna 'hour' a partir de 'time'. 
        df_clean['hour'] = pd.to_datetime(df_clean['time'].dt.strftime('%H:%M:%S.%f'), format="%H:%M:%S.%f")
        
        # Extraer solo la hora (0–23) como int
        df_clean["hr"] = pd.to_datetime(df_clean["time"], unit="ms").dt.hour.astype("int32")
        
        # Elimino la columna auxiliar 'time', ya que no es necesaria
        # Elimino la columna 'is_best_match' ya que es siempre True y no suma al análisis
        df_clean = df_clean.drop(columns=['time','isBestMatch'])
        
        # Casteo de datos numéricos tipo object a float32 para hacer agregaciones futuras
        df_clean = castear_tipos_de_dato(df_clean, CONVERSION_MAPPING)
        
        # Cambio la posición de las columnas para presentar de forma más prolija
        df_clean = cambiar_posicion_de_columna(df_clean, 'date', 'is_buyer_maker')
        df_clean = cambiar_posicion_de_columna(df_clean, 'hour', 'is_buyer_maker')
        
        # Verificación de tipos de datos y espacio en memoria luego de iniciar transformaciones.
        mostrar_espacio_en_memoria_df(df_clean)
        
        
        #-----------------------------------------------------------------------------------------
        end_time = datetime.now()
        metricas['end_time'] = end_time
        
        duration = (end_time - start_time).total_seconds()
        logger.info(f'Pipeline completado en: {duration:.2f} segundos')
        
    except Exception as e:
        logger.error(f'No se pudo correr el pipeline: {e}')
        
    
if __name__=='__main__':
    run_incremental_pipeline()