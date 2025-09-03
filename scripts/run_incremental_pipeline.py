#!user/bin/env python3
"""
Pipeline completo ETL para extracción incremental de API de Binance
Realiza extracción -> transformación -> carga -> quality testing
"""

import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from src.utils.helpers import setup_paths
from config.logging_config import setup_logging
from src.utils.file_utils import crear_archivo_incremental
from config.binance_hist_trading_settings import CONTENIDO_INCREMENTAL, BINANCE_HIST_TRADES
from config.paths import ARCHIVO_INCREMENTAL, CARPETA_INCREMENTAL, INCREMENTAL_DIR


# Configuracion de paths para importaciones
setup_paths()

# Configuracion de logging
setup_logging('incremental')
logger = logging.getLogger('pipeline')

# Creación de archivo json con metadata
if not os.path.exists(INCREMENTAL_DIR): # Esta línea evita que se reinicie a 0 en cada ejecución
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
        
        # Importación de módulos
        from config.paths import PATH_BRONZE_DELTALAKE_INCREMENTAL, PATH_SILVER_DELTALAKE_INCREMENTAL, PATH_GOLD_SUMMARIZED_TABLE_INCREMENTAL
        from src.load.delta_writer import save_new_data_as_delta, leer_extraccion_reciente, save_data_as_delta
        from src.extract.incremental_extraction import extract_from_api
        
        # Extracción de datos de la API
        df_raw = extract_from_api(extraction_limit=1000, batch_qtty=5)        
        
        # Guardo el DataFrame en formato Delta Lake
        # Como los campos nuevos son siempre distintos utilizo un MERGE sin UPDATE
        save_new_data_as_delta(df_raw, PATH_BRONZE_DELTALAKE_INCREMENTAL, 'src.id = tgt.id')
        logger.info('Datos cargados en capa bronze exitosamente.')
        
        # Traigo solo los valores de la última extracción para el procesamiento
        df_raw = leer_extraccion_reciente(PATH_BRONZE_DELTALAKE_INCREMENTAL, INCREMENTAL_DIR)
        
        metricas['filas_extraidas'] = len(df_raw)
        min_id = df_raw['id'].min()
        max_id = df_raw['id'].max()
        metricas['min_id'] = min_id
        metricas['max_id'] = max_id
        
        logger.info(f'Filas extraidas: {metricas['filas_extraidas']}')
        logger.info(f'Filas extraidas desde id {metricas['min_id']} hasta id {metricas['max_id']}')
        print(metricas)
        
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
        
        # Ordeno por 'id' y elimino duplicados de haberlos
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
        
        
        # -----------------------------------------------------------------------------------------
        logger.info('Etapa 3: Sumarización')
        
        group_by_cols = ['date','hr','is_buyer_maker'] # Lista de columnas por las que agrupar

        agg_dict = {# Diccionario con columnas como clave y agregaciones como valor
            'price':'mean',      # promedio de la columna 'price'
            'quantity':'sum',    # promedio de la columna 'quantity'
            'quote_qty':'sum',   # Sumatoria de cantidades
            'id':'count'         # cuenta de registros por grupo
        }

        rename_cols = {# Diccionario con los cambios de nombres para las columnas agregadas
            'price':'mean_price',
            'quantity':'qty_per_hour',
            'quote_qty':'total_quote_qty',
            'id':'id_count'
        }

        from src.transform.aggregations import sumarizar_df
        # Asigna el DF sumarizado a un nuevo DF
        df_sumarizado = sumarizar_df(df_clean, group_by_cols, agg_dict, rename_cols)

        # Redondeo para presentación
        cols = ['mean_price', 'qty_per_hour', 'total_quote_qty']
        df_sumarizado[cols] = df_sumarizado[cols].round(3).map("{:.3f}".format)

        df_sumarizado.head()
        
        
        # -----------------------------------------------------------------------------------------
        logger.info('Etapa 4: Carga')
        
        # Guardo el DF en la capa silver, particionando por fecha y hora.
        save_new_data_as_delta(df_clean, PATH_SILVER_DELTALAKE_INCREMENTAL, 'src.id = tgt.id', ['date','hr'])
        logger.info('Datos cargados en capa silver exitosamente.')
        
        # Guardado del DF en formato Delta Lake, en modo 'overwrite' por defecto
        # Este modo sobreescribe los cambios, pero permite consultar registro histórico
        save_data_as_delta(df_sumarizado, PATH_GOLD_SUMMARIZED_TABLE_INCREMENTAL)
        logger.info('Datos cargados en capa gold exitosamente.')
        
        
        #-------------------------------------------------------------------------------------
        # 5. QUALITY CHECK
        logger.info("Etapa 5: Control de calidad")
        from src.quality.profiling import generar_profiling_report
        
        report = generar_profiling_report(df_clean)
        report_path = Path("reports") / "incremental" / f"profile_{BINANCE_HIST_TRADES['params']['symbol']}_{start_time.strftime('%Y%m%d_%H%M%S')}.html"
        report_path.parent.mkdir(exist_ok=True)
        report.to_file(report_path)

        # -----------------------------------------------------------------------------------------
        # Métricas finales
        end_time = datetime.now()
        metricas['end_time'] = end_time
        
        duration = (end_time - start_time).total_seconds()
        logger.info(f'Pipeline completado en: {duration:.2f} segundos')
        logger.info(f"Registros procesados: {len(df_clean)}")
        logger.info(f"Reporte guardado en: {report_path}")
    except Exception as e:
        logger.error(f'No se pudo correr el pipeline: {e}')
        raise
    
if __name__=='__main__':
    run_incremental_pipeline()