#!usr/bin/env python3
"""
Pipeline completo ETL para extracción incremental de API de Binance
Realiza extracción -> transformación -> carga -> quality testing
"""

import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

from config.settings import (CONTENIDO_INCREMENTAL, 
                            NOMBRE_COLUMNAS_DESEADAS_HT, 
                            CONVERSION_MAPPING_HT, 
                            BINANCE_API)
from config.paths import (ARCHIVO_INCREMENTAL, 
                          CARPETA_INCREMENTAL, 
                          INCREMENTAL_DIR, 
                          PATH_BRONZE_DELTALAKE_INCREMENTAL, 
                          PATH_SILVER_DELTALAKE_INCREMENTAL, 
                          PATH_GOLD_SUMMARIZED_TABLE_INCREMENTAL)
from config.logging_config import setup_logging
from src.utils import helpers, file_utils, memory_utils
from src.extract import incremental_extraction as extr
from src.transform import data_cleaning as clean, data_transformation as dtrans, aggregations
from src.load import delta_writer


# Configuracion de paths para importaciones
helpers.setup_paths()

# Configuracion de logging
setup_logging('incremental')
logger = logging.getLogger('pipeline')

# Creación de archivo json con metadata
if not os.path.exists(INCREMENTAL_DIR): # Esta línea evita que se reinicie a 0 en cada ejecución
    file_utils.crear_archivo_incremental(CONTENIDO_INCREMENTAL, 
                                         ARCHIVO_INCREMENTAL, 
                                         CARPETA_INCREMENTAL)


#----------------------------------------------
# ORQUESTACION DEL PIPELINE
#----------------------------------------------
def run_incremental_pipeline():
    """Ejecuta el pipeline ETL completo"""
    try:
        logger.info('---------------------------------------------------------------------------------------')
        logger.info('Iniciando pipeline completo.')
        
        # Inicia conteo del tiempo de ejecución
        start_time = datetime.now()
        metricas = {'start_time':start_time}
        
        
        #----------------------------------------------
        # 1. EXTRACCION
        #----------------------------------------------
        logger.info('Etapa 1: Extracción')        
        
        # Extracción de datos de la API
        df_raw = extr.extract_from_api()
        
        # Guardo el DataFrame en formato Delta Lake
        # Como los campos nuevos son siempre distintos utilizo un MERGE sin UPDATE
        delta_writer.save_new_data_as_delta(df_raw, PATH_BRONZE_DELTALAKE_INCREMENTAL, 'src.id = tgt.id')
        logger.info('Datos cargados en capa bronze exitosamente.')
        
        # Traigo solo los valores de la última extracción para el procesamiento
        df_raw = delta_writer.leer_extraccion_reciente(PATH_BRONZE_DELTALAKE_INCREMENTAL, INCREMENTAL_DIR)
        
        metricas['filas_extraidas'] = len(df_raw)
        min_id = df_raw['id'].min()
        max_id = df_raw['id'].max()
        metricas['min_id'] = min_id
        metricas['max_id'] = max_id
        
        logger.info(f"Filas extraidas: {metricas['filas_extraidas']}")
        logger.info(f"Filas extraidas desde id {metricas['min_id']} hasta id {metricas['max_id']}")

        
        #----------------------------------------------
        # 2. TRANSFORMACION
        #----------------------------------------------
        logger.info('Etapa 2: Transformación')
            
        # Verificación de tipos de datos y espacio en memoria antes de iniciar transformaciones.
        memory_utils.mostrar_espacio_en_memoria_df(df_raw)
        
        # Contar registros nulos por columna
        cols = df_raw.columns
        clean.contar_registros_nulos(df_raw, cols)
        
        # Ordeno por 'id' y elimino duplicados de haberlos
        df_clean = dtrans.ordenar_dataframe(df_raw, sort_by='id')
        
        # Eliminación de registros duplicados
        df_clean = clean.eliminar_duplicados(df_clean)
        print(f'Cantidad de filas: {df_clean.shape[0]}')
        
        # Renombro columnas
        df_clean = dtrans.renombrar_columnas(df_clean, NOMBRE_COLUMNAS_DESEADAS_HT)
        
        # Convierto los 'milisegundos' a datetime y los asigno a la columna auxiliar 'time'
        df_clean['time'] = dtrans.convertir_milisegundos_a_datetime(df_clean, ['miliseconds'])
        
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
        df_clean = dtrans.castear_tipos_de_dato(df_clean, CONVERSION_MAPPING_HT)
        
        # Cambio la posición de las columnas para presentar de forma más prolija
        df_clean = dtrans.cambiar_posicion_de_columna(df_clean, 'date', 'is_buyer_maker')
        df_clean = dtrans.cambiar_posicion_de_columna(df_clean, 'hour', 'is_buyer_maker')
        
        # Verificación de tipos de datos y espacio en memoria luego de iniciar transformaciones.
        memory_utils.mostrar_espacio_en_memoria_df(df_clean)
        
        logger.info(f'Se limpiaron/ transformaron {len(df_clean)} registros')
        
        
        #----------------------------------------------
        # 3. SUMARIZACION
        #----------------------------------------------
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
        
        # Asigna el DF sumarizado a un nuevo DF
        df_sumarizado = aggregations.sumarizar_df(df_clean, group_by_cols, agg_dict, rename_cols)

        # Redondeo para presentación
        cols = ['mean_price', 'qty_per_hour', 'total_quote_qty']
        
        if not df_sumarizado.empty or None:
            df_sumarizado[cols] = df_sumarizado[cols].round(3).map("{:.3f}".format)
        else:
            logger.error(f'Error al crear "df_sumarizado"')

        logger.debug(df_sumarizado.head().to_string())
        
        
        #----------------------------------------------
        # 4. CARGA
        #----------------------------------------------
        logger.info('Etapa 4: Carga')
        
        # Guardo el DF en la capa silver, particionando por fecha y hora.
        BINANCE_HIST_TRADES = BINANCE_API['historical_trades']
        delta_writer.save_new_data_as_delta(df_clean, PATH_SILVER_DELTALAKE_INCREMENTAL, 
                                            'src.id = tgt.id', BINANCE_HIST_TRADES['partition_cols'])
        logger.info('Datos cargados en capa silver exitosamente.')
        
        # Guardado del DF en formato Delta Lake, en modo 'overwrite' por defecto
        # Este modo sobreescribe los cambios, pero permite consultar registro histórico
        delta_writer.save_data_as_delta(df_sumarizado, PATH_GOLD_SUMMARIZED_TABLE_INCREMENTAL)
        logger.info('Datos cargados en capa gold exitosamente.')
        
        
        #----------------------------------------------
        # 5. QUALITY CHECK
        #----------------------------------------------
        logger.info("Etapa 5: Control de calidad")
        from src.quality import profiling
        
        report = profiling.generar_profiling_report(df_clean)
        report_path = Path("reports") / "incremental" / f"profile_{BINANCE_HIST_TRADES['params']['symbol']}_{start_time.strftime('%Y%m%d_%H%M%S')}.html"
        report_path.parent.mkdir(exist_ok=True)
        report.to_file(report_path)


        #----------------------------------------------
        # METRICAS FINALES
        #----------------------------------------------
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