#!/usr/bin/env python3
"""
Pipeline completo ETL para extracción full de datos de API de Binance.
Ejecuta: extracción -> transformación -> carga
"""

import logging
import pandas as pd
import sys
from datetime import datetime
from pathlib import Path
from config import logging_config

# # Configuracion de paths para importaciones
# root_path = Path(__file__).resolve().parent.parent
# if str(root_path) not in sys.path:
#     sys.path.insert(0, str(root_path))
    
from config.settings import (BINANCE_API, 
                             NOMBRE_COLUMNAS_DESEADAS_KL, 
                             CONVERSION_MAPPING_KL)
from config.paths import (PATH_BRONZE_DELTALAKE_FULL, 
                          PATH_SILVER_DELTALAKE_FULL, 
                          PATH_GOLD_SUMMARIZED_TABLE_FULL, 
                          PATH_GOLD_PIVOT_TABLE_FULL,
                          REPORTS_FULL)
from src.utils import helpers, memory_utils
from src.extract import api_extractor, data_loader
from src.transform import data_cleaning, data_transformation, aggregations
from src.load import delta_writer
from src.quality import profiling

# Configuracion de paths para importaciones
helpers.setup_paths()

# Configuración de logging y creación de carpeta de logs
logging_config.setup_logging('full')

logger = logging.getLogger("pipeline")


def run_full_pipeline():
    """Ejecuta el pipeline completo de ETL"""
    try:
        logger.info("Iniciando pipeline completo")
        start_time = datetime.now()
        
        
        #---------------------------------------
        # 1. EXTRACCIÓN
        #---------------------------------------
        logger.info("Etapa 1: Extracción")
        
        BINANCE_KLINES = BINANCE_API['klines']
        datos = api_extractor.get_data(BINANCE_KLINES['base_url'], 
                                       BINANCE_KLINES['endpoint'], 
                                       params=BINANCE_KLINES['params'], 
                                       headers=BINANCE_KLINES['headers'])
     
        df_raw = data_loader.build_table(datos)
        logger.info(f"Extraídos {len(df_raw)} registros")
        
        
        #---------------------------------------
        # 2. TRANSFORMACIÓN
        #---------------------------------------
        logger.info("Etapa 2: Transformación")
        
        # Modificacion de columnas
        df_clean = data_transformation.renombrar_columnas(df_raw, NOMBRE_COLUMNAS_DESEADAS_KL)
        print(df_clean.head())
        df_clean = data_cleaning.eliminar_columnas(df_clean, ['ignore'])
        
        # Verifico tipos de datos y espacio en memoria
        memory_utils.mostrar_espacio_en_memoria_df(df_clean) 
        
        # Limpieza de registros duplicados y con valores nulos
        df_clean = data_cleaning.eliminar_duplicados(df_clean, subset=['open_time'])
        df_clean = data_cleaning.eliminar_registros_nulos(df_clean, ['open_time', 'close_time'])
        
        # Casteo de tipos de dato
        df_clean[['open_time', 'close_time']] = data_transformation.convertir_milisegundos_a_datetime(df_clean, ['open_time', 'close_time'])
        df_clean = data_transformation.castear_tipos_de_dato(df_clean, CONVERSION_MAPPING_KL)
        
        # Verifico tipos de datos y espacio en memoria
        memory_utils.mostrar_espacio_en_memoria_df(df_clean)
        
        logger.info(f"Transformados {len(df_clean)} registros")
        print(df_clean.head())
        
        
        #---------------------------------------
        # 3. SUMARIZACION
        #---------------------------------------
        logger.info("Etapa 3: Sumarizacion")
        
        # Separo la columna open_time en year y month para agrupar
        df_clean[["year", "month"]] = df_clean["open_time"].apply(lambda x: pd.Series([x.year, x.month]))
        
        # Sumarización de data frame
        by_col = ['year', 'month']
        agg_col = {'low':'mean','high':'mean','volume':'mean','num_trades':'sum'}
        rename_cols = {'low':'avg_low_price','high':'avg_high_price','volume':'avg_volume','num_trades':'total_trades'}

        # Crear cuadro con agregaciones para análisis
        df_summarized = aggregations.sumarizar_df(df_clean, by_col, agg_col, rename_cols)

        # Agrego a la tabla una columna con el total de trades por año
        df_summarized["total_trades_per_year"] = (df_summarized.groupby(level=0)["total_trades"].transform("sum"))   
        print(df_summarized.head())
    
        # Tabla pivote adicional
        df_pivot = pd.pivot_table(
        df_clean,
        values="num_trades", # Columna donde se aplican las agregaciones
        index=["year", "month"], # GROUP BY
        aggfunc=["min", "max","sum"] # Tipos de agregaciones a aplicar sobre num_trades
        )
        print(df_pivot.head())
        
        
        #---------------------------------------
        # 4. CARGA
        #---------------------------------------
        logger.info("Etapa 4: Carga")
        
        
        # Guardar en bronze (datos crudos)
        delta_writer.save_data_as_delta(df_raw, PATH_BRONZE_DELTALAKE_FULL / f"{BINANCE_KLINES['params']['symbol']}_raw")
        
        # Guardar en silver (datos procesados)
        delta_writer.save_data_as_delta(df_clean, PATH_SILVER_DELTALAKE_FULL / f"{BINANCE_KLINES['params']['symbol']}_clean")
        
        # Guardar en gold (datos agregados)
        delta_writer.save_data_as_delta(df_summarized, PATH_GOLD_SUMMARIZED_TABLE_FULL / f"{BINANCE_KLINES['params']['symbol']}_agg")
        
        # Guardar en gold (tabla pivote)
        delta_writer.save_data_as_delta(df_pivot, PATH_GOLD_PIVOT_TABLE_FULL / f"{BINANCE_KLINES['params']['symbol']}_pivot")
        
        
        #---------------------------------------
        # 5. QUALITY CHECK
        #---------------------------------------
        logger.info("Etapa 5: Control de calidad")
        
        report = profiling.generar_profiling_report(df_clean)
        report_path = REPORTS_FULL /f"profile_{BINANCE_KLINES['params']['symbol']}_{start_time.strftime('%Y%m%d_%H%M%S')}.html"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_file(report_path)


        #---------------------------------------
        # Métricas finales
        #---------------------------------------
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Pipeline completado en {duration:.2f} segundos")
        logger.info(f"Registros procesados: {len(df_clean)}")
        logger.info(f"Reporte guardado en: {report_path}")
        
    except Exception as e:
        logger.error(f"Error en el pipeline: {e}")
        raise

if __name__ == "__main__":
    run_full_pipeline()
    