#!/usr/bin/env python3
"""
Pipeline completo ETL para datos de Binance.
Ejecuta: extracción → transformación → carga
"""

import logging
from datetime import datetime
from pathlib import Path
from src.utils.helpers import setup_paths


setup_paths()

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("pipeline")

def run_full_pipeline():
    """Ejecuta el pipeline completo de ETL"""
    try:
        logger.info("🚀 Iniciando pipeline completo")
        start_time = datetime.now()
        
        # 1. EXTRACCIÓN
        logger.info("📥 Etapa 1: Extracción")
        from src.extract.api_extractor import get_data
        from src.extract.data_loader import build_table
        from config import BINANCE_BASE_URL, SYMBOL, ENDPOINT, PARAMS, HEADERS
        
        datos = get_data(BINANCE_BASE_URL, endpoint=ENDPOINT, params=PARAMS, headers=HEADERS)
     
        df_raw = build_table(datos)
        logger.info(f"✅ Extraídos {len(df_raw)} registros")
        
        
        # 2. TRANSFORMACIÓN
        logger.info("🔄 Etapa 2: Transformación")
        from src.transform.data_cleaning import eliminar_duplicados, eliminar_registros_nulos
        from src.transform.data_transformation import convertir_milisegundos_a_datetime, renombrar_columnas
        from config.constants import COLS
        
        df_clean = renombrar_columnas(df_raw, COLS)
        print(df_clean.to_string(index=False))
        # df_clean = eliminar_duplicados(df_raw, subset=['id'])
        # df_clean = eliminar_registros_nulos(df_clean, ['open_time', 'close_time'])
        # df_clean = convertir_milisegundos_a_datetime(df_clean, ['open_time', 'close_time'])
        logger.info(f"✅ Transformados {len(df_clean)} registros")
        
        # 3. CARGA
        logger.info("💾 Etapa 3: Carga")
        from src.load.delta_writer import save_data_as_delta
        from config import BRONZE_DIR, SILVER_DIR
        
        # Guardar en bronze (datos crudos)
        save_data_as_delta(df_raw, BRONZE_DIR / f"{SYMBOL}_raw")
        
        # Guardar en silver (datos procesados)
        save_data_as_delta(df_clean, SILVER_DIR / f"{SYMBOL}_clean")
        
        # 4. QUALITY CHECK
        logger.info("🔍 Etapa 4: Control de calidad")
        from src.quality.profiling import generar_profiling_report
        
        report = generar_profiling_report(df_clean)
        report_path = Path("reports") / f"profile_{SYMBOL}_{start_time.strftime('%Y%m%d_%H%M%S')}.html"
        report_path.parent.mkdir(exist_ok=True)
        report.to_file(report_path)
        
        # Métricas finales
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"🎉 Pipeline completado en {duration:.2f} segundos")
        logger.info(f"📊 Registros procesados: {len(df_clean)}")
        logger.info(f"📁 Reporte guardado en: {report_path}")
        
    except Exception as e:
        logger.error(f"❌ Error en el pipeline: {e}")
        raise

if __name__ == "__main__":
    run_full_pipeline()