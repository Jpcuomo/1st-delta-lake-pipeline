#!/usr/bin/env python3
"""
Complete ETL pipeline for full data extraction from Binance API.
Executes: extraction -> transformation -> loading
"""

import logging
import pandas as pd
from datetime import datetime
from config import logging_config
    
from config.settings import (BINANCE_API_CONFIG, 
                             DESIRED_COLS_KL, 
                             CONVERSION_MAPPING_KL)
from config.paths import (PATH_BRONZE_DELTALAKE_FULL, 
                          PATH_SILVER_DELTALAKE_FULL, 
                          PATH_GOLD_SUMMARIZED_TABLE_FULL, 
                          PATH_GOLD_PIVOT_TABLE_FULL,
                          REPORTS_FULL)
from src.utils import helpers, memory_utils
from src.extract import api_extractor, data_loader
from src.transform import data_cleaning, data_transformation as dt, aggregations
from src.load import delta_writer
from src.quality import profiling

# Configuring paths for imports
helpers.setup_paths()

# Logging configuration and log folder creation
logging_config.logging_setup('full')

logger = logging.getLogger("pipeline")


def run_full_pipeline():
    """Run the complete ETL pipeline"""
    try:
        logger.info("Starting full pipeline")
        start_time = datetime.now()
        
        
        #---------------------------------------
        # 1. EXTRACT
        #---------------------------------------
        logger.info("""
                    -------------------------
                    Stage 1: Extraction
                    -------------------------""")
        
        BINANCE_KLINES = BINANCE_API_CONFIG['klines']
        datos = api_extractor.get_data(BINANCE_KLINES['base_url'], 
                                       BINANCE_KLINES['endpoint'], 
                                       params=BINANCE_KLINES['params'], 
                                       headers=BINANCE_KLINES['headers'])
     
        df_raw = data_loader.build_table(datos)
        
        logger.info(f"{len(df_raw)} records extracted")
        
        
        #---------------------------------------
        # 2. TRANSFORM
        #---------------------------------------
        logger.info("Stage 2: Transformation")
        
        # Column modification
        df_clean = dt.rename_columns(df_raw, DESIRED_COLS_KL)
        print(df_clean.head())
        df_clean = data_cleaning.delete_columns(df_clean, ['ignore'])
        
        # Check data types and memory usage
        memory_utils.show_df_memory_space(df_clean) 
        
        # Cleaning duplicate records and records with null values
        df_clean = data_cleaning.remove_duplicates(df_clean, subset=['open_time'])
        df_clean = data_cleaning.delete_null_records(df_clean, ['open_time', 'close_time'])
        
        # Data type casting
        df_clean[['open_time', 'close_time']] = dt.convert_milliseconds_to_datetime(df_clean, ['open_time', 'close_time'])
        df_clean = dt.cast_data_types(df_clean, CONVERSION_MAPPING_KL)
        
        # Check data types and memory space.
        memory_utils.show_df_memory_space(df_clean)
        
        logger.info(f"Transformed {len(df_clean)} records")
        print(df_clean.head())
        
        
        #---------------------------------------
        # 3. SUMMARIZE
        #---------------------------------------
        logger.info("Stage 3: Summarization")
        
        # Split the open_time column into year and month to group them.
        df_clean[["year", "month"]] = df_clean["open_time"].apply(lambda x: pd.Series([x.year, x.month]))
        
        # Data frame summarization
        by_col = ['year', 'month']
        agg_col = {'low':'mean','high':'mean','volume':'mean','num_trades':'sum'}
        rename_cols = {'low':'avg_low_price','high':'avg_high_price','volume':'avg_volume','num_trades':'total_trades'}

        # Create table with aggregations for analysis
        df_summarized = aggregations.sumarizar_df(df_clean, by_col, agg_col, rename_cols)

        # Add a column to the table with the total number of trades per year.
        df_summarized["total_trades_per_year"] = (df_summarized.groupby(level=0)["total_trades"].transform("sum"))   
        print(df_summarized.head())
    
        # Additional pivot table
        df_pivot = pd.pivot_table(
        df_clean,
        values="num_trades", # Column where aggregations are applied
        index=["year", "month"], # GROUP BY
        aggfunc=["min", "max","sum"] # Types of aggregations to apply to num_trades
        )
        print(df_pivot.head())
        
        
        #---------------------------------------
        # 4. LOAD
        #---------------------------------------
        logger.info("Stage 4: Loading")
        
        
        # Save in bronze (raw data))
        delta_writer.save_data_as_delta(df_raw, PATH_BRONZE_DELTALAKE_FULL / f"{BINANCE_KLINES['params']['symbol']}_raw")
        
        # Save in silver (processed data)
        delta_writer.save_data_as_delta(df_clean, PATH_SILVER_DELTALAKE_FULL / f"{BINANCE_KLINES['params']['symbol']}_clean")
        
        # Save in gold (aggregated data)
        delta_writer.save_data_as_delta(df_summarized, PATH_GOLD_SUMMARIZED_TABLE_FULL / f"{BINANCE_KLINES['params']['symbol']}_agg")
        
        # Save in gold (pivot table)
        delta_writer.save_data_as_delta(df_pivot, PATH_GOLD_PIVOT_TABLE_FULL / f"{BINANCE_KLINES['params']['symbol']}_pivot")
        
        
        #---------------------------------------
        # 5. QUALITY CHECK
        #---------------------------------------
        logger.info("Stage 5: Quality check")
        
        report = profiling.generate_profiling_report(df_clean)
        report_path = REPORTS_FULL /f"profile_{BINANCE_KLINES['params']['symbol']}_{start_time.strftime('%Y%m%d_%H%M%S')}.html"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_file(report_path)


        #---------------------------------------
        # Final metrics
        #---------------------------------------
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Pipeline completed in {duration:.2f} seconds")
        logger.info(f"Processed records: {len(df_clean)}")
        logger.info(f"Report saved in: {report_path}")
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise

if __name__ == "__main__":
    run_full_pipeline()
    