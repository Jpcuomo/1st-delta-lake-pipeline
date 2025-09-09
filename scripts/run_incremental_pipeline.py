#!usr/bin/env python3
"""
Complete ETL pipeline for incremental extraction of Binance API
Performs extraction -> transformation -> loading -> quality testing
"""

import os
import logging
import pandas as pd
from datetime import datetime

from config.settings import (INCREMENTAL_CONTENT, 
                            DESIRED_COLS_HT, 
                            CONVERSION_MAPPING_HT, 
                            BINANCE_API_CONFIG, LIMIT, SYMBOL)
from config.paths import (INCREMENTAL_FILE, 
                          INCREMENTAL_FOLDER, 
                          INCREMENTAL_DIR, 
                          PATH_BRONZE_DELTALAKE_INCREMENTAL, 
                          PATH_SILVER_DELTALAKE_INCREMENTAL, 
                          PATH_GOLD_SUMMARIZED_TABLE_INCREMENTAL,
                          REPORTS_INCREMENTAL)
from config.logging_config import logging_setup
from src.utils import helpers, file_utils, memory_utils
from src.extract import incremental_extraction as extr
from src.transform import data_cleaning as dc, data_transformation as dt, aggregations
from src.load import delta_writer

# Configuring paths for imports
helpers.setup_paths()

# Logging configuration and log folder creation
correlation_id = logging_setup('incremental')
logger = logging.getLogger('pipeline')

# Creation of json file with metadata
if not os.path.exists(INCREMENTAL_DIR): # This line prevents it from resetting to 0 on each execution.
    file_utils.create_incremental_file(INCREMENTAL_CONTENT, 
                                         INCREMENTAL_FILE, 
                                         INCREMENTAL_FOLDER)

#----------------------------------------------
# PIPELINE ORCHESTRATION
#----------------------------------------------
def run_incremental_pipeline():
    """Runs the entire ETL pipeline"""
    try:
        # Start counting execution time
        start_time = datetime.now()
        metrics = {'start_time':start_time}
        
        logger.info("""
                    -----------------------------
                    Starting complete pipeline
                    -----------------------------""")  

        #----------------------------------------------
        # 1. EXTRACTION
        #----------------------------------------------
        logger.info('----- Stage 1: Extraction -----')        
        logger.info(f'Processing symbol: {SYMBOL}')
        
        # API data extraction
        df_raw = extr.extract_from_api()
        
        # Save the DataFrame in Delta Lake format.
        # Since the new fields retrieved from the API are always different, I use a MERGE without UPDATE.
        delta_writer.save_new_data_as_delta(df_raw, PATH_BRONZE_DELTALAKE_INCREMENTAL, 'src.id = tgt.id')
        logger.info('Data successfully uploaded to bronze layer.')
        
        # Only bring the values from the last extraction for processing.
        df_raw = delta_writer.read_recent_extraction(PATH_BRONZE_DELTALAKE_INCREMENTAL, INCREMENTAL_DIR)
        print(df_raw.head())
        
        metrics['extracted_files'] = len(df_raw)
        min_id = df_raw['id'].min()
        max_id = df_raw['id'].max()
        metrics['min_id'] = min_id
        metrics['max_id'] = max_id
        
        if metrics['extracted_files'] < LIMIT:
             logger.warning(f'Warning! {metrics['extracted_files']} were extracted instead of {LIMIT}. Data might be axhausted')
                
        logger.debug(f"Extracted rows: {metrics['extracted_files']}")
        logger.debug(f"Rows extracted from id {metrics['min_id']} to id {metrics['max_id']}")

        # Check memory size of data frame
        mem_size = df_raw.memory_usage(deep=True).sum()
        metrics['raw_data_size'] = mem_size / 1024
        logger.info(f'Raw data frame size: {metrics['raw_data_size']:.2f} MB')
        
        
        #----------------------------------------------
        # 2. TRANSFORM
        #----------------------------------------------
        logger.info('----- Stage 2: Transformation -----')
            
        # Verification of data types and memory space before starting transformations.
        memory_utils.show_df_memory_space(df_raw)
        
        # Count null records per column
        cols = df_raw.columns
        dc.count_null_records(df_raw, cols)
        
        # Sort by 'id' and remove duplicates if any
        df_clean = dt.sort_dataframe(df_raw, sort_by='id')
        
        # Removal of duplicate records
        df_clean = dc.remove_duplicates(df_clean)
        print(f'Number of rows: {df_clean.shape[0]}')
        
        # Rename columns
        df_clean = dt.rename_columns(df_clean, DESIRED_COLS_HT)
        
        # I convert the 'milliseconds’+' to datetime and assign them to the auxiliary column 'time'.
        df_clean['time'] = dt.convert_milliseconds_to_datetime(df_clean, ['milliseconds'])
        
        # Create the 'date' column with the format dd/mm/yy based on 'time'
        df_clean["date"] = pd.to_datetime(df_clean["time"], unit="ms").dt.date

        # Creo la columna 'hour' a partir de 'time'. 
        df_clean['hour'] = pd.to_datetime(df_clean['time'].dt.strftime('%H:%M:%S.%f'), format="%H:%M:%S.%f")
        
        # Extract only the hour (0–23) as int
        df_clean["hr"] = pd.to_datetime(df_clean["time"], unit="ms").dt.hour.astype("int32")

        # Remove the auxiliary column 'time', as it is not necessary.
        # Remove the column 'is_best_match' as it is always True and does not add to the analysis.
        df_clean = df_clean.drop(columns=['time','isBestMatch'])
        
        # Casting numeric data types from object to float32 for future aggregations
        df_clean = dt.cast_data_types(df_clean, CONVERSION_MAPPING_HT)
        
        # I change the position of the columns to present them in a more orderly manner.
        df_clean = dt.change_column_position(df_clean, 'date', 'is_buyer_maker')
        df_clean = dt.change_column_position(df_clean, 'hour', 'is_buyer_maker')
        print(df_clean.head())
        
        # Verification of data types and memory space after initiating transformations.
        memory_utils.show_df_memory_space(df_clean)
        
        logger.info(f'{len(df_clean)} records were cleaned/transformed.')
        
        mem_size = df_clean.memory_usage(deep=True).sum()
        metrics['cleansed_data_size'] = mem_size / 1024
        logger.info(f'Cleansed data frame size: {metrics['cleansed_data_size']:.2f} MB')
        
        
        #----------------------------------------------
        # 3. SUMMARIZATION
        #----------------------------------------------
        logger.info('----- Stage 3: Summarization -----')
        
        group_by_cols = ['date','hr','is_buyer_maker'] # List of columns to group by

        agg_dict = {# Dictionary with columns as keys and aggregations as values
            'price':'mean',      # average of the 'price' column
            'quantity':'sum',    # average of the 'quantity' column
            'quote_qty':'sum',   # Sum of quantities
            'id':'count'         # record count per group
        }

        rename_cols = {# Dictionary with name changes for added columns
            'price':'mean_price',
            'quantity':'qty_per_hour',
            'quote_qty':'total_quote_qty',
            'id':'id_count'
        }
        
        # Assign the summarized DF to a new DF
        df_summarized = aggregations.sumarizar_df(df_clean, group_by_cols, agg_dict, rename_cols)

        # Rounding for presentation
        cols = ['mean_price', 'qty_per_hour', 'total_quote_qty']
        
        if df_summarized is not None and not df_summarized.empty:
            df_summarized[cols] = df_summarized[cols].round(3).map("{:.3f}".format)
            logger.debug(df_summarized.head().to_string())
            logger.info('Summarization stage completed.')
        else:
            logger.error(f'Error creating "df_summarized"')

        
        #----------------------------------------------
        # 4. LOAD
        #----------------------------------------------
        logger.info('----- Stage 4: Loading -----')
        
        # Store the DF in the silver layer, partitioning by date and time.
        BINANCE_HIST_TRADES = BINANCE_API_CONFIG['historical_trades']
        delta_writer.save_new_data_as_delta(df_clean, PATH_SILVER_DELTALAKE_INCREMENTAL, 
                                            'src.id = tgt.id', BINANCE_HIST_TRADES['partition_cols'])
        logger.info('Transformed data successfully loaded to silver layer.')
        
        # Save the DF in Delta Lake format, in 'overwrite' mode by default.
        # This mode overwrites changes, but allows historical records to be queried.
        delta_writer.save_data_as_delta(df_summarized, PATH_GOLD_SUMMARIZED_TABLE_INCREMENTAL)
        logger.info('Summarized data successfully loaded to gold layer.')
        
        
        #----------------------------------------------
        # 5. QUALITY CHECK
        #----------------------------------------------
        logger.info("----- Stage 5: Quality check -----")
        from src.quality import profiling
        
        report = profiling.generate_profiling_report(df_clean)
        report_path = REPORTS_INCREMENTAL / f"profile_{BINANCE_HIST_TRADES['params']['symbol']}_{start_time.strftime('%Y%m%d_%H%M%S')}.html"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_file(report_path)


        #----------------------------------------------
        # FINAL METRICS
        #----------------------------------------------
        end_time = datetime.now()
        metrics['end_time'] = end_time
        
        duration = (end_time - start_time).total_seconds()
        logger.info(f'Pipeline completed in: {duration:.2f} seconds')
        logger.debug(f"Processed records: {len(df_clean)}")
        logger.debug(f"Report saved in: {report_path}")
        
        summary = f"""
        ------------------- PIPELINE SUMMARY -------------------
        Exec_id: {correlation_id}
        Symbol: {SYMBOL}
        Records processed: {len(df_clean)}
        Duration: {duration:.2f} seconds
        Raw size: {metrics['raw_data_size']:.2f} MB -> Cleansed size: {metrics['cleansed_data_size']:.2f} MB
        Report: {report_path}
        --------------------------------------------------------
        """
        logger.info(summary)
        
    except Exception as e:
        logger.error(f'The pipeline could not be run: {e}')
        raise
    
if __name__=='__main__':
    run_incremental_pipeline()