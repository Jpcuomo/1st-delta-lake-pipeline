import os
import json
import pandas as pd
import pyarrow as pa
import logging
from pathlib import Path

from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from src.utils.file_utils import get_incremental_data

logger = logging.getLogger('pipeline')


def read_delta_lake(path:Path) -> pd.DataFrame|None:
    '''
    Reads the Delta Lake file located at the parameter path 
    and transforms it into a Pandas DataFrame.
    
    Args:
        path (str): String with the relative path to the Delta Lake file.
    
    Returns:
        pd.DataFrame|None: 
        Pandas DataFrame if it finds the path, otherwise None
    '''
    if os.path.exists(path):
        return DeltaTable(path).to_pandas()
    else:
        logger.info('The path to the file was not found')
        return None
    
    
def read_recent_extraction(bronze_path:Path, incremental_path:Path) -> pd.DataFrame:
    '''
    Read and return only records with an ID greater than the last value 
    processed from Delta Lake in the bronze layer.

    Args:
        bronze_path (str): Path where the table is stored in Delta Lake.
        incremental_path (str): Path to the .json file with the incremental variables.

    Returns:
        pd.DataFrame: DataFrame with the new (incremental) records.
    '''
    try:
        dt = DeltaTable(bronze_path)
    
        incremental_content = get_incremental_data(incremental_path)
        previous_value = incremental_content['previous_value'] # last ID already processed in silver
        last_value = incremental_content['last_value'] # last ID available in bronze
        
        # Filter to read only the records
        df = dt.to_pandas(filters=[("id", ">", previous_value)])
        
        # Update previous value with latest value
        previous_value = incremental_content['last_value']
        
        # Update the last value
        with open(incremental_path, "w", encoding="utf-8") as f:
            json.dump({"previous_value":previous_value,"last_value": last_value}, f, indent=4, ensure_ascii=False)
            
        return df
    
    except Exception as e:
        logger.info(f'The Delta Lake table could not be processed: {e}')
        raise
    
    
def save_data_as_delta(df:pd.DataFrame, path:Path, mode:str="overwrite", partition_cols:list|str=None) -> None:
    """
    Guarda un dataframe en formato Delta Lake en la ruta especificada.
    A su vez, es capaz de particionar el dataframe por una o varias columnas.
    Por defecto, el modo de guardado es "overwrite".

    Args:
        df (pd.DataFrame): El dataframe a guardar.
        path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
        mode (str): El modo de guardado. Son los modos que soporta la libreria deltalake: "overwrite", "append", "error", "ignore".
        partition_cols (list or str): La/s columna/s por las que se particionará el dataframe: Si no se especifica, no se particionará.
        
    Returns:
        None
    """
    write_deltalake(path, df, mode=mode, partition_by=partition_cols)
    
    
def save_new_data_as_delta(new_data:pd.DataFrame, data_path:Path, predicate:str, partition_cols:list|str=None) -> None:
    """
    Guarda solo nuevos datos en formato Delta Lake usando la operación MERGE,
    comparando los datos ya cargados con los datos que se desean almacenar
    asegurando que no se guarden registros duplicados.

    Args:
        new_data (pd.DataFrame): Los datos que se desean guardar.
        data_path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
        predicate (str): La condición de predicado para la operación MERGE.
        partition_cols (list): Columnas sobre las que particionar
    """
    try:
        dt = DeltaTable(data_path)
        new_data_pa = pa.Table.from_pandas(new_data)
        # Se insertan en target, datos de source que no existen en target
        dt.merge(
            source=new_data_pa,
            source_alias="src",
            target_alias="tgt",
            predicate=predicate
        ).when_not_matched_insert_all().execute()
        # Si no existe la tabla Delta Lake, se guarda como nueva
    except TableNotFoundError:
        save_data_as_delta(new_data, data_path, partition_cols=partition_cols)