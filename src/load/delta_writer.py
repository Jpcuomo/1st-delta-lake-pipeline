import os
import json
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from utils.config_utils import obtener_archivo_incremental


def leer_delta_lake(path:str) -> pd.DataFrame|None:
    '''
    Lee el archivo Delta Lake ubicado en la ruta del parámetro y lo
    transforma en un DataFrame de Pandas.
    
    Args:
        path (str): String con la ruta relativa al archivo Delta Lake.
    
    Returns:
        pd.DataFrame|None: 
        DataFrame de Pandas si encuentra el path, sino None
    '''
    if os.path.exists(path):
        return DeltaTable(path).to_pandas()
    else:
        print('El path al archivo no fue encontrado')
        return None
    
    
def leer_extraccion_reciente(path_bronce:str, path_incremental:str) -> pd.DataFrame:
    '''
    Lee y devuelve únicamente los registros con id mayor al último valor 
    procesado desde el Delta Lake en la capa bronce.

    Args:
        path_bronce (str): Ruta donde está almacenada la tabla en Delta Lake.
        path_incremental (str): Ruta al archivo .json con las variables incrementales.

    Returns:
        pd.DataFrame: DataFrame con los registros nuevos (incrementales).
    '''
    try:
        dt = DeltaTable(path_bronce)
    
        contenido_incremental = obtener_archivo_incremental(path_incremental)
        valor_previo = contenido_incremental['valor_previo'] # último id ya procesado en silver
        ultimo_valor = contenido_incremental['ultimo_valor'] # último id disponible en bronce
        
        # Filtro para leer solo los registros
        df = dt.to_pandas(filters=[("id", ">", valor_previo)])
        # Actualizo valor previo con el último valor
        valor_previo = contenido_incremental['ultimo_valor']
        
        # Actualizar el último valor
        with open(path_incremental, "w", encoding="utf-8") as f:
            json.dump({"valor_previo":valor_previo,"ultimo_valor": ultimo_valor}, f, indent=4, ensure_ascii=False)
            
        return df
    
    except Exception as e:
        raise Exception(f'No se pudo procesar la tabla Delta Lake: {e}')
    
    
def save_data_as_delta(df:pd.DataFrame, path:str, mode:str="overwrite", partition_cols:list|str=None) -> None:
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
    
    
def save_new_data_as_delta(new_data:pd.DataFrame, data_path:str, predicate:str, partition_cols:list|str=None) -> None:
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