import os
import json
import pandas as pd


def crear_archivo_incremental(contenido_incremental:list|dict, archivo_json:str, carpeta:str=None) -> None:
    """
    Verifica si existe la carpeta metadata, sino la crea.
    Luego guarda en ella el json con valor inicial.
    
    Args:
        contenido_incremental (list | dict): contenido que se va a guardar como json
        archivo_json (str): nombre del archivo json
    
    Returns:
        None
    """
    if carpeta:
        os.makedirs(carpeta, exist_ok=True)
        ruta = os.path.join(carpeta, archivo_json) 
    else:
        ruta = archivo_json
        
    if isinstance(contenido_incremental, (list,dict)): 
        with open(ruta, 'w', encoding='utf-8') as f:
            json.dump(contenido_incremental, f, indent=4, ensure_ascii=False)
    else:
        raise ValueError('El parámetro no tiene un formato json')
    
    
def obtener_archivo_incremental(ruta_archivo_incremental:str) -> dict | list:
    """
    Lee y retorna el contenido de un archivo JSON incremental.

    Args:
        ruta_archivo_incremental (str): Ruta de archivo JSON.

    Returns:
        dict|list|None: Contenido de archivo o None si hay error.
    """
    if os.path.exists(ruta_archivo_incremental):
        try:
            with open(ruta_archivo_incremental, 'r', encoding='utf-8') as f:
                contenido = json.load(f)
            return contenido
        except json.JSONDecodeError:
            print('El archivo json es defectuoso')
            return None
    else:
        print(f'La ruta "{ruta_archivo_incremental}" no es válida')
        return None
    
    
def guardar_formato_parquet(df:pd.DataFrame, path:str, engine:str='pyarrow', compression:str='snappy', index:bool=False):
    '''
    Guarda un DF en formato parquet
    
    Args:
        df (pd.DataFrame): El DF que se desea guardar en formato parquet
        path (str): Path del archivo de guardado. Si no existe lo crea
        engine (str): Motor de guardado ('pyarrow' | 'fastparquet'), por defecto 'pyarrow'
        compression (str): Método de compresión
        index (bool): Agregar o no una columna indexada
        
    Returns:
        None
    '''
    # Creo el directorio si no existe
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    # Guardo en formato parquet
    df.to_parquet(
    path,
    engine=engine,
    compression=compression,
    index=index)