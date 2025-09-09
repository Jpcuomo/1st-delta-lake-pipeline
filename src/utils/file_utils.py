import os
import json
import pandas as pd


def create_incremental_file(incremental_content:list|dict, json_file:str, folder:str=None) -> None:
    """
    Check if the metadata folder exists, and if not, create it.
    Then save the json file with the initial value in it.
    
    Args:
        incremental_content (list | dict): content to be saved as json
        json_file (str): name of the json file
        folder (str): folder that will contain the incremental json file
    
    Returns:
        None
    """
    if folder:
        os.makedirs(folder, exist_ok=True)
        ruta = os.path.join(folder, json_file) 
    else:
        ruta = json_file
        
    if isinstance(incremental_content, (list,dict)): 
        with open(ruta, 'w', encoding='utf-8') as f:
            json.dump(incremental_content, f, indent=4, ensure_ascii=False)
    else:
        raise ValueError('El parámetro no tiene un formato json')
    
    
def get_incremental_data(incremental_file_path:str) -> dict | list:
    """
    Reads and returns the contents of an incremental JSON file.

    Args:
        incremental_file_path (str): JSON file path.

    Returns:
        dict|list|None: File content or None if there is an error.
    """
    if os.path.exists(incremental_file_path):
        try:
            with open(incremental_file_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
            return content
        except json.JSONDecodeError:
            print('The JSON file is corrupt.')
            return None
    else:
        print(f'Path to "{incremental_file_path}" is not válid')
        return None
    
    
def save_parquet_format(df:pd.DataFrame, path:str, engine:str='pyarrow', compression:str='snappy', index:bool=False):
    '''
    Save a DF in parquet format
    
    Args:
        df (pd.DataFrame): The DF you want to save in parquet format.
        path (str): Path of the saved file. If it does not exist, it will be created.
        engine (str): Saving engine ('pyarrow' | 'fastparquet'), default 'pyarrow'.
        compression (str): Compression method.
        index (bool): Whether or not to add an indexed column.
        
    Returns:
        None
    '''
    # Create the directory if it does not exist
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    # save in parquet format
    df.to_parquet(
    path,
    engine=engine,
    compression=compression,
    index=index)