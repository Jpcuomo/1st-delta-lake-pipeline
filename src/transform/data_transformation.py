import pandas as pd
import numpy as np
import logging


def ordenar_dataframe(df: pd.DataFrame, sort_by: str, ascending: bool = True) -> pd.DataFrame:
    """
    Ordena un DataFrame según una columna.

    Args:
        df (pd.DataFrame): DataFrame a ordenar.
        sort_by (str): Nombre de la columna por la cual ordenar.
        ascending (bool, optional): Orden ascendente si es True (default).

    Returns:
        pd.DataFrame: DataFrame ordenado.
    """
    print(f'Ordenando DataFrame por {sort_by}...')
    return df.sort_values(by=sort_by, ascending=ascending)


import pandas as pd
import logging

def renombrar_columnas(df: pd.DataFrame, columnas_dict: dict) -> pd.DataFrame:
    """
    Renombra columnas de un DataFrame según un diccionario.

    Args:
        df (pd.DataFrame): DataFrame de entrada
        columnas_dict (dict): Diccionario {columna_original: columna_nueva}

    Returns:
        pd.DataFrame: DataFrame con columnas renombradas
    """
    logger = logging.getLogger('data_transformations')

    if not isinstance(columnas_dict, dict):
        logger.error("Error: el parámetro debe ser un diccionario")
        raise TypeError("El parámetro 'columnas_dict' debe ser un diccionario")
    
    if not columnas_dict:
        logger.error("Error: el diccionario no puede ser vacío")
        raise ValueError("El diccionario no puede ser vacío")

    columnas_actuales = list(df.columns)
    faltantes = [c for c in columnas_dict.keys() if c not in columnas_actuales]

    for col in faltantes:
        logger.warning(f"La columna '{col}' no existe en el DataFrame. No se aplicará el renombrado.")

    try:
        return df.rename(columns=columnas_dict)
    except Exception as e:
        logger.error(f"Error al renombrar columnas: {e}")
        raise

    
def castear_tipos_de_dato(df: pd.DataFrame, conversion_mapping: dict) -> pd.DataFrame:
    """
    Cambia los tipos de dato de columnas en un DataFrame usando un mapeo.
    
    Args:
        df (pd.DataFrame): DataFrame de Pandas.
        conversion_mapping (dict): Diccionario con formato {columna: tipo}, por ejemplo {"col1": "int", "col2": "float"}.
    
    Returns:
        pd.DataFrame: DataFrame con las columnas convertidas a los tipos especificados.
    """
    try:
        return df.astype(conversion_mapping)
    except KeyError as e:
        print(f"Error: alguna columna no existe en el DataFrame -> {e}")
        return df
    except ValueError as e:
        print(f"Error de conversión de tipos -> {e}")
        return df
    
    
def convertir_milisegundos_a_datetime(df:pd.DataFrame, cols:list[str]) -> pd.DataFrame:
    '''
    Convierte una o varias columnas de un DataFrame que contienen valores en milisegundos 
    a tipo datetime64[ns].

    Parámetros
    ----------
    df : pd.DataFrame
        DataFrame que contiene las columnas a convertir.
    cols : list[str]
        Lista de nombres de columnas a transformar.

    Retorna
    -------
    pd.DataFrame
        DataFrame con las columnas seleccionadas convertidas a datetime64[ns].

    Notas
    -----
    - Se usa `pd.to_datetime` con el argumento `unit='ms'` para indicar que los valores
      representan milisegundos desde época Unix (1970-01-01).
    - `errors='coerce'` asegura que los valores inválidos se conviertan en `NaT` en lugar de 
      provocar un error.
    - `exact=True` obliga a que el parsing sea exacto (más estricto).
    '''
    return df[cols].apply(lambda col: pd.to_datetime(col, unit='ms', errors='coerce', exact=True))


def cambiar_posicion_de_columna(df:pd.DataFrame, col_deseada:str, col_desplazada:str) -> pd.DataFrame:
    """
    Mueve la columna `col_deseada` a la posición donde está `col_desplazada`.

    Args:
        df (pd.DataFrame): DataFrame original
        col_deseada (str): columna que marca la posición destino
        col_desplazada (str): columna que se moverá

    Returns:
        pd.DataFrame: DataFrame con las columnas reordenadas
    """
    cols = list(df.columns)
    indice_col_deseada = cols.index(col_deseada)
    indice_col_desplazada = cols.index(col_desplazada)
    cols.insert(indice_col_desplazada, cols.pop(indice_col_deseada))
    return df[cols]
