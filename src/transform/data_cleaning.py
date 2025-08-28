import pandas as pd


def eliminar_duplicados(df: pd.DataFrame, subset:list=None, keep='first') -> pd.DataFrame:
    """
    Elimina filas duplicadas en un DataFrame según una columna,
    manteniendo la primera aparición.

    Args:
        df (pd.DataFrame): DataFrame a limpiar.
        subset (str): Columna en la que se evaluarán duplicados.

    Returns:
        pd.DataFrame: DataFrame sin duplicados en la columna especificada.
    """
    return df.drop_duplicates(subset, keep)


def eliminar_registros_nulos(df: pd.DataFrame, subset: list) -> pd.DataFrame:
    '''
    Elimina registros nulos en las columnas especificadas.

    Args:
    df (pd.DataFrame): DataFrame a procesar.
    subset (list): Lista de columnas en las que se buscarán valores nulos. 
        Si alguna de esas columnas contiene NaN en una fila, la fila completa será eliminada.

    Returns:
    pd.DataFrame: DataFrame sin registros nulos en las columnas indicadas.
    '''
    return df.dropna(subset=subset)


def imputar_registros_nulos(df:pd.DataFrame, imputation_mapping:dict) -> pd.DataFrame:
    '''
    Imputa valores nulos en columnas específicas usando un mapeo definido.

    Args:
        df (pd.DataFrame): DataFrame que contiene los registros con valores nulos.
        imputation_mapping (dict): Diccionario donde las claves son los nombres de las columnas
        y los valores son los valores con los que se reemplazarán los NaN.

            Ejemplo: {'edad': 0, 'nombre': 'Desconocido'}

    Returns:
    pd.DataFrame: DataFrame con los valores nulos imputados según el mapeo especificado.
    '''
    return df.fillna(imputation_mapping)


def contar_registros_nulos(df: pd.DataFrame, subset: list) -> None:
    '''
    Cuenta registros nulos en las columnas especificadas.

    Args:
        df (pd.DataFrame): DataFrame a procesar.
        subset (list): Lista de columnas sobre las cuales se verificará la presencia de valores nulos.

    Returns:
        None
    '''
    for col in subset:
        print(f"Valores nulos en columna {col}: {df[col].isnull().sum()}")
        
        
