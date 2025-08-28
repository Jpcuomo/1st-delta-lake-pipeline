import pandas as pd
import numpy as np


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


def renombrar_columnas(df: pd.DataFrame, columnas_dict: dict) -> pd.DataFrame:
    '''
    Renombra columnas funcionando con ambos formatos (string e integer)
    '''
    # Verificar qué formato tienen las columnas actuales
    current_columns = list(df.columns)
    
    # Si las columnas son integers, convertir el diccionario
    if all(isinstance(col, (int, np.integer)) for col in current_columns): 
        columnas_int = {int(k): v for k, v in columnas_dict.items()}
        return df.rename(columns=columnas_int)
    
    # Si las columnas son strings, usar el diccionario tal cual
    elif all(isinstance(col, str) for col in current_columns):
        return df.rename(columns=columnas_dict)
    
    else:
        try:
            columnas_int = {int(k): v for k, v in columnas_dict.items()}
            return df.rename(columns=columnas_int)
        except:
            return df.rename(columns=columnas_dict)
    
    
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
