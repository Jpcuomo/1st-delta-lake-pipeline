import pandas as pd


def mostrar_espacio_en_memoria_df(df:pd.DataFrame):
    '''
    Muestra una tabla con tipos de dato de cada columna, cantidad de registros, cantidad de no nulos
    y espacio total en memoria del Data frame
    
    Args:
        df (pd.DataFrame): Es el Data Frame que se desea consultar
    '''
    df.info(memory_usage='deep')