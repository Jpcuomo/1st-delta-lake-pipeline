import pandas as pd


def sumarizar_df(df: pd.DataFrame, by_col: list, agg_col: dict, rename_cols:dict) -> pd.DataFrame:
    """
    Agrupa un DataFrame por una o varias columnas y realiza agregaciones específicas sobre columnas seleccionadas.

    Args:
        df (pd.DataFrame): DataFrame a resumir.
        by_col (list): Lista de columnas por las cuales agrupar.
        agg_col (dict): Diccionario con las agregaciones para cada columna
        rename_cols (dict): Diccionario para renombrar las columnas agregadas,
            por ejemplo {'price':'avg_price', 'quantity':'avg_qty', 'id':'count_id'}.

    Returns:
        pd.DataFrame: 
        DataFrame agrupado con las agregaciones realizadas y columnas renombradas.
    """
    return df.groupby(by_col).agg(agg_col).rename(columns=rename_cols)