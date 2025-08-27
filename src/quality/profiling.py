import pandas as pd
from ydata_profiling import ProfileReport


def generar_profiling_report(df:pd.DataFrame) -> ProfileReport|None:
    '''
    Devuleve un reporte detallado con caracteristicas del DataFrame,
    como registros distintos, faltantes, tamaño en memoria, etc.

    Args:
        df (pd.DataFrame): DataFrame de Pandas que se desea analizar
    
    Returns:
        ProfileReport: Informe sobre los perfiles
    '''
    if isinstance(df, pd.DataFrame):
        return ProfileReport(df)
    else:
        print('El Data frame no es válido')
        return None