import pandas as pd


def build_table(json_data:dict|list, columns:list=None) -> pd.DataFrame:
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON
    
    Args:
        json_data (dict|list): los datos en formato JSON obtenidos de una API.  
        columns (list|None): lista con nombres deseados de columnas (opcional)
        
    Retorna:
        DataFrame: Un DataFrame de pandas que contiene los datos.
    """
    try:
        if isinstance(json_data, list):
            df = pd.DataFrame(json_data, columns=columns)
        elif isinstance(json_data, dict):
            df = pd.json_normalize(json_data)
        else:
            print('Formato no soportado')
            return None
        # Conversión de ms a fechas. Realizo esta pequeña transformación para mayor 
        # legibilidad de datos recibidos y parámetros entregados en la extracción full.
        if columns:
            for col in ['close_time', 'open_time']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce", exact=True)
        return df
    except:
        print("Los datos no están en el formato esperado")
        return None