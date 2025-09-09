import pandas as pd
import logging


logger = logging.getLogger('pipeline')

def build_table(json_data:dict|list, columns:list=None) -> pd.DataFrame:
    """
    Build a pandas DataFrame from JSON-formatted data
    
    Args:
        json_data (dict|list): JSON-formatted data obtained from an API.
        columns (list|None): list with desired column names (optional)
        
    Returns:
        DataFrame: A pandas DataFrame containing the data.
    """
    try:
        if isinstance(json_data, list):
            df = pd.DataFrame(json_data, columns=columns)
        elif isinstance(json_data, dict):
            df = pd.json_normalize(json_data)
        else:
            logger.error('Unsupported format. Should be a dict or list')
            return None
    except:
        logger.error("The data is not in the expected json format.")
        return None
    return df
        