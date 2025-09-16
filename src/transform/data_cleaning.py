import pandas as pd
import logging

logger = logging.getLogger('cleaning')


def remove_duplicates(df: pd.DataFrame, subset:list=None, keep='first') -> pd.DataFrame|None:
    """
    Removes duplicate rows in a DataFrame based on specified columns.

    Args:
        df: DataFrame to be cleaned
        subset: List of columns to check for duplicates
        keep: Which duplicates to keep ('first', 'last', False)

    Returns:
        DataFrame without duplicates, or None if input is None
    """
    if df is None:
        logger.error('Data frame nulo')
        return None      
    
    if df.empty:
        logger.error('Data frame vacío, no duplicates to remove')
        return df  
    
    return df.drop_duplicates(subset=subset, keep=keep)


def delete_null_records(df: pd.DataFrame, subset: list=None) -> pd.DataFrame:
    '''
    Removes null records in the specified columns.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        subset (list): List of columns in which null values will be searched for.
            If any of these columns contain NaN in a row, the entire row will be removed.

    Returns:
        pd.DataFrame: DataFrame without null records in the specified columns.
    '''
    if df is None:
        logger.error('Error: Null data frame')
        return None      
    
    if df.empty:
        logger.error('Error: Empty data frame, no null records')
        return df
    return df.dropna(subset=subset)


def replace_null_records(df:pd.DataFrame, imputation_mapping:dict) -> pd.DataFrame:
    '''
    Replaces null values in specific columns using a defined mapping.

    Args:
        df (pd.DataFrame): DataFrame containing records with null values.
        imputation_mapping (dict): Dictionary where the keys are the column names
            and the values are the values with which the NaNs will be replaced.

            Example: {'age': 0, 'name': 'Unknown'}

    Returns:
        pd.DataFrame: DataFrame with the null values imputed according to the specified mapping.
    '''
    if df is None:
        raise ValueError('Error: Data frame cannot be None')
    if df.empty:
        raise ValueError('Error: Data frame cannot be empty')
    if not isinstance(imputation_mapping, dict):
        raise TypeError('Error: imputation_mapping must be a dictionary')
    
    wrong_cols = [col for col in imputation_mapping.keys() if col not in df.columns]
    for col in wrong_cols:
        logger.warning(f'Column "{col}" does not exists in the data frame')
        
    valid_cols = {col: value for col, value in imputation_mapping.items() if col in df.columns}
    for col in valid_cols.keys():
        logger.debug(f'Column "{col}" has modified its null values') 
        
    return df.fillna(valid_cols)


def count_null_records(df:pd.DataFrame, subset:list=None) -> dict:
    '''
    Counts null records in the specified columns.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        subset (list): List of columns to be checked for null values.

    Returns:
        dict: name columns with nulls count
    '''
    if df is None:
        raise ValueError('Data frame is None')
    
    if subset is None:
        subset = df.columns.to_list()
        
    count_nulls = {}

    for col in subset:
        if col in df.columns.to_list():
            null_count = df[col].isnull().sum()
            count_nulls[col] = null_count
            logger.debug(f"Null values in column '{col}': {null_count}")
        else:
            logger.warning(f"Column '{col}' not found in DataFrame")
    return count_nulls
            
        
def delete_columns(df:pd.DataFrame, columns:list) -> pd.DataFrame:
    '''
    Remove the previous column(s) as a parameter
    ''' 
    if df is None: 
        raise ValueError('Data frame cannot be None')
    
    if columns is None:
        raise ValueError('columns cannot be None')
    
    if not isinstance(columns, list):
        raise TypeError('Columns must be a list')
    
    if not columns:
        raise ValueError('Columns cannot be empty')
    
    # columns not contained in the data frame
    incorrect_columns = [col for col in columns if col not in df.columns]
    
    for col in incorrect_columns:
        logger.warning(f'Column: "{col}" does not exist in the data frame')
            
    correct_cols = [col for col in columns if col in df.columns]
    for col in correct_cols:
        logger.debug(f'Column "{col}" will be deleted from the data frame')
    
    return df.drop(columns=correct_cols)
        
