import pandas as pd


def remove_duplicates(df: pd.DataFrame, subset:list=None, keep='first') -> pd.DataFrame:
    """
    Removes duplicate rows in a DataFrame based on a column,
    keeping the first occurrence.

    Args:
        df (pd.DataFrame): DataFrame to be cleaned.
        subset (str): Column in which duplicates will be evaluated.

    Returns:
        pd.DataFrame: DataFrame without duplicates in the specified column.
    """
    return df.drop_duplicates(subset=subset, keep=keep)


def delete_null_records(df: pd.DataFrame, subset: list) -> pd.DataFrame:
    '''
    Removes null records in the specified columns.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        subset (list): List of columns in which null values will be searched for.
            If any of these columns contain NaN in a row, the entire row will be removed.

    Returns:
        pd.DataFrame: DataFrame without null records in the specified columns.
    '''
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
    return df.fillna(imputation_mapping)


def count_null_records(df: pd.DataFrame, subset: list) -> None:
    '''
    Counts null records in the specified columns.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        subset (list): List of columns to be checked for null values.

    Returns:
        None
    '''
    for col in subset:
        print(f"Null values in column {col}: {df[col].isnull().sum()}")
  
        
def delete_columns(df:pd.DataFrame, columns:list) -> pd.DataFrame:
    '''
    Remove the previous column(s) as a parameter
    ''' 
    return df.drop(columns=columns)
        
