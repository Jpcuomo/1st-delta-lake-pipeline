import pandas as pd
import logging


def sort_dataframe(df: pd.DataFrame, sort_by: str, ascending: bool = True) -> pd.DataFrame:
    """
    Sort a DataFrame by a column.

    Args:
        df (pd.DataFrame): DataFrame to sort.
        sort_by (str): Name of the column to sort by.
        ascending (bool, optional): Ascending order if True (default).

    Returns:
        pd.DataFrame: Sorted DataFrame.
    """
    print(f'Sorting DataFrame by {sort_by}...')
    return df.sort_values(by=sort_by, ascending=ascending)


import pandas as pd
import logging

def rename_columns(df: pd.DataFrame, dict_colums: dict) -> pd.DataFrame:
    """
    Rename columns in a DataFrame according to a dictionary.

    Args:
        df (pd.DataFrame): Input DataFrame
        dict_columns (dict): Dictionary {original_name: new_name}

    Returns:
        pd.DataFrame: DataFrame with renamed columns
    """
    logger = logging.getLogger('data_transformations')

    if not isinstance(dict_colums, dict):
        logger.error("Error: the parameter must be a dictionary")
        raise TypeError("The 'dict_columns' parameter must be a dictionary.")
    
    if not dict_colums:
        logger.error("Error: The dictionary cannot be empty.")
        raise ValueError("The dictionary cannot be empty.")

    current_columns = list(df.columns)
    missing_columns = [c for c in dict_colums.keys() if c not in current_columns]

    for col in missing_columns:
        logger.warning(f"The column '{col}' does not exist in the DataFrame. Renaming will not be applied.")

    try:
        return df.rename(columns=dict_colums)
    except Exception as e:
        logger.error(f"Error renaming columns: {e}")
        raise

    
def cast_data_types(df: pd.DataFrame, conversion_mapping: dict) -> pd.DataFrame:
    """
    Change the data types of columns in a DataFrame using mapping.
    
    Args:
        df (pd.DataFrame): Pandas DataFrame.
        conversion_mapping (dict): Dictionary with format {column: type}, for example {"col1": "int", "col2": "float"}.
    
    Returns:
        pd.DataFrame: DataFrame with columns converted to the specified types.
    """
    try:
        return df.astype(conversion_mapping)
    except KeyError as e:
        print(f"Error: some column does not exist in the DataFrame -> {e}")
        return df
    except ValueError as e:
        print(f"Type conversion error -> {e}")
        return df
    
    
def convert_milliseconds_to_datetime(df:pd.DataFrame, cols:list[str]) -> pd.DataFrame:
    '''
    Converts one or more columns of a DataFrame containing values in milliseconds 
    to datetime64[ns] type.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the columns to be converted.
    cols : list[str]
        List of column names to be transformed.

    Returns
    -------
    pd.DataFrame
        DataFrame with the selected columns converted to datetime64[ns].

    Notes
    -----
    - `pd.to_datetime` is used with the argument `unit='ms'` to indicate that the values
       represent milliseconds since the Unix epoch (1970-01-01).
    - `errors='coerce'` ensures that invalid values are converted to `NaT` instead of 
       causing an error.
    - `exact=True` forces exact (stricter) parsing.
    '''
    return df[cols].apply(lambda col: pd.to_datetime(col, unit='ms', errors='coerce', exact=True))


def change_column_position(df:pd.DataFrame, desired_col:str, shifted_col:str) -> pd.DataFrame:
    """
    Move the column `desired_col` to the position where `shifted_col` is located..

    Args:
        df (pd.DataFrame): Original DataFrame
        desired_col (str): Column that marks the destination position
        shifted_col (str): Column that will be moved

    Returns:
        pd.DataFrame: DataFrame with the columns reordered
    """
    cols = list(df.columns)
    desired_col_index = cols.index(desired_col)
    shifted_col_index = cols.index(shifted_col)
    cols.insert(shifted_col_index, cols.pop(desired_col_index))
    return df[cols]
