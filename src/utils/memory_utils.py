import pandas as pd


def show_df_memory_space(df:pd.DataFrame):
    '''
    Displays a table with data types for each column, number of records, number of non-nulls,
    and total memory space of the data frame.
    
    Args:
        df (pd.DataFrame): It is the Data Frame you want to query.
    '''
    df.info(memory_usage='deep')