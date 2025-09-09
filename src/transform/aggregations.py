import pandas as pd


def sumarizar_df(df: pd.DataFrame, by_col: list, agg_col: dict, rename_cols:dict) -> pd.DataFrame:
    """
    Group a DataFrame by one or more columns and perform specific aggregations on selected columns.

    Args:
        df (pd.DataFrame): DataFrame to summarize.
        by_col (list): List of columns to group by.
        agg_col (dict): Dictionary with aggregations for each column
        rename_cols (dict): Dictionary to rename the aggregated columns, for example {'price':'avg_price', 'quantity':'avg_qty', 'id':'count_id'}.
    Returns:
        pd.DataFrame:

    Returns:
        pd.DataFrame:
        DataFrame grouped with the aggregations performed and columns renamed.
    """
    return df.groupby(by_col).agg(agg_col).rename(columns=rename_cols)