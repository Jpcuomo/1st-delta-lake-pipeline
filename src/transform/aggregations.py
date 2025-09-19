import pandas as pd
import logging

logger = logging.getLogger('aggregations')


def summarize_df(df: pd.DataFrame, by_col: list=None, agg_col: dict=None, rename_cols: dict=None) -> pd.DataFrame:
    """
    Group a DataFrame by one or more columns and perform specific aggregations on selected columns.

    Args:
        df (pd.DataFrame): DataFrame to summarize.
        by_col (list): List of columns to group by.
        agg_col (dict): Dictionary with aggregations for each column, e.g. {'price': 'mean'}.
        rename_cols (dict): Dictionary to rename the aggregated columns, e.g. {'price':'avg_price'}.

    Returns:
        pd.DataFrame: DataFrame grouped with the aggregations performed and columns renamed.
    """
    # Validations 
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    if df.empty:
        raise ValueError("df is empty")

    if not isinstance(by_col, list):
        raise TypeError("by_col must be a list")
    if not by_col:
        raise ValueError("by_col must have at least one value")

    if agg_col is None:
        agg_col = {}
    if rename_cols is None:
        rename_cols = {}

    # Validate grouping columns
    missing_by = [col for col in by_col if col not in df.columns]
    if missing_by:
        logger.warning(f"The following group-by columns do not exist in df and will be ignored: {missing_by}")
    by_col = [col for col in by_col if col in df.columns]

    if not by_col:
        raise ValueError("None of the provided by_col exist in df")

    # Validate agg_col
    VALID_AGGS = {
        'sum', 'mean', 'median', 'mode', 'std', 'var', 'min', 'max',
        'count', 'nunique', 'first', 'last', 'size', 'skew', 'kurt',
        'sem', 'mad', 'prod'
    }
    valid_agg_col = {}
    for k, v in agg_col.items():
        if k not in df.columns:
            logger.warning(f'Column "{k}" does not exist in df and will be ignored in aggregation')
            continue
        if isinstance(v, list):
            for agg in v:
                if agg not in VALID_AGGS:
                    logger.warning(f'Aggregation "{v}" for column "{k}" is not valid and will be ignored')
                    continue
                else:
                    valid_agg_col[k] = v
                    continue    
        elif v not in VALID_AGGS:
            logger.warning(f'Aggregation "{v}" for column "{k}" is not valid and will be ignored')
            continue
        else:
            valid_agg_col[k] = v

    if not valid_agg_col:
        raise ValueError("No valid columns/aggregations provided in agg_col")

    # Perform aggregation
    grouped = df.groupby(by_col).agg(valid_agg_col)
    
    # Flatten MultiIndex columns
    if isinstance(grouped.columns, pd.MultiIndex):
        new_columns = []
        for col in grouped.columns:
            if isinstance(col, tuple) and len(col) > 1:
                new_name = f"{col[0]}_{col[1]}"
                new_columns.append(new_name)
            else:
                new_columns.append(str(col))
        grouped.columns = new_columns
        
    # Reset multi index
    grouped = grouped.reset_index()

    # Validate rename_cols against grouped columns
    missing_renames = [col for col in rename_cols.keys() if col not in grouped.columns]
    if missing_renames:
        logger.warning(f"The following rename target/s do not exist in grouped df and will be ignored: {missing_renames}")
    valid_rename_cols = {k: v for k, v in rename_cols.items() if k in grouped.columns}

    return grouped.rename(columns=valid_rename_cols)
