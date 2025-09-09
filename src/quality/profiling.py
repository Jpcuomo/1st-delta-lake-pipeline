import pandas as pd
from ydata_profiling import ProfileReport


def generate_profiling_report(df:pd.DataFrame) -> ProfileReport|None:
    '''
    Returns a detailed report with DataFrame characteristics,
    such as distinct records, missing records, memory size, etc.

    Args:
        df (pd.DataFrame): Pandas DataFrame to be analyzed

    Returns:
        ProfileReport: Profile report
    '''
    if isinstance(df, pd.DataFrame):
        return ProfileReport(df)
    else:
        print('The data frame is invalid.')
        return None