import pandas as pd
import pytest
from src.transform import data_cleaning as dc


def test_remove_duplicates():
    # Test1: Normal data frame
    df = pd.DataFrame(
        [{'col1':1, 'col2':'a'},
         {'col1':2, 'col2':'b'},
         {'col1':1,'col2':'a'}])
    df = dc.remove_duplicates(df)
    assert len(df) == 2, \
        f'Error: Expected lenght: 2, got len: {len(df)}'
    
    # Test 2: Empty data frame
    df = pd.DataFrame()
    df = dc.remove_duplicates(df)
    assert df.empty == True, \
        f'Error: Should return en empty data frame'
        
    # Test 3: Data frame is None
    df = dc.remove_duplicates(None)
    assert df is None, \
        f'Error: Should be None'
        
	# Test 4: One column data frame
    df = pd.DataFrame([{'col1':1}, {'col1':2}, {'col1':1}])
    df = dc.remove_duplicates(df)
    assert len(df) == 2, \
        f'Error: Expected length: 2, got length: {len(df)}'
    
    
def test_delete_null_records():
    # Test 1: Empty data frame
    df = pd.DataFrame()
    df = dc.delete_null_records(df)
    assert df.empty, \
        f'Error: Should return an empty data frame'
        
    # Test 2: Data frame is None
    df = dc.delete_null_records(None)
    assert df is None, \
        f'Error: Should be None'
        
    # Test 3: Normal data frame with subset
    df = pd.DataFrame([{'col1':None, 'col2':1},
                      {'col1':'a', 'col2':2},
                      {'col1':'b', 'col2':None}])
    subset = ['col2']
    df1 = dc.delete_null_records(df, subset=subset)
    assert len(df1) == 2, \
        f'Error: expected length: 2, got length: {len(df1)}'
    assert df1['col1'].to_list() == [None, 'a']
        
    # Test 4: Normal data frame without subset
    df2 = dc.delete_null_records(df)
    assert len(df2) == 1, \
        f'Error: expected length: 1, got length: {len(df2)}'
    assert df2.iloc[0]['col1'] == 'a'
        

def test_replace_null_records():
    df = pd.DataFrame([
        {'symbol': 'SOLUSDT', 'price': 34.56, 'volume': 1000},
        {'symbol': None, 'price': 35.20, 'volume': 1500},
        {'symbol': 'BTCUSDT', 'price': None, 'volume': 2000},
        {'symbol': 'ETHUSDT', 'price': 42.18, 'volume': None}
    ])
    imputation_mapping = {'symbol':'N/A', 'price':-1, 'volume':-1}
    df_imputated = dc.replace_null_records(df, imputation_mapping)
    # Imputation of Null values
    assert df_imputated.iloc[1]['symbol'] == 'N/A'
    assert df_imputated.iloc[2]['price'] == -1
    assert df_imputated.iloc[3]['volume'] == -1
    # Imputation without null values - should remain the same
    assert df_imputated.iloc[0]['symbol'] == 'SOLUSDT'
    assert df_imputated.iloc[0]['price'] == 34.56
    assert df_imputated.iloc[0]['volume'] == 1000
    # Table integrity
    assert len(df_imputated) == 4
    assert list(df_imputated.columns) == ['symbol', 'price', 'volume']
    
    
def test_count_null_records():
    # Test 1: Check normal data frame
    df = pd.DataFrame([
        {'symbol': 'SOLUSDT', 'price': 34.56, 'volume': 1000},
        {'symbol': None, 'price': 35.20, 'volume': 1500},
        {'symbol': 'BTCUSDT', 'price': None, 'volume': 2000},
        {'symbol': None, 'price': 42.18, 'volume': None}
    ])
    check_list = ['symbol', 'price', 'volume']
    null_counts = dc.count_null_records(df, check_list)
    assert null_counts == {'symbol':2, 'price':1, 'volume':1}
    
    # Test 2: check empty data frame
    subset = ['symbol','price']
    df_empty = pd.DataFrame(columns=subset)
    null_counts = dc.count_null_records(df_empty, subset=subset)
    assert null_counts == {'symbol':0, 'price':0}
    
    # Test 3: Check non existing columns
    check_cols = ['symbol', 'date']
    null_counts = dc.count_null_records(df, subset=check_cols)
    assert null_counts == {'symbol':2}
    
    # Test 4: data frame is None
    with pytest.raises(ValueError):
        dc.count_null_records(None, ['symbol'])
        
    
def test_delete_columns():
    df = pd.DataFrame([
        {'symbol': 'SOLUSDT', 'price': 34.56, 'volume': 1000},
        {'symbol': 'ADAUSDT', 'price': 35.20, 'volume': 1500},
        {'symbol': 'BTCUSDT', 'price': 1.25, 'volume': 2000},
        {'symbol': 'ETHUSDT', 'price': 42.18, 'volume': 1200}
    ])
    
    # Test 1: passing a list with not existing column names
    cols = ['end_date', 'start_date']
    df_modified = dc.delete_columns(df, cols)
    assert list(df_modified.columns) == ['symbol','price','volume']
    
    # Test 2: Passing existing and non existing column names
    cols = ['symbol','date']
    df_modified = dc.delete_columns(df, cols)
    assert list(df_modified.columns) == ['price','volume']
    
    # Test 3: data frame is None
    with pytest.raises(ValueError):
        dc.delete_columns(None, ['symbol','price','volume'])
        
    # Test 4: Data frame with columns but no rows
    cols = ['price']
    df_empty = pd.DataFrame(columns=['symbol','price','volume'])
    df_reduced = dc.delete_columns(df_empty, cols)
    assert list(df_reduced.columns) == ['symbol','volume']
    
    # Test 5: columns is None
    with pytest.raises(ValueError):
        dc.delete_columns(df, None)
        
    # Test 6: columns is empty
    with pytest.raises(ValueError):
        dc.delete_columns(df, [])
        
    # Test 7: columns is not a list
    with pytest.raises(TypeError):
        dc.delete_columns(df, {})
