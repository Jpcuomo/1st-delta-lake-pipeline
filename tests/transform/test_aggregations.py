import pytest
import pandas as pd
from src.transform import aggregations as agg


def test_summarize_df():
    df = pd.DataFrame([
    {'symbol': 'SOLUSDT', 'exchange':'binance','price': 34.56, 'volume': 1000},
    {'symbol': 'SOLUSDT', 'exchange':'coinbase','price': 35.20, 'volume': 1500},
    {'symbol': 'BTCUSDT', 'exchange':'binance','price': 1.25, 'volume': 2000},
    {'symbol': 'ETHUSDT', 'exchange':'binance','price': 42.18, 'volume': 1200}
    ])
    by_col = ['symbol', 'exchange']
    agg_col = {'price':['min','max','mean'],'volume':['sum','mean']}
 
    agg_df = agg.summarize_df(df, by_col=by_col, agg_col=agg_col)
 
    #Test 1- Expected columns with index resetted
    expected_columns = ['symbol','exchange','price_min','price_max','price_mean','volume_sum','volume_mean']
    assert list(agg_df.columns) == expected_columns, \
        f"Expected {expected_columns}, got {list(agg_df.columns)}"
    
     # Test 2- Edge cases
    # df is None
    with pytest.raises(TypeError):
        agg.summarize_df(None, by_col, agg_col)
    # df is not a df
    with pytest.raises(TypeError):
        agg.summarize_df('not_a_df', by_col, agg_col)
    # df is an empty df
    with pytest.raises(ValueError):
        agg.summarize_df(pd.DataFrame(), by_col, agg_col)
    # by_col is an empty list
    with pytest.raises(ValueError):
        agg.summarize_df(df, [], agg_col)
    # by_col is not a list
    with pytest.raises(TypeError):
        agg.summarize_df(df, 'not_a_list', agg_col)
    
     # Check data corrupcy
    assert agg_df.iloc[2].to_dict() == {'symbol':'SOLUSDT', 'exchange':'binance','price_min':pytest.approx(34.56),'price_max':pytest.approx(34.56),'price_mean':pytest.approx(34.56),'volume_sum':1000,'volume_mean':1000}
 
    # Check correct management for incorrect and correct renaming
    rename_cols = {'name':'symbol','symbol':'pair'}
    df_renamed_cols = agg.summarize_df(df, by_col=by_col,agg_col=agg_col,rename_cols=rename_cols)
    assert df_renamed_cols.columns[0] == 'pair'
 
    # Test 4: Rename columns (valid + invalid)
    rename_cols = {'symbol': 'pair', 'not_exists': 'ignored'}
    df_renamed = agg.summarize_df(df, by_col=by_col, agg_col=agg_col, rename_cols=rename_cols)
    assert 'pair' in df_renamed.columns
    assert 'not_exists' not in df_renamed.columns

    # Test 5: agg_col with invalid column
    bad_agg_col = {'nonexistent': 'mean', 'price': 'foobar'}
    with pytest.raises(ValueError):
        agg.summarize_df(df, by_col, bad_agg_col)

    # Test 6: by_col with invalid columns
    with pytest.raises(ValueError):
        agg.summarize_df(df, ['doesnotexist'], agg_col)

    result = agg.summarize_df(df, ['symbol', 'doesnotexist'], agg_col)
    assert 'symbol' in result.columns

    # Test 7: Minimal case
    df_small = pd.DataFrame([{'a': 1, 'b': 10}])
    res = agg.summarize_df(df_small, by_col=['a'], agg_col={'b':'mean'})
    assert res.iloc[0]["b"] == 10
 
