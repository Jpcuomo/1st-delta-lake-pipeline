import pandas as pd
import pytest
from src.transform import data_transformation as dt


def test_sort_dataframe():
    data = {
        'id':[2, 3, 1],
        'name':['juan','sofi','kari']
    }
    df = pd.DataFrame(data)
    sorted_df = dt.sort_dataframe(df, 'id')
    
    # Test 1: Verify that it maintains the number of records
    assert len(sorted_df) == 3, \
        f'Error, there should be 3 rows, but {len(sorted_df)} were delivered.'
    
    # Test 2: Check that it is correctly sorted
    assert sorted_df["id"].tolist() == [1, 2, 3], \
        f'Error in order. Current order {sorted_df["id"].tolist()}'
    
    # Tast 3: Verify that it is sorted correctly in descending order.
    df_desc = dt.sort_dataframe(df, 'id', ascending=False)
    assert df_desc["id"].tolist() == [3, 2, 1], \
        f'Error in the order. Current order {df_desc["id"].tolist()}, should be {[3, 2, 1]}'
    
    # Test 4: Check that it is correctly sorted
    assert sorted_df['name'].tolist() == ['kari','juan','sofi'], \
        f"Error in order. Current order {sorted_df['name'].tolist()}"
    
    # Test 5: Verify that the same elements are maintained.
    assert set(sorted_df['id']) == set(df['id']), \
        f'Error, IDs modified. "id":{set(sorted_df["id"])}'
    

def test_rename_columnas():
    # Test 1: Str-type columns to str-type columns
    desired_cols = {'col1':'sofi', 'col2':'kari', 'col3':'juan'}
    # Test data frame
    df = pd.DataFrame([{'col1':1, 'col2':2, 'col3':3}])
    # Renaming columns
    df = dt.rename_columns(df, desired_cols)

    assert df.to_dict('records') == [{'sofi':1, 'kari':2, 'juan':3}], \
        f'Error: fallo en convertir str -> str'
    
    # Test 2: Int columns to str columns
    desired_cols = {1:'sofi', 2:'kari', 3:'juan'}
    # Test data frame
    df = pd.DataFrame([{1:'col1', 2:'col2', 3:'col3'}])
    # Renaming columns
    df = dt.rename_columns(df, desired_cols)
    
    assert df.to_dict('records') == [{'sofi':'col1', 'kari':'col2', 'juan':'col3'}], \
        f'Error: fallo en convertir int -> str'
    
    # Test 3: Str columns to int columns
    desired_cols = {'col1':1, 'col2':2, 'col3':3}
    # Test data frame
    df = pd.DataFrame([{'col1':5, 'col2':6, 'col3':3}])
    # Renaming columns
    df = dt.rename_columns(df, desired_cols)
    
    assert df.to_dict('records') == [{1:5, 2:6, 3:3}], \
        f'Error: error converting str -> int' 
    
    # Test 4: Int columns to int columns
    desired_cols = {1:10, 2:20, 3:30}
    # Test data frame
    df = pd.DataFrame([{1:5, 2:6, 3:3}])
    # Renaming columns
    df = dt.rename_columns(df, desired_cols)
    
    assert df.to_dict('records') == [{10:5, 20:6, 30:3}], \
        f'Error: failure to convert int -> int' 
        
    # Test 5: Exception for empty dictionary
def test_diccionario_vacio():
    df = pd.DataFrame([{'col1':1}])
    with pytest.raises(TypeError):
        dt.rename_columns(df, {})
        

