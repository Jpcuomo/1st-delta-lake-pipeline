import pandas as pd
import pytest
from src.transform import data_transformation as dt


def test_ordenar_dataframe():
    data = {
        'id':[2, 3, 1],
        'name':['juan','sofi','kari']
    }
    df = pd.DataFrame(data)
    df_ordenado = dt.ordenar_dataframe(df, 'id')
    
    # Test 1: Verifica que mantiene cantidad de registros
    assert len(df_ordenado) == 3, \
        f'Error, deberían ser 3 filas, se entregaron {len(df_ordenado)}'
    
    # Test 2: Verifica que se ordene correctamente
    assert df_ordenado["id"].tolist() == [1, 2, 3], \
        f'Error en el orden. Orden actual {df_ordenado["id"].tolist()}'
    
    # Tast 3: Verifica que se ordene correctamente en orden descendente
    df_desc = dt.ordenar_dataframe(df, 'id', ascending=False)
    assert df_desc["id"].tolist() == [3, 2, 1], \
        f'Error en el orden. Orden actual {df_desc["id"].tolist()}, debería ser {[3, 2, 1]}'
    
    # Test 4: Verifica que se ordene correctamente
    assert df_ordenado['name'].tolist() == ['kari','juan','sofi'], \
        f"Error en el orden. Orden actual {df_ordenado['name'].tolist()}"
    
    # Test 5: Verifica que se mantengan los mismos elementos
    assert set(df_ordenado['id']) == set(df['id']), \
        f'Error, id modificados. "id":{set(df_ordenado["id"])}'
    

def test_renombrar_columnas():
    # Test 1: Columnas tipo str a columnas tipo str
    cols_deseadas = {'col1':'sofi', 'col2':'kari', 'col3':'juan'}
    # Data frame de prueba
    df = pd.DataFrame([{'col1':1, 'col2':2, 'col3':3}])
    # Renombrado de columnas
    df = dt.renombrar_columnas(df, cols_deseadas)

    assert df.to_dict('records') == [{'sofi':1, 'kari':2, 'juan':3}], \
        f'Error: fallo en convertir str -> str'
    
    # Test 2: Columnas tipo int a columnas tipo str
    cols_deseadas = {1:'sofi', 2:'kari', 3:'juan'}
    # Data frame de prueba
    df = pd.DataFrame([{1:'col1', 2:'col2', 3:'col3'}])
    # Renombrado de columnas
    df = dt.renombrar_columnas(df, cols_deseadas)
    
    assert df.to_dict('records') == [{'sofi':'col1', 'kari':'col2', 'juan':'col3'}], \
        f'Error: fallo en convertir int -> str'
    
    # Test 3: Columnas tipo str a columnas tipo int
    cols_deseadas = {'col1':1, 'col2':2, 'col3':3}
    # Data frame de prueba
    df = pd.DataFrame([{'col1':5, 'col2':6, 'col3':3}])
    # Renombrado de columnas
    df = dt.renombrar_columnas(df, cols_deseadas)
    
    assert df.to_dict('records') == [{1:5, 2:6, 3:3}], \
        f'Error: fallo en convertir str -> int' 
    
    # Test 4: Columnas tipo int a columnas tipo int
    cols_deseadas = {1:10, 2:20, 3:30}
    # Data frame de prueba
    df = pd.DataFrame([{1:5, 2:6, 3:3}])
    # Renombrado de columnas
    df = dt.renombrar_columnas(df, cols_deseadas)
    
    assert df.to_dict('records') == [{10:5, 20:6, 30:3}], \
        f'Error: fallo en convertir int -> int' 
        
    # Test 5: Excepción por diccionario vacío
    df = pd.DataFrame([{'col1':1}])
    with pytest.raises(TypeError):
        dt.renombrar_columnas(df, {})
        

