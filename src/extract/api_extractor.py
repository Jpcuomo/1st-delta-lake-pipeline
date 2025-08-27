import json
import requests
import pandas as pd
from utils.config_utils import obtener_archivo_incremental


def get_data(base_url:str, endpoint:str, data_field:str=None, params:dict=None, headers:dict=None) -> dict | list | None:
    """
    Realizar una solicitud GET a una API para obtener datos.
    
    Args:
        base_url (str): La URL base de la API.
        endpoint (str): El endpoint de la API al que se realizará la solicitud.
        params (dict): Parámetros de consulta para enviar con la solicitud.
        data_field (str): El nombre del campo en el JSON que contiene los datos.
        headers (dict): Encabezados para enviar la solicitud.
    
    Returns:
        dict|list|None: Los datos obtenidos de la API en formato JSON.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()
        print(f'Código de estado: {response.status_code}')
        
        try:
            data = response.json()
            if data_field:
                data = data[data_field]
        except:
            print("La respuesta no es un JSON válido")
            return None
        return data
    except requests.exceptions.RequestException as e:
        print(f"La petición ha fallado. código de error: {e}")
        
        
def get_data_incremental(ruta_archivo_incremental: str, base_url: str, endpoint: str, params: dict = None, headers: dict = None) -> dict|None:
    """
    Realiza una extracción incremental usando 'id' como campo incremental.
    Guarda el último id en un archivo JSON para la siguiente ejecución.
    
    Args:
        ruta_archivo_incremental (str): ruta relativa al archivo .json con la variable de control incremental.
        base_url (str): La URL base de la API.
        endpoint (str): El endpoint de la API al que se realizará la solicitud.
        params (dict): Parámetros de consulta para enviar con la solicitud.
        headers (dict): Encabezados para enviar la solicitud.
    
    Returns:
        dict|None: Los datos obtenidos de la API en formato JSON.
    
    """
    # Leer último ID guardado
    archivo_incremental = obtener_archivo_incremental(ruta_archivo_incremental)
    if not archivo_incremental or ('ultimo_valor' not in archivo_incremental and 'valor_previo' not in archivo_incremental):
        print('Archivo incremental no creado o sin campo "ultimo_valor" o "valor_previo".')
        return None

    ultimo_valor = archivo_incremental['ultimo_valor']
    valor_previo =archivo_incremental['valor_previo']
        
    params = params or {}
    params["fromId"] = ultimo_valor + 1

    # Llamada a la API
    datos = get_data(base_url, endpoint, params=params, headers=headers)
    print(f"Solicitando desde ID: {ultimo_valor + 1}")

    # Filtrar solo IDs mayores al último valor
    nuevo_valor = max(dato['id'] for dato in datos)

    # Actualizar el último valor
    with open(ruta_archivo_incremental, "w", encoding="utf-8") as f:
        json.dump({"valor_previo":valor_previo,"ultimo_valor": nuevo_valor}, f, indent=4, ensure_ascii=False)

    print(f"Archivo incremental actualizado: {ultimo_valor} -> {nuevo_valor}")

    return datos
