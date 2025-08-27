import os
import json
import pandas as pd
from configparser import ConfigParser, SectionProxy


def leer_archivo_conf(archivo_conf:str, seccion:str) -> SectionProxy:
    """
    Instancia un ConfigPaerser para leer el archivo de configuración .conf, que contiene las credenciales.
    
    Args:
        archivo_conf (str): ruta de archivo .conf
        seccion (str): seccion de archivo .conf para esta API
    
    Returns: 
        SectionProxy: similar a diccionario con credenciales
    """
    parser = ConfigParser()
    parser.read(archivo_conf)
    return parser[seccion]


def crear_archivo_incremental(contenido_incremental:list|dict, archivo_json:str, carpeta:str=None) -> None:
    """
    Verifica si existe la carpeta metadata, sino la crea.
    Luego guarda en ella el json con valor inicial.
    
    Args:
        contenido_incremental (list | dict): contenido que se va a guardar como json
        archivo_json (str): nombre del archivo json
    
    Returns:
        None
    """
    if carpeta:
        os.makedirs(carpeta, exist_ok=True)
        ruta = os.path.join(carpeta, archivo_json) 
    else:
        ruta = archivo_json
        
    if isinstance(contenido_incremental, (list,dict)): 
        with open(ruta, 'w', encoding='utf-8') as f:
            json.dump(contenido_incremental, f, indent=4, ensure_ascii=False)
    else:
        raise ValueError('El parámetro no tiene un formato json')
    

def obtener_archivo_incremental(ruta_archivo_incremental:str) -> dict | list:
    """
    Lee y retorna el contenido de un archivo JSON incremental.

    Args:
        ruta_archivo_incremental (str): Ruta de archivo JSON.

    Returns:
        dict|list|None: Contenido de archivo o None si hay error.
    """
    if os.path.exists(ruta_archivo_incremental):
        try:
            with open(ruta_archivo_incremental, 'r', encoding='utf-8') as f:
                contenido = json.load(f)
            return contenido
        except json.JSONDecodeError:
            print('El archivo json es defectuoso')
            return None
    else:
        print(f'La ruta "{ruta_archivo_incremental}" no es válida')
        return None
    
    
