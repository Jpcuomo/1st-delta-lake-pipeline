import sys
import pandas as pd
from pathlib import Path

def setup_paths():
    """Configura los paths para que funcionen las importaciones"""
    root_path = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root_path))
    
    
def set_fecha_inicial(fecha_inicial:str) -> int:
    """Convierte a milisegundos la fecha de inicio ingresada"""
    return int(pd.Timestamp(fecha_inicial).timestamp() * 1000)


def set_fecha_final(fecha_final:str) -> int:
    """Convierte a milisegundos la fecha de finalización ingresada"""
    return int(pd.Timestamp(fecha_final).timestamp() * 1000)