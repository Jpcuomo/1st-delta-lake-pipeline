import sys
import pandas as pd
from pathlib import Path

def setup_paths():
    """Set up the paths so that imports work"""
    root_path = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root_path))
    
    
def set_start_date(fecha_inicial:str) -> int:
    """Convert the entered start date to milliseconds"""
    return int(pd.Timestamp(fecha_inicial).timestamp() * 1000)


def set_end_date(fecha_final:str) -> int:
    """Convert the entered end date to milliseconds"""
    return int(pd.Timestamp(fecha_final).timestamp() * 1000)