# config/logging_config.py
import logging
from pathlib import Path
from datetime import datetime

def setup_logging(tipo_extraccion:str, level:int=logging.INFO) -> None:
    """Configuración centralizada de logging"""
    Path(f'logs/{tipo_extraccion}').mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"logs/{tipo_extraccion}/pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
            logging.StreamHandler()
        ]
    )