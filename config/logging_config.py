# config/logging_config.py
import logging
from pathlib import Path
from datetime import datetime

def setup_logging(tipo_extraccion:str, level:int=logging.INFO) -> None:
    """Configuración centralizada de logging"""
    
    base_dir = Path(__file__).resolve().parent.parent
    log_dir = base_dir / 'logs' / tipo_extraccion
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
            logging.StreamHandler()
        ]
    )