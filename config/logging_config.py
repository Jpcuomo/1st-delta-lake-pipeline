# config/logging_config.py
import logging
from pathlib import Path
from datetime import datetime

def setup_logging(level=logging.INFO):
    """Configuración centralizada de logging"""
    Path('logs').mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
            logging.StreamHandler()
        ]
    )