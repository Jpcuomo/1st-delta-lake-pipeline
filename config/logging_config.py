# config/logging_config.py
import logging
import uuid
from pathlib import Path
from datetime import datetime

def logging_setup(extraction_type:str, level:int=logging.INFO) -> str:
    """Centralized logging configuration"""
    
    correlation_id = str(uuid.uuid4())[:8]
    
    base_dir = Path(__file__).resolve().parent.parent
    log_dir = base_dir / 'logs' / extraction_type
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=level,
        format=f'%(asctime)s - %(name)s - %(levelname)s - [exec_id:{correlation_id}] - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
            logging.StreamHandler()
        ]
    )
    return correlation_id