import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging(log_level=logging.INFO, log_dir="logs"):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "selection.log")

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.stream = open(os.devnull, 'w', encoding='utf-8')
    
    file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(formatter)

    logging.basicConfig(level=log_level, handlers=[console_handler, file_handler])
