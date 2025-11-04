import logging
import os
from datetime import datetime

def setup_logger(excel_file_path):
    """
    Configura el sistema de logging basado en el archivo Excel procesado
    """
    # Obtener el nombre base del archivo Excel (sin extensión)
    excel_filename = os.path.basename(excel_file_path)
    log_filename = os.path.splitext(excel_filename)[0] + ".log"
    
    # Crear directorio de logs si no existe
    log_dir = os.path.join(os.getcwd(), "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_path = os.path.join(log_dir, log_filename)
    
    # Configurar el logger
    logger = logging.getLogger('serfinsa_processor')
    logger.setLevel(logging.INFO)
    
    # Evitar duplicar handlers si ya existen
    if logger.handlers:
        logger.handlers.clear()
    
    # Crear formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para archivo
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Agregar handlers al logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger, log_file_path

def log_separator(logger, message="=" * 50):
    """
    Agrega una línea separadora en el log
    """
    logger.info(message)
