# src/advance_1/main.py

import logging
from dotenv import load_dotenv
import pandas as pd

# Carga las variables de entorno desde el archivo .env
load_dotenv() 

# Importa las funciones de los módulos del pipeline
from .extraction.extract_data import run_extraction_pipeline
from .load.validate_and_load_gcs import run_validation_and_load
from .load.load_data import run_pipeline

# Configuración de logging para todo el pipeline
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Función principal que orquesta el pipeline ELT completo.
    """
    logging.info("========================================")
    logging.info("= Iniciando pipeline ELT de datos      =")
    logging.info("========================================")

    try:
       logging.info(">> Paso 1: Iniciando extracción de datos...")
       extracted_data = run_extraction_pipeline()
       if not extracted_data:
           logging.error("La extracción no produjo datos. Terminando el pipeline.")
           return
       logging.info(">> Extracción completada exitosamente.")

    except Exception as e:
       logging.error(f"Error fatal durante la extracción: {e}")
       return


    try:
       logging.info(">> Paso 2: Iniciando validación y carga a GCS...")
       if not run_validation_and_load(extracted_data):
           logging.error("Validación o carga fallida. Terminando el pipeline.")
           return
       logging.info(">> Validación y carga a GCS completadas.")
    except Exception as e:
       logging.error(f"Error fatal durante la validación y carga: {e}")
       return

    try:
        logging.info(">> Paso 3: Iniciando carga a Cloud SQL...")
        if not run_pipeline():
            logging.error("Carga a Cloud SQL fallida. Terminando el pipeline.")
            return
        logging.info(">> Carga completada.")
    except Exception as e:
        logging.error(f"Error fatal durante la carga: {e}")
        return

    logging.info("========================================")
    logging.info("= Pipeline ELT finalizado con éxito    =")
    logging.info("========================================")

if __name__ == "__main__":
    main()