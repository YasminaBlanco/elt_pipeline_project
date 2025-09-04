# src/advance_1/validate_and_load.py
from google.cloud import storage
import json
import logging
from datetime import datetime, timezone
import os
from google.cloud.storage.client import Client
import pandas as pd
import io

# Configura el logger.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Usa os.environ.get para manejar el caso en que la variable no exista.
GOOGLE_BUCKET_NAME = os.environ.get("GOOGLE_BUCKET_NAME")

def validate_data_quality(data, source_name):
    """
    Realiza validaciones de calidad, completitud y coherencia para
    cada fuente de datos.
    """
    if not data:
        logging.error(f"Validación fallida para {source_name}: los datos están vacíos o son nulos.")
        return False
    
    if source_name == 'bcra_api':
        if not all(k in data for k in ['status', 'results']) or 'detalle' not in data['results']:
            logging.error(f"Validación fallida para {source_name}: faltan claves principales.")
            return False
        detalle_list = data['results']['detalle']
        if not isinstance(detalle_list, list) or not detalle_list:
            logging.error(f"Validación fallida para {source_name}: la lista de detalle está vacía.")
            return False
        sample_record = detalle_list[0]
        if not all(k in sample_record for k in ['codigoMoneda', 'tipoCotizacion']):
            logging.error(f"Validación de calidad fallida para {source_name}: faltan campos en los registros.")
            return False
        
    elif source_name == 'csv_file':
        # Para el archivo CSV, la validación debe ser diferente
        if not isinstance(data, list) or not data:
            logging.error(f"Validación fallida para {source_name}: los datos no son una lista o están vacíos.")
            return False
        sample_record = data[0]
        if not all(k in sample_record for k in ['price', 'neighbourhood']):
            logging.error(f"Validación de completitud fallida para {source_name}: faltan campos en los registros.")
            return False
        
    elif source_name in ['web_scraping_atracciones', 'web_scraping_museos']:
        if not isinstance(data, list) or not data:
            logging.error(f"Validación fallida para {source_name}: los datos no son una lista o están vacíos.")
            return False
        sample_record = data[0]
        if not all(k in sample_record for k in ['nombre', 'url', 'direccion']):
            logging.error(f"Validación de completitud fallida para {source_name}: faltan campos esperados en los registros.")
            return False
        
    logging.info(f"Validación de calidad para {source_name} completada.")
    return True

def upload_to_gcs(bucket_name, data, source_name, gcs_file_name=None):
    """
    Sube los datos a Google Cloud Storage.
    Ahora usa el nombre de archivo para decidir el formato de subida y convierte
    a DataFrame si es necesario.
    """
    if not bucket_name:
        logging.error("Nombre de bucket no definido. No se puede subir el archivo.")
        return False
    
    try:
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not creds_path or not os.path.exists(creds_path):
            logging.error(f"No se encontró el archivo de credenciales en {creds_path}")
            return False

        client = storage.Client.from_service_account_json(creds_path)
        bucket = client.bucket(bucket_name)
        
        now = datetime.now(timezone.utc)
        final_source_name = gcs_file_name if gcs_file_name else source_name
        
        # Decide la extensión del archivo y cómo subirlo
        # Si el nombre del archivo final es 'nyc_csv_file', forzamos el formato CSV.
        if final_source_name == 'nyc_csv_file':
            file_path = f"raw/{final_source_name}/{now.year}/{now.month:02d}/{now.day:02d}/{final_source_name}.csv"
            
            # Convertimos la lista de diccionarios a un DataFrame para guardarlo como CSV.
            if not isinstance(data, pd.DataFrame):
                df = pd.DataFrame(data)
            else:
                df = data
                
            csv_data = df.to_csv(index=False)
            blob = bucket.blob(file_path)
            blob.upload_from_string(csv_data, content_type='text/csv')
            
        else:
            # Para el resto de los archivos, la lógica JSON es la correcta.
            file_path = f"raw/{final_source_name}/{now.year}/{now.month:02d}/{now.day:02d}/{final_source_name}.json"
            json_data = json.dumps(data)
            blob = bucket.blob(file_path)
            blob.upload_from_string(json_data, content_type='application/json')
            
        logging.info(f"Archivo subido a GCS: gs://{bucket_name}/{file_path}")
        return True
    except Exception as e:
        logging.error(f"Error al subir el archivo a GCS: {e}")
        return False

def run_validation_and_load(extracted_data):
    """
    Orquesta la validación de los datos extraídos y su carga a GCS.
    """
    for source, data in extracted_data.items():
        if not validate_data_quality(data, source):
            logging.error(f"Proceso detenido debido a un error de validación en la fuente: {source}")
            continue
        
        gcs_name_map = {
            'csv_file': 'nyc_csv_file',
            'bcra_api': 'bcra_api',
            'web_scraping_atracciones': 'atracciones_web_scrapping',
            'web_scraping_museos': 'museos_web_scrapping'
        }
        final_name = gcs_name_map.get(source, source)

        if not upload_to_gcs(GOOGLE_BUCKET_NAME, data, source, final_name):
            logging.error(f"Proceso detenido debido a un error de carga en la fuente: {source}")
            continue
            
    logging.info("Validación y carga a GCS completadas con éxito.")
    return True