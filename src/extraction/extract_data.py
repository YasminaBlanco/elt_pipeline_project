from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import time
import logging
import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from urllib.parse import urljoin
from google.cloud import storage
from datetime import datetime, timezone
from ..load.validate_and_load_gcs import run_validation_and_load

# Configura el logger para que la salida se parezca a los registros del usuario.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Usa os.environ.get para manejar el caso en que la variable no exista.
GOOGLE_BUCKET_NAME = os.environ.get("GOOGLE_BUCKET_NAME")

# --- Clases para el patrón Factory Method ---

class BaseExtractor(ABC):
    """Clase base abstracta para todos los extractores."""
    @abstractmethod
    def extract_data(self):
        """Método abstracto para la extracción de datos."""
        pass

class BcraApiExtractor(BaseExtractor):
    """Extractor para la API del BCRA."""
    def __init__(self, api_url):
        self.api_url = api_url
    def extract_data(self):
        try:
            logging.info(f"Iniciando extracción de datos de la API: {self.api_url}")
            # El uso de 'verify=False' no es seguro en producción, se utiliza solo para fines de prueba.
            response = requests.get(self.api_url, verify=False, timeout=30) 
            response.raise_for_status()
            data = response.json()
            logging.info("Datos de la API obtenidos correctamente.")
            return {"source": "bcra_api", "data": data}
        except requests.exceptions.RequestException as e:
            logging.error(f"Error al obtener datos de la API: {e}")
            return None

class CsvFileExtractor(BaseExtractor):
    """Extractor para archivos CSV locales."""
    def __init__(self, file_path):
        self.file_path = file_path
    def extract_data(self):
        try:
            logging.info(f"Iniciando lectura de datos del archivo CSV: {self.file_path}")
            df = pd.read_csv(self.file_path)
            logging.info("Datos del CSV leídos correctamente.")
            return {"source": "csv_file", "data": df.to_dict('records')}
        except FileNotFoundError:
            logging.error(f"Error: El archivo {self.file_path} no se encontró.")
            return None

class WebScrapingExtractor(BaseExtractor):
    """
    Extractor reutilizable para web scraping que extrae la dirección de las
    páginas de detalles, buscando por el encabezado de "localización".
    """
    # Se agrega el nombre de la fuente al inicializador para que sea único.
    def __init__(self, url, source_name):
        self.url = url
        self.source_name = source_name

    def _scrape_details(self, url: str) -> str:
        """
        Método auxiliar para visitar una URL de detalle y extraer la dirección
        buscando el texto "Localización" o "Ubicación".
        """
        try:
            logging.info(f"    Visitando página de detalles: {url}")
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            response = requests.get(url, headers=headers, timeout=10) 
            
            if response.status_code == 406:
                logging.error(f"    Error 406 (Not Acceptable) en {url}. Reintentando con otro User-Agent...")
                headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
                response = requests.get(url, headers=headers, timeout=10)

            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Busca el encabezado que contiene la palabra "Localización" o "Ubicación"
            address_heading = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4'] and ('Localización' in tag.text or 'Ubicación' in tag.text))

            if address_heading:
                # Encuentra el siguiente elemento de texto después del encabezado.
                address_element = address_heading.find_next_sibling()
                if address_element:
                    address = address_element.text.strip()
                    # Limpia el texto para eliminar etiquetas o espacios innecesarios.
                    return ' '.join(address.split())
                else:
                    logging.warning(f"No se encontró el texto de dirección después del encabezado de localización en {url}")
                    return 'N/A'
            else:
                logging.warning(f"No se encontró el encabezado de 'Localización' o 'Ubicación' en {url}")
                return 'N/A'

        except requests.exceptions.RequestException as e:
            logging.error(f"    Error al visitar {url}: {e}")
            return 'N/A'

    def extract_data(self):
        """
        Extrae los nombres, URLs y direcciones de una lista de atracciones.
        """
        logging.info(f"Iniciando scraping de: {self.url}")
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            response = requests.get(self.url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            data = []
            # Encuentra todos los enlaces a los artículos de interés.
            attraction_links = soup.select('div.list-group-item a, h3 a')

            if attraction_links:
                logging.info(f"Se encontraron {len(attraction_links)} elementos.")
                for link in attraction_links:
                    name = link.text.strip()
                    full_url = urljoin(self.url, link['href'])
                    
                    # Llama al método auxiliar para obtener la dirección.
                    address = self._scrape_details(full_url)

                    item_data = {
                        'nombre': name,
                        'url': full_url,
                        'direccion': address
                    }
                    data.append(item_data)
            else:
                logging.info("No se encontraron elementos. La estructura HTML podría ser diferente.")
            
            logging.info(">> Extracción completada exitosamente.")

            df = pd.DataFrame(data)
            # Se usa el nombre de la fuente único en el diccionario de retorno.
            return {"source": self.source_name, "data": df.to_dict('records')}

        except requests.exceptions.RequestException as e:
            logging.error(f"Error de conexión: {e}")
            return None
        except Exception as e:
            logging.error(f"Error al procesar la respuesta HTML: {e}")
            return None
        
# --- Nueva clase para geocodificación ---
# --- Clase para geocodificación dinámica ---

class GeocodingTransformer:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="my-data-pipeline-app")
        # El diccionario se construirá dinámicamente
        self.known_locations = {}

    def geocode_by_name(self, name: str):
        """
        Geocodifica un lugar por su nombre y almacena el resultado en el diccionario.
        """
        normalized_name = name.lower().strip()
        
        # 1. Revisa si ya tenemos las coordenadas en el diccionario
        if normalized_name in self.known_locations:
            logging.info(f"   Coordenadas encontradas en caché para: {name}")
            return self.known_locations[normalized_name]
            
        # 2. Si no están en el caché, las busca a través de la API
        try:
            logging.info(f"   Buscando coordenadas para: {name}")
            # Añade "New York, USA" para mejorar la precisión
            full_query = f"{name}, New York, USA"
            location = self.geolocator.geocode(full_query, timeout=10)
            
            if location:
                latitude, longitude = location.latitude, location.longitude
                logging.info(f"   Coordenadas encontradas: {latitude}, {longitude}")
                # Almacena el resultado en el diccionario para futuras búsquedas
                self.known_locations[normalized_name] = (latitude, longitude)
                return latitude, longitude
            else:
                logging.warning(f"   No se encontraron coordenadas para el nombre: {name}")
                self.known_locations[normalized_name] = (None, None)
                return None, None
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            logging.error(f"   Error de geocodificación: {e}. Reintentando...")
            time.sleep(2)
            return self.geocode_by_name(name)
        except Exception as e:
            logging.error(f"   Error inesperado en la geocodificación: {e}")
            return None, None


# --- Función Fábrica y orquestación del módulo ---

def create_extractor(source_type, *args):
    """Función de fábrica para crear la instancia del extractor."""
    if source_type == "bcra_api":
        return BcraApiExtractor(*args)
    elif source_type == "csv_file":
        return CsvFileExtractor(*args)
    elif source_type == "web_scraping":
        # Se verifica que el nombre de la fuente esté presente para web scraping.
        if len(args) < 2:
            raise ValueError("Falta el argumento 'source_name' para el extractor de web scraping.")
        return WebScrapingExtractor(*args)
    else:
        raise ValueError(f"Tipo de extractor desconocido: {source_type}")
    
def check_if_file_exists_in_gcs(bucket_name: str, file_name: str) -> bool:
    """
    Verifica si un archivo con el nombre especificado ya existe para el día de hoy.
    """
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.exists(creds_path):
        logging.error(f"No se encontró el archivo de credenciales en {creds_path}")
        return False

    client = storage.Client.from_service_account_json(creds_path)
    bucket = client.bucket(bucket_name)
    now = datetime.now(timezone.utc)
    prefix = f"raw/{now.year}/{now.month:02d}/{now.day:02d}/{file_name}/"
    blobs = client.list_blobs(bucket, prefix=prefix, max_results=1)
    
    return len(list(blobs)) > 0

def run_extraction_pipeline():
    """Ejecuta el pipeline de extracción de datos de múltiples fuentes."""
    fecha_hoy = datetime.today().strftime('%Y-%m-%d')
    url_bcra = f"https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones"
    
    sources = [
        {"type": "bcra_api", "args": [url_bcra]},
        {"type": "csv_file", "args": ["./data/AB_NYC.csv"]},
        {"type": "web_scraping", "args": ["https://www.nuevayork.net/museos", "web_scraping_museos"]},
        {"type": "web_scraping", "args": ["https://www.nuevayork.net/monumentos-atracciones", "web_scraping_atracciones"]}
    ]
    
    # Crea una única instancia del transformador de geocodificación
    geocoder = GeocodingTransformer()

    uploaded_sources = {}  # <-- aquí guardaremos un resumen

    for source in sources:
        try:
            source_type = source["type"]
            source_args = source["args"]
            
            extractor = create_extractor(source_type, *source_args)
            data = extractor.extract_data()
            
            if data and data["source"] in ["web_scraping_museos", "web_scraping_atracciones"]:
                logging.info(f"Iniciando geocodificación por nombre para {data['source']}.")
                geocoded_data = []
                for item in data["data"]:
                    name = item.get('nombre')
                    if name:
                        latitude, longitude = geocoder.geocode_by_name(name)
                        item['latitude'] = latitude
                        item['longitude'] = longitude
                    else:
                        item['latitude'] = None
                        item['longitude'] = None
                    geocoded_data.append(item)
                
                data["data"] = geocoded_data
                logging.info("Geocodificación por nombre completada.")

            if data:
                # Validación y subida a GCS directamente aquí
                run_validation_and_load({data["source"]: data["data"]})

                # Solo guardamos un resumen ligero
                uploaded_sources[data["source"]] = "uploaded"

        except ValueError as e:
            logging.error(f"Error en la creación del extractor: {e}")

    # Retornamos un resumen pequeño para Airflow
    return uploaded_sources