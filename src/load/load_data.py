import os
import io
import json
import logging
import pandas as pd
from google.cloud import storage
from datetime import datetime, timezone
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, BigInteger, Numeric, Date, Text
from sqlalchemy.exc import OperationalError
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    """
    Genera y devuelve un objeto Engine de SQLAlchemy para la conexión a Cloud SQL.
    """
    try:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        
        if not all([db_user, db_password, db_host, db_port, db_name]):
            logging.error("Faltan variables de entorno para la conexión a la base de datos.")
            return None
        
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        logging.info(f"Conectando a Cloud SQL: {db_user}@...:{db_port}/{db_name}")
        
        engine = create_engine(connection_string, connect_args={"connect_timeout": 10})
        
        with engine.connect():
            logging.info("Conexión a la base de datos establecida con éxito. ✅")
        
        return engine
    except OperationalError as e:
        logging.error(f"Error de conexión a la base de datos: {e}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado al conectar a la base de datos: {e}")
        return None

def create_all_tables(engine):
    """
    Crea todas las tablas en la base de datos si no existen.
    """
    metadata = MetaData()

    nyc_raw = Table('nyc_raw', metadata,
        Column('id', BigInteger, primary_key=True),
        Column('name', Text),
        Column('host_id', BigInteger),
        Column('host_name', String(255)),
        Column('neighbourhood_group', String(255)),
        Column('neighbourhood', String(255)),
        Column('latitude', Numeric(precision=10, scale=8)),
        Column('longitude', Numeric(precision=11, scale=8)),
        Column('room_type', String(255)),
        Column('price', Numeric(precision=10, scale=2)),
        Column('minimum_nights', Integer),
        Column('number_of_reviews', Integer),
        Column('last_review', Date),
        Column('reviews_per_month', Numeric(precision=10, scale=2)),
        Column('calculated_host_listings_count', Integer),
        Column('availability_365', Integer)
    )

    bcra_raw = Table('bcra_raw', metadata,
        Column('fecha', Date),
        Column('tipoMoneda', String(255)),
        Column('descripcion', Text),
        Column('codigoMoneda', String(255)),
        Column('tipoPase', Numeric(precision=18, scale=8)),
        Column('tipoCotizacion', Numeric(precision=18, scale=8)),
        Column('compra', Numeric(precision=18, scale=8)),
        Column('venta', Numeric(precision=18, scale=8)),
    )

    atracciones_raw = Table('atracciones_raw', metadata,
        Column('nombre', String(255)),
        Column('url', Text),
        Column('direccion', Text),
        Column('latitude', Numeric(precision=10, scale=8)),
        Column('longitude', Numeric(precision=11, scale=8)),
    )

    museos_raw = Table('museos_raw', metadata,
        Column('nombre', String(255)),
        Column('url', Text),
        Column('direccion', Text),
        Column('latitude', Numeric(precision=10, scale=8)),
        Column('longitude', Numeric(precision=11, scale=8)),
    )

    try:
        metadata.create_all(engine)
        logging.info("Todas las tablas han sido creadas exitosamente. ✨")
        return True
    except Exception as e:
        logging.error(f"Error al crear las tablas: {e}")
        return False

def read_data_from_gcs(bucket_name, file_path):

    
    """Lee el contenido de un archivo desde Google Cloud Storage (GCS) como texto."""
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.exists(creds_path):
        logging.error(f"No se encontró el archivo de credenciales en {creds_path}")
        return False
    client = storage.Client.from_service_account_json(creds_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    try:
        data = blob.download_as_text()
        logging.info(f"Datos leídos de GCS: {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error al leer datos de GCS ({file_path}): {e}")
        return None

def load_dataframe_to_sql_pymysql(df: pd.DataFrame, table_name: str):
    """
    Carga un DataFrame en una tabla SQL usando el driver PyMySQL directamente,
    manejando NaN y estructuras anidadas.
    """
    if df.empty:
        logging.warning(f"DataFrame '{table_name}' está vacío. No se cargará nada.")
        return True
    
    # Manejar NaNs de manera integral antes de cualquier otra manipulación
    # Esto convertirá NaN y NaT a None, que es lo que MySQL necesita
    df_cleaned = df.astype(object).where(pd.notna(df), None)

    if "last_review" in df_cleaned.columns:
        df_cleaned["last_review"] = pd.to_datetime(df_cleaned["last_review"], errors="coerce").dt.date
        df_cleaned["last_review"] = df_cleaned["last_review"].where(df_cleaned["last_review"].notna(), None)
    # Convertir diccionarios a cadenas JSON si existen
    for col in df_cleaned.columns:
        if isinstance(df_cleaned[col].iloc[0], (dict, list)):
            df_cleaned[col] = df_cleaned[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT"))
        )
        with conn.cursor() as cursor:
            cols = ", ".join([f"`{col}`" for col in df_cleaned.columns])
            vals = ", ".join(["%s"] * len(df_cleaned.columns))
            sql = f"INSERT IGNORE INTO `{table_name}` ({cols}) VALUES ({vals})"
            
            records_to_insert = [tuple(row) for row in df_cleaned.itertuples(index=False)]
            cursor.executemany(sql, records_to_insert)
            conn.commit()
        logging.info(f"Datos cargados correctamente en la tabla '{table_name}' con PyMySQL. 🎉")
        return True
    except Exception as e:
        logging.error(f"Error al cargar DataFrame '{table_name}' en SQL con PyMySQL: {e}")
        return False
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

def run_pipeline() -> bool:
    """
    Orquesta y ejecuta el pipeline de carga a la base de datos.
    """
    engine = None
    try:
        logging.info("=== Iniciando el pipeline de datos (EL) para Cloud SQL ===")
        now = datetime.now(timezone.utc)
        bucket_name = "proyecto-integrador"

        paths = {
            "bcra": f"raw/bcra_api/{now.year}/{now.month:02d}/{now.day:02d}/bcra_api.json",
            "nyc": f"raw/nyc_csv_file/{now.year}/{now.month:02d}/{now.day:02d}/nyc_csv_file.csv",
            "atracciones": f"raw/atracciones_web_scrapping/{now.year}/{now.month:02d}/{now.day:02d}/atracciones_web_scrapping.json",
            "museos": f"raw/museos_web_scrapping/{now.year}/{now.month:02d}/{now.day:02d}/museos_web_scrapping.json"
        }

        dataframes = {}
        for key, path in paths.items():
            data = read_data_from_gcs(bucket_name, path)
            if data is None:
                logging.error(f"No se pudo leer '{key}' desde GCS. Pipeline detenido.")
                return False
            
            if path.endswith('.json'):
                json_data = json.loads(data)
                if key == 'bcra':
                    df_bcra = pd.DataFrame(json_data['results']['detalle'])
                    df_bcra['fecha'] = pd.to_datetime(json_data['results']['fecha']).date()
                    dataframes[key] = df_bcra
                else:
                    dataframes[key] = pd.DataFrame(json_data)
            elif path.endswith('.csv'):
                dataframes[key] = pd.read_csv(io.StringIO(data))
        
        logging.info("Dataframes creados con éxito a partir de los datos de GCS.")

        engine = get_db_connection()
        if not engine:
            logging.error("No se pudo obtener el motor de la base de datos.")
            return False

        if not create_all_tables(engine):
            return False
            
        success = True
        
        if not load_dataframe_to_sql_pymysql(dataframes['bcra'], "bcra_raw"): success = False
        if not load_dataframe_to_sql_pymysql(dataframes['nyc'], "nyc_raw"): success = False
        if not load_dataframe_to_sql_pymysql(dataframes['atracciones'], "atracciones_raw"): success = False
        if not load_dataframe_to_sql_pymysql(dataframes['museos'], "museos_raw"): success = False

        if success:
            logging.info("Carga de datos finalizada correctamente. ✅")
        else:
            logging.error("Carga a Cloud SQL fallida. Terminando el pipeline. ❌")
        
        return success

    except Exception as e:
        logging.error(f"Error en el pipeline de datos: {e}")
        return False
    finally:
        if engine:
            engine.dispose()
            logging.info("Conexión a la base de datos cerrada.")