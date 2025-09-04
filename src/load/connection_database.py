# Módulo para manejar la conexión a la base de datos
import os
from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import OperationalError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    """
    Establece la conexión a la base de datos.
    
    Usa la variable de entorno 'USE_CLOUD_SQL' para decidir si se conecta
    a Cloud SQL o a una base de datos SQLite local.
    
    Retorna un objeto 'Engine' o 'None' si falla.
    """
    conn_string = None
    
    # Comprobamos si la variable de entorno para Cloud SQL está habilitada
    use_cloud_sql = os.getenv("USE_CLOUD_SQL", "False").lower() == "true"
    
    if use_cloud_sql:
        try:
            # Usa os.getenv() para obtener las variables de entorno de forma segura
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT")
            db_name = os.getenv("DB_NAME", "pipeline_db")

            # Validación para asegurarse de que todas las variables existen
            if not all([db_user, db_password, db_host, db_port]):
                logging.error("Faltan una o más variables de entorno para la conexión a Cloud SQL.")
                return None

            # Crea la cadena de conexión para Cloud SQL (MySQL)
            # Asegúrate de que el driver pymysql esté instalado (pip install pymysql)
            conn_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            logging.info(f"Conectando a Cloud SQL: {conn_string}")

        except Exception as e:
            logging.error(f"Error al obtener las variables de entorno para Cloud SQL: {e}")
            return None
    else:
        # Crea una cadena de conexión para una base de datos SQLite local
        db_path = os.path.join(os.getcwd(), 'local_pipeline.db')
        conn_string = f'sqlite:///{db_path}'
        logging.info(f"Conectando a la base de datos local: {conn_string}")

    try:
    # Prueba la conexión sin guardarla.
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            logging.info("Conexión a la base de datos establecida con éxito.")
            # Desecha el engine ya que no lo necesitas fuera de esta función.
            engine.dispose()
        return conn_string
    except Exception as e:
        logging.error(f"Error al crear la conexión a la base de datos: {e}")
        return None

    except OperationalError as e:
        logging.error(f"Error de conexión operacional (credenciales o host incorrecto): {e}")
        return None
    except Exception as e:
        logging.error(f"Error al crear la conexión a la base de datos: {e}")
        return None