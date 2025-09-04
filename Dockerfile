# Utiliza la imagen oficial de Airflow como base
# Usa la imagen oficial de Airflow en la versión correcta
FROM apache/airflow:2.8.4-python3.10

# Establece el directorio de trabajo
WORKDIR /opt/airflow

# Copia los archivos de dependencias
COPY requirements.txt .
COPY dbt_profiles/ /root/.dbt/

# Instala todas las librerías necesarias
RUN pip install --no-cache-dir -r requirements.txt

# Copia los archivos de tu pipeline y configuración
COPY ./src/ ./src/
COPY ./.env ./.env