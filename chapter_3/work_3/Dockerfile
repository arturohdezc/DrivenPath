# Usa la imagen oficial de Airflow como base
FROM apache/airflow:2.10.2

# Cambia al usuario airflow
USER airflow

# Copia el archivo de requerimientos al contenedor
COPY requirements.txt .

# Instala las dependencias listadas en requirements.txt
RUN pip install -r requirements.txt

# Copia la carpeta dbt al contenedor (debe estar creada en tu proyecto)
COPY dbt /opt/airflow/dbt

