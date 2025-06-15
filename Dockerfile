FROM apache/airflow:2.9.3-python3.11

# Install extra system packages you need
USER root
RUN apt-get update && apt-get install -y build-essential unixodbc-dev

# Copy your code
USER airflow
COPY dags/ /opt/airflow/dags/
COPY configs/ /opt/airflow/configs/
COPY dags/processors /opt/airflow/dags/processors
COPY dags/utils /opt/airflow/dags/utils

# Extra Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Expose Airflow web-server port
EXPOSE 8080
