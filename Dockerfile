FROM apache/airflow
COPY ./src/pipelines /opt/airflow/dags/
COPY requirements.txt .
RUN pip install -r requirements.txt