FROM apache/airflow
COPY ../../src/pipelines /opt/airflow/dags/
COPY ../../requirements.txt .
RUN pip install -r requirements.txt
ENV AIRFLOW_UID=1000
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow_meta_db/airflow
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW_CONN_POSTGRES_STAGING=postgresql://postgres:pass@postgres_db:5432/chicago_taxi_trips_staging
ENV AIRFLOW_CONN_POSTGRES_PRD=postgresql://postgres:pass@postgres_db:5432/chicago_taxi_trips
ENV AIRFLOW_CONN_MONGO_PRD=mongodb://root:rootpassword@mongo_db:27017
ENV AIRFLOW_VAR_CHICAGO_APP_TOKEN=CydGgk1HJhuh23SvIpv4fJnss
ENV AIRFLOW_VAR_POSTGRES_STAGING_CONN=postgresql://postgres:pass@postgres_db:5432/chicago_taxi_trips_staging
ENV AIRFLOW_VAR_DATA_BUCKET=remote-storage
ENTRYPOINT [ "sh", "/opt/airflow/scripts/entrypoint.sh" ]