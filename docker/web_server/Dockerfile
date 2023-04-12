FROM apache/airflow
#COPY ../../src/pipelines /opt/airflow/dags/
COPY ../../requirements.txt .
RUN pip install -r requirements.txt
#
#COPY ./entrypoint.sh /opt/airflow/scripts/entrypoint.sh

#ENTRYPOINT [ "sh", "/opt/airflow/scripts/entrypoint.sh" ]