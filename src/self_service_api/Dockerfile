FROM python:3.8

WORKDIR /code

COPY ./src/self_service_api/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./src/self_service_api/src /code/src

ENV MONGO_CONN_STR=mongodb://root:rootpassword@mongo_db:27017
ENV PSQL_CONN_STR=postgresql://postgres:pass@postgres_db:5432/chicago_taxi_trips

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "9001"]