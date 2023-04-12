FROM python:3.8

WORKDIR /code

COPY ./src/self_service_api/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./src/self_service_api/src /code/src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "9001"]