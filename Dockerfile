
#imagen de docker base - capa1
FROM python:3.9.1

#instalacion de los prerequisitos
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

ENTRYPOINT [ "python", "pipeline.py" ]