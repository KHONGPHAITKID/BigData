FROM python:3.9-slim

WORKDIR /app

RUN pip install confluent-kafka
RUN pip install matplotlib
RUN pip install pika

COPY . /app