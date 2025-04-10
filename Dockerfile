FROM python:3.9-slim

WORKDIR /app

RUN pip3 install confluent-kafka
RUN pip3 install matplotlib
RUN pip3 install pika

COPY . /app
