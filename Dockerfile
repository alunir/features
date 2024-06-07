FROM python:3.12-slim

WORKDIR /usr/src/app
EXPOSE 8000

RUN apt-get update && apt-get install -y libopenblas-dev git build-essential

RUN pip install --upgrade pip

ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=$GITHUB_TOKEN

COPY requirements.txt .
RUN pip install -r requirements.txt

ARG BASE_DIR
ENV BASE_DIR=$BASE_DIR
COPY ${BASE_DIR} .
