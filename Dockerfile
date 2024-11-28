FROM python:3.12-slim

WORKDIR /usr/src/app
EXPOSE 8000

RUN apt-get update && apt-get install -y libopenblas-dev git build-essential

RUN pip install --upgrade pip

ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=$GITHUB_TOKEN

COPY requirements.txt .
RUN pip install -r requirements.txt

ARG PREFECT_API_KEY
ENV PREFECT_API_KEY=$PREFECT_API_KEY
ARG PREFECT_WORKSPACE
ENV PREFECT_WORKSPACE=$PREFECT_WORKSPACE
RUN prefect cloud login --key ${PREFECT_API_KEY} --workspace ${PREFECT_WORKSPACE}

COPY ./prefect /usr/src/app/prefect
