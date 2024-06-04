FROM python:3.12

WORKDIR /usr/src/app
EXPOSE 8000

RUN apt-get update && apt-get install -y libopenblas-dev

RUN pip install --upgrade pip

ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=$GITHUB_TOKEN

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]