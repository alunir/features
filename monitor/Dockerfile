FROM python:3.9-slim

WORKDIR /app

# 必要なパッケージをインストール
RUN pip install aiodocker slackweb

# スクリプトをコピー
COPY main.py /app

CMD ["python", "/app/main.py"]