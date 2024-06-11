FROM python:3.9-slim

WORKDIR /app

# 必要なパッケージをインストール
RUN pip install aiodocker aiohttp

# スクリプトをコピー
COPY monitor.py /app

CMD ["python", "/app/monitor.py"]