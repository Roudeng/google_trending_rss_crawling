FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
# 環境變數層預設要用的port，即時輸出LOG
ENV PORT=8080 PYTHONUNBUFFERED=1

CMD ["sh", "-c", "gunicorn -b 0.0.0.0:$PORT main:app"]

