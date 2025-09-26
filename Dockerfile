# Dockerfile - build GrimReaperBot
FROM python:3.11-slim

# metadata labels
LABEL org.opencontainers.image.title="grimreaperbot"
ARG COMMIT_SHA=local

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV SQLITE_DB=/data/grim_reaper.db

VOLUME ["/data"]

# default port for web API
EXPOSE 8080

CMD ["python", "grim_reaper_bot.py"]
