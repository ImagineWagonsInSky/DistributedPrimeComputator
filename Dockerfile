# Dockerfile
FROM python:3.12.3

# set noninteractive
ENV PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy full repo
COPY . .

# 