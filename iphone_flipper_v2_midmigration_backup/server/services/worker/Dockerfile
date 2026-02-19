FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY server/services/worker/requirements.txt /app/server/services/worker/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt -r /app/server/services/worker/requirements.txt

# Ensure browser binaries match installed Playwright package.
RUN python -m playwright install chromium

COPY . /app

CMD ["python", "server/services/worker/worker.py"]
