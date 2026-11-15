# One small image that can run any of the Python services in this folder.
# The hub (deploy/opsbox/docker-compose.yml) uses it for ring-alerts.
FROM python:3.12-slim

# Print log lines as they happen instead of holding them in a buffer.
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shop shop
COPY home home

# Don't run as root.
USER nobody

CMD ["python", "-m", "home.consumers.ring_alerts"]
