FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

# Install deps
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy app code
COPY . .

# Defaults (override via env if needed)
ENV PORT=8000
# Your app lives in main.py and exposes `app`
ENV APP_MODULE=main:app

# Start
CMD bash -lc 'uvicorn "$APP_MODULE" --host 0.0.0.0 --port "$PORT"'
