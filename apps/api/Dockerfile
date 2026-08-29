FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir uv==0.10.11

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY apps/api ./apps/api

RUN useradd --create-home --uid 10001 product-api \
    && chown -R product-api:product-api /app

USER product-api

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=25s --start-period=30s --retries=4 \
  CMD ["/app/.venv/bin/python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=20).read()"]

CMD ["/app/.venv/bin/uvicorn", "apps.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
