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

# Readiness resolves one ready publication and verifies both serving-table
# counts. Each ClickHouse query has a 60-second default server deadline, so the
# probe allows both bounded queries to finish before Docker declares failure.
HEALTHCHECK --interval=30s --timeout=135s --start-period=30s --retries=4 \
  CMD ["/app/.venv/bin/python", "-m", "apps.api.operational_check", "--timeout-seconds", "130", "--max-p95-ms", "130000"]

CMD ["/app/.venv/bin/uvicorn", "apps.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
