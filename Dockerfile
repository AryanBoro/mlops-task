# ── Stage 1: build deps ────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /install

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install/pkg -r requirements.txt


# ── Stage 2: runtime image ─────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

LABEL maintainer="mlops-task"
LABEL description="MLOps batch job: rolling mean signal generator"

# Copy installed packages from builder
COPY --from=builder /install/pkg /usr/local

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash appuser
USER appuser
WORKDIR /app

# Copy project files
COPY --chown=appuser:appuser run.py        ./
COPY --chown=appuser:appuser config.yaml   ./
COPY --chown=appuser:appuser data.csv      ./

# Entrypoint: run the job, then print metrics.json to stdout
# Exit code mirrors the job's exit code.
CMD ["sh", "-c", "\
    python run.py \
        --input    data.csv \
        --config   config.yaml \
        --output   metrics.json \
        --log-file run.log \
    && echo '' \
    && echo '===== metrics.json =====' \
    && cat metrics.json \
    && echo '' \
    && echo '========================'"]
