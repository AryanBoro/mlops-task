FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir pandas numpy pyyaml

# Copy application files
COPY run.py config.yaml data.csv ./

CMD ["python", "run.py", \
     "--input",    "data.csv", \
     "--config",   "config.yaml", \
     "--output",   "metrics.json", \
     "--log-file", "run.log"]
