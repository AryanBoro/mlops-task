# mlops-task — Time-Series Signal Processor

Reads a CSV file, computes a rolling mean on the `close` column, generates a binary signal (`1` if `close > rolling_mean`, else `0`), and outputs metrics as JSON.

---

## Files

```
mlops-task/
├── run.py          # All pipeline logic
├── config.yaml     # Window size and seed
├── data.csv        # Input time-series data
├── requirements.txt
├── Dockerfile
├── README.md
├── metrics.json    # Example output from a successful run
└── run.log         # Example log from a successful run
```

---

## Local Setup

```bash
pip install -r requirements.txt
```

## Local Run

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

---

## Docker

### Build

```bash
docker build -t mlops-task .
```

### Run

```bash
docker run --rm mlops-task
```

To access the output files after the run, mount a local directory:

```bash
docker run --rm -v $(pwd)/output:/app mlops-task
```

The container writes `metrics.json` and `run.log` to `/app` inside the container (or your mounted directory).

---

## config.yaml

```yaml
window: 3   # Rolling window size (periods)
seed: 42    # Reproducibility seed
```

---

## Example metrics.json (success)

```json
{
  "version": "v1",
  "rows_processed": 10,
  "metric": "signal_rate",
  "value": 0.6,
  "latency_ms": 6,
  "seed": 42,
  "status": "success"
}
```

## Example metrics.json (error)

```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Missing required column 'close'. Columns found: ['date', 'price']"
}
```

---

## Error Handling

| Scenario | Exit Code | status in JSON |
|---|---|---|
| Missing input file | 1 | `"error"` |
| Empty CSV | 1 | `"error"` |
| Missing `close` column | 1 | `"error"` |
| Invalid YAML structure | 1 | `"error"` |
| Invalid CSV format | 1 | `"error"` |
| Success | 0 | `"success"` |
