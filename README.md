# MLOps Batch Job — Rolling Mean Signal Generator

A minimal, production-style MLOps batch job that demonstrates **reproducibility**, **observability**, and **deployment readiness**.

---

## 📁 Project Structure

```
mlops-task/
├── run.py            # Main program
├── config.yaml       # Job configuration (seed, window, version)
├── data.csv          # 10,000-row OHLCV sample dataset
├── requirements.txt  # Python dependencies
├── Dockerfile        # Multi-stage Docker build
├── metrics.json      # Example output — structured metrics
├── run.log           # Example output — detailed run log
└── README.md         # This file
```

---

## ⚙️ Configuration (`config.yaml`)

| Key       | Type    | Description                                      |
|-----------|---------|--------------------------------------------------|
| `seed`    | integer | NumPy random seed for full reproducibility       |
| `window`  | integer | Rolling mean window size (number of rows)        |
| `version` | string  | Job/model version tag written to `metrics.json`  |

```yaml
seed: 42
window: 20
version: "1.0.0"
```

---

## 🚀 Running Locally

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run the job

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

All CLI arguments have defaults, so this also works:

```bash
python run.py
```

### CLI Arguments

| Argument      | Default        | Description                    |
|---------------|----------------|--------------------------------|
| `--input`     | `data.csv`     | Path to OHLCV CSV file         |
| `--config`    | `config.yaml`  | Path to YAML config file       |
| `--output`    | `metrics.json` | Path to write metrics JSON     |
| `--log-file`  | `run.log`      | Path to write detailed log     |

---

## 🐳 Running with Docker

### Build the image

```bash
docker build -t mlops-task .
```

### Run the container

```bash
docker run --rm mlops-task
```

The container will:
1. Run the batch job against the bundled `data.csv` and `config.yaml`
2. Print `metrics.json` to stdout on success
3. Exit with code `0` on success, `1` on handled errors, `2` on unexpected errors

### Bring your own data / config

Mount your files over the bundled ones:

```bash
docker run --rm \
  -v $(pwd)/my_data.csv:/app/data.csv \
  -v $(pwd)/my_config.yaml:/app/config.yaml \
  mlops-task
```

To retrieve output files from the container:

```bash
docker run --rm \
  -v $(pwd)/output:/app/output \
  mlops-task sh -c "python run.py --output output/metrics.json --log-file output/run.log && cat output/metrics.json"
```

---

## 📊 Output Files

### `metrics.json` (success)

```json
{
  "version": "1.0.0",
  "status": "success",
  "rows_processed": 10000,
  "signal_rate": 0.4967,
  "latency_ms": 27.649
}
```

### `metrics.json` (error)

```json
{
  "version": "unknown",
  "status": "error",
  "rows_processed": 0,
  "signal_rate": null,
  "latency_ms": 3.112,
  "error_message": "Required column 'close' not found. Columns present: ['date', 'price']"
}
```

### `run.log`

```
2026-02-23T09:17:05 | INFO     | ============================================================
2026-02-23T09:17:05 | INFO     | MLOps Batch Job starting.
2026-02-23T09:17:05 | INFO     | Arguments: input=data.csv | config=config.yaml | output=metrics.json | log=run.log
2026-02-23T09:17:05 | INFO     | Loading configuration from 'config.yaml'
2026-02-23T09:17:05 | INFO     | Config loaded — version=1.0.0 | seed=42 | window=20
2026-02-23T09:17:05 | DEBUG    | Full config: {'seed': 42, 'window': 20, 'version': '1.0.0'}
2026-02-23T09:17:05 | INFO     | Loading data from 'data.csv'
2026-02-23T09:17:05 | INFO     | Data loaded — 10000 rows, 6 columns. Columns: [...]
2026-02-23T09:17:05 | INFO     | Setting numpy random seed to 42 for reproducibility.
2026-02-23T09:17:05 | INFO     | Computing rolling mean with window=20.
2026-02-23T09:17:05 | INFO     | Computing binary signal (1 if close > rolling_mean, else 0).
2026-02-23T09:17:05 | DEBUG    | 19 warmup row(s) have NaN rolling_mean (window size); signal defaulted to 0.
2026-02-23T09:17:05 | INFO     | Processing complete — rows=10000 | signal_rate=0.496700
2026-02-23T09:17:05 | INFO     | Metrics written to 'metrics.json'.
2026-02-23T09:17:05 | INFO     | Job finished successfully in 27.649 ms.
```

---

## 🧠 How It Works

```
config.yaml ──┐
              ├──▶  validate  ──▶  set seed  ──▶  rolling_mean  ──▶  signal  ──▶  metrics.json
data.csv    ──┘                                                                 └──▶  run.log
```

1. **Config loaded** from `config.yaml` — all keys validated for presence and type.
2. **CSV loaded** — `close` column required; non-numeric rows warned and dropped.
3. **Seed set** — `numpy.random.seed(seed)` ensures deterministic behaviour.
4. **Rolling mean** — `pandas` `rolling(window).mean()` over `close`.
5. **Binary signal** — `1` where `close > rolling_mean`, `0` otherwise.
6. **Metrics written** — JSON with version, status, rows, signal rate, latency.
7. **Log written** — timestamped entries at DEBUG/INFO/WARNING/ERROR levels.

---

## ✅ Error Handling Matrix

| Condition                          | Behaviour                                  |
|------------------------------------|--------------------------------------------|
| Config file not found              | Logs error, writes error metrics, exit 1   |
| Invalid YAML                       | Logs error, writes error metrics, exit 1   |
| Missing config key                 | Logs error, writes error metrics, exit 1   |
| Wrong type for config value        | Logs error, writes error metrics, exit 1   |
| CSV file not found                 | Logs error, writes error metrics, exit 1   |
| Empty CSV file                     | Logs error, writes error metrics, exit 1   |
| Missing `close` column             | Logs error, writes error metrics, exit 1   |
| Non-numeric close values (partial) | Logs warning, drops rows, continues        |
| All close values non-numeric       | Logs error, writes error metrics, exit 1   |
| Window larger than dataset         | Logs error, writes error metrics, exit 1   |
| Unexpected exception               | Logs critical, writes error metrics, exit 2|

---

## 🔬 Reproducibility

Setting `seed` in `config.yaml` pins the NumPy random state before all stochastic operations. Given the same `data.csv`, `config.yaml`, and Python/library versions, the job will always produce identical `metrics.json` output.

---

## 🐍 Python Version

Tested with **Python 3.12**. Requires Python ≥ 3.10 (uses `X | Y` union type hints).
