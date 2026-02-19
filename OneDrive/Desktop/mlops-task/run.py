#!/usr/bin/env python3
"""
run.py
------
Time-series signal processor.

Reads a CSV file, computes a rolling mean on the 'close' column using a
window defined in config.yaml, generates a binary signal, and writes
metrics to a JSON file.

Usage:
    python run.py --input data.csv --config config.yaml \
                  --output metrics.json --log-file run.log
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_file: str) -> None:
    """Attach a console handler and a file handler to the root logger."""
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="w", encoding="utf-8"),
        ],
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Time-series rolling-mean signal processor."
    )
    parser.add_argument("--input",    required=True, help="Path to input CSV file.")
    parser.add_argument("--config",   required=True, help="Path to YAML config file.")
    parser.add_argument("--output",   required=True, help="Path for output metrics JSON.")
    parser.add_argument("--log-file", required=True, help="Path for the log file.")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def write_output(path: str, payload: dict) -> None:
    """Serialise *payload* to *path* and echo it to stdout."""
    text = json.dumps(payload, indent=2)
    Path(path).write_text(text, encoding="utf-8")
    print(text)


def error_exit(output_path: str, message: str) -> None:
    """Log an error, write the error JSON payload, then exit with code 1."""
    logging.error(message)
    write_output(output_path, {
        "version": "v1",
        "status": "error",
        "error_message": message,
    })
    sys.exit(1)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)

    t_start = time.time()
    logging.info("Job start — input=%s  config=%s  output=%s",
                 args.input, args.config, args.output)

    # ------------------------------------------------------------------
    # 1. Load config
    # ------------------------------------------------------------------
    if not Path(args.config).exists():
        error_exit(args.output, f"Config file not found: {args.config}")

    try:
        with open(args.config, "r", encoding="utf-8") as fh:
            config = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        error_exit(args.output, f"Invalid YAML structure: {exc}")

    if not isinstance(config, dict):
        error_exit(args.output, "Config must be a YAML mapping (key: value pairs).")

    if "version" not in config:
        error_exit(args.output, "Config is missing required key: 'version'.")

    if config["version"] != "v1":
        error_exit(args.output,
                   f"Unsupported config version: '{config['version']}'. Expected 'v1'.")

    if "window" not in config:
        error_exit(args.output, "Config is missing required key: 'window'.")

    if "seed" not in config:
        error_exit(args.output, "Config is missing required key: 'seed'.")

    window = config["window"]
    seed = config["seed"]

    if not isinstance(window, int) or window < 1:
        error_exit(args.output,
                   f"'window' must be a positive integer, got: {window!r}")

    logging.info("Config loaded — window=%d  seed=%d", window, seed)

    np.random.seed(seed)
    logging.info("Random seed set to %d", seed)

    # ------------------------------------------------------------------
    # 2. Load CSV
    # ------------------------------------------------------------------
    if not Path(args.input).exists():
        error_exit(args.output, f"Input file not found: {args.input}")

    try:
        df = pd.read_csv(args.input)
    except Exception as exc:
        error_exit(args.output, f"Invalid CSV format: {exc}")

    if df.empty:
        error_exit(args.output, "Input CSV is empty.")

    if "close" not in df.columns:
        error_exit(args.output,
                   f"Missing required column 'close'. "
                   f"Columns found: {list(df.columns)}")

    logging.info("Rows loaded: %d", len(df))

    # ------------------------------------------------------------------
    # 3. Rolling mean
    # ------------------------------------------------------------------
    df["rolling_mean"] = df["close"].rolling(window=window).mean()
    logging.info("Rolling mean calculated — window=%d", window)

    # ------------------------------------------------------------------
    # 4. Signal generation
    # ------------------------------------------------------------------
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)
    signal_rate = float(df["signal"].mean())
    logging.info("Signals generated — signal_rate=%.4f", signal_rate)

    # ------------------------------------------------------------------
    # 5. Metrics
    # ------------------------------------------------------------------
    latency_ms = int((time.time() - t_start) * 1000)

    metrics = {
        "version":        "v1",
        "rows_processed": len(df),
        "metric":         "signal_rate",
        "value":          round(signal_rate, 6),
        "latency_ms":     latency_ms,
        "seed":           seed,
        "status":         "success",
    }

    logging.info(
        "Metrics summary — rows=%d  signal_rate=%.4f  latency_ms=%d",
        metrics["rows_processed"], metrics["value"], metrics["latency_ms"],
    )

    write_output(args.output, metrics)
    logging.info("Job complete — metrics written to %s", args.output)


if __name__ == "__main__":
    main()
