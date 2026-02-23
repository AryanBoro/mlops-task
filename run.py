"""
MLOps Batch Job: Rolling Mean Signal Generator
Demonstrates reproducibility, observability, and deployment readiness.
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

def setup_logging(log_file: str) -> logging.Logger:
    """Configure structured logging to both file and stdout."""
    logger = logging.getLogger("mlops_job")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # File handler — always DEBUG level
    fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler — INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ---------------------------------------------------------------------------
# Metrics writer
# ---------------------------------------------------------------------------

def write_metrics(
    output_path: str,
    version: str,
    status: str,
    rows_processed: int,
    signal_rate: float | None,
    latency_ms: float,
    error_message: str | None = None,
) -> None:
    """Write structured metrics to a JSON file."""
    payload: dict = {
        "version": version,
        "status": status,
        "rows_processed": rows_processed,
        "signal_rate": round(signal_rate, 6) if signal_rate is not None else None,
        "latency_ms": round(latency_ms, 3),
    }
    if error_message is not None:
        payload["error_message"] = error_message

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


# ---------------------------------------------------------------------------
# Config loading & validation
# ---------------------------------------------------------------------------

REQUIRED_CONFIG_KEYS = {"seed", "window", "version"}


def load_config(config_path: str, logger: logging.Logger) -> dict:
    """Load and validate the YAML configuration file."""
    logger.info("Loading configuration from '%s'", config_path)

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    if path.stat().st_size == 0:
        raise ValueError(f"Config file is empty: {config_path}")

    try:
        with open(path, "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in config file: {exc}") from exc

    if not isinstance(cfg, dict):
        raise ValueError("Config file must contain a YAML mapping (key: value pairs).")

    missing = REQUIRED_CONFIG_KEYS - cfg.keys()
    if missing:
        raise KeyError(f"Missing required config keys: {sorted(missing)}")

    # Type validation
    if not isinstance(cfg["seed"], int):
        raise TypeError(f"'seed' must be an integer, got {type(cfg['seed']).__name__}")
    if not isinstance(cfg["window"], int) or cfg["window"] < 1:
        raise ValueError(f"'window' must be a positive integer, got {cfg['window']!r}")
    if not isinstance(cfg["version"], str) or not cfg["version"].strip():
        raise ValueError(f"'version' must be a non-empty string, got {cfg['version']!r}")

    logger.info(
        "Config loaded — version=%s | seed=%d | window=%d",
        cfg["version"], cfg["seed"], cfg["window"],
    )
    logger.debug("Full config: %s", cfg)
    return cfg


# ---------------------------------------------------------------------------
# Data loading & validation
# ---------------------------------------------------------------------------

def load_data(input_path: str, logger: logging.Logger) -> pd.DataFrame:
    """Load and validate the OHLCV CSV file."""
    logger.info("Loading data from '%s'", input_path)

    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")
    if path.stat().st_size == 0:
        raise ValueError(f"Input CSV file is empty: {input_path}")

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise ValueError(f"Failed to parse CSV file: {exc}") from exc

    if df.empty:
        raise ValueError("CSV file contains no data rows.")

    if "close" not in df.columns:
        raise ValueError(
            f"Required column 'close' not found. Columns present: {list(df.columns)}"
        )

    # Coerce 'close' to numeric, flag non-numeric values
    original_len = len(df)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    invalid_rows = df["close"].isna().sum()
    if invalid_rows == original_len:
        raise ValueError("All values in 'close' column are non-numeric or NaN.")
    if invalid_rows > 0:
        logger.warning(
            "%d row(s) with non-numeric 'close' values will be dropped.", invalid_rows
        )
        df = df.dropna(subset=["close"]).reset_index(drop=True)

    logger.info(
        "Data loaded — %d rows, %d columns. Columns: %s",
        len(df), len(df.columns), list(df.columns),
    )
    return df


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def compute_signals(
    df: pd.DataFrame,
    window: int,
    seed: int,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Compute rolling mean and binary signal on the close price."""
    logger.info("Setting numpy random seed to %d for reproducibility.", seed)
    np.random.seed(seed)

    if len(df) < window:
        raise ValueError(
            f"Not enough rows ({len(df)}) to compute a rolling mean "
            f"with window={window}. Need at least {window} rows."
        )

    logger.info("Computing rolling mean with window=%d.", window)
    df = df.copy()
    df["rolling_mean"] = df["close"].rolling(window=window).mean()

    logger.info("Computing binary signal (1 if close > rolling_mean, else 0).")
    df["signal"] = np.where(df["close"] > df["rolling_mean"], 1, 0)

    # Rows where rolling_mean is NaN (first window-1 rows) get signal=0 by default;
    # document this explicitly.
    warmup_rows = df["rolling_mean"].isna().sum()
    if warmup_rows > 0:
        logger.debug(
            "%d warmup row(s) have NaN rolling_mean (window size); signal defaulted to 0.",
            warmup_rows,
        )

    signal_rate = df["signal"].mean()
    logger.info(
        "Processing complete — rows=%d | signal_rate=%.6f",
        len(df), signal_rate,
    )
    return df


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MLOps Batch Job: Rolling Mean Signal Generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input",    default="data.csv",     help="Path to OHLCV CSV file")
    parser.add_argument("--config",   default="config.yaml",  help="Path to YAML config file")
    parser.add_argument("--output",   default="metrics.json", help="Path to write metrics JSON")
    parser.add_argument("--log-file", default="run.log",      help="Path to write log file")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    start_time = time.perf_counter()

    # Bootstrap logger immediately so errors during config load are captured
    logger = setup_logging(args.log_file)
    logger.info("=" * 60)
    logger.info("MLOps Batch Job starting.")
    logger.info("Arguments: input=%s | config=%s | output=%s | log=%s",
                args.input, args.config, args.output, args.log_file)

    # Defaults used if config load fails before version is known
    version = "unknown"
    rows_processed = 0
    signal_rate: float | None = None

    try:
        # ---- 1. Load & validate config ----
        cfg = load_config(args.config, logger)
        version = cfg["version"]

        # ---- 2. Load & validate data ----
        df = load_data(args.input, logger)

        # ---- 3. Core processing ----
        df = compute_signals(df, cfg["window"], cfg["seed"], logger)
        rows_processed = len(df)
        signal_rate = float(df["signal"].mean())

        # ---- 4. Write metrics (success) ----
        latency_ms = (time.perf_counter() - start_time) * 1000
        write_metrics(
            output_path=args.output,
            version=version,
            status="success",
            rows_processed=rows_processed,
            signal_rate=signal_rate,
            latency_ms=latency_ms,
        )
        logger.info("Metrics written to '%s'.", args.output)
        logger.info("Job finished successfully in %.3f ms.", latency_ms)
        logger.info("=" * 60)
        sys.exit(0)

    except (FileNotFoundError, ValueError, KeyError, TypeError) as exc:
        latency_ms = (time.perf_counter() - start_time) * 1000
        logger.error("Job failed: %s", exc, exc_info=True)
        write_metrics(
            output_path=args.output,
            version=version,
            status="error",
            rows_processed=rows_processed,
            signal_rate=signal_rate,
            latency_ms=latency_ms,
            error_message=str(exc),
        )
        logger.info("Error metrics written to '%s'.", args.output)
        logger.info("=" * 60)
        sys.exit(1)

    except Exception as exc:  # noqa: BLE001
        latency_ms = (time.perf_counter() - start_time) * 1000
        logger.critical("Unexpected error: %s", exc, exc_info=True)
        write_metrics(
            output_path=args.output,
            version=version,
            status="error",
            rows_processed=rows_processed,
            signal_rate=signal_rate,
            latency_ms=latency_ms,
            error_message=f"Unexpected error: {exc}",
        )
        logger.info("=" * 60)
        sys.exit(2)


if __name__ == "__main__":
    main()
