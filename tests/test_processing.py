import pandas as pd
import numpy as np
import pytest
from run import compute_signals, setup_logging

@pytest.fixture
def logger(tmp_path):
    return setup_logging(tmp_path / "log.txt")

def test_compute_signals_basic(logger):
    df = pd.DataFrame({"close": [1, 2, 3, 4, 5]})
    result = compute_signals(df, window=2, seed=42, logger=logger)
    assert "rolling_mean" in result.columns
    assert "signal" in result.columns
    # signal should only be 0 or 1
    assert set(result["signal"].tolist()).issubset({0, 1})

def test_signal_values(logger):
    df = pd.DataFrame({"close": [10, 9, 8, 7, 6]})
    result = compute_signals(df, window=2, seed=42, logger=logger)
    # first row rolling_mean is NaN so signal 0
    assert result["signal"].iloc[0] == 0
    # ensure signals are numeric and correct type
    assert result["signal"].dtype == np.int64

def test_not_enough_rows_raises(logger):
    df = pd.DataFrame({"close": [1]})
    with pytest.raises(ValueError):
        compute_signals(df, window=2, seed=42, logger=logger)