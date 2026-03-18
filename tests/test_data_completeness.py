import os
import sys

import pytest
import pandas as pd

# Allow running tests directly and via pytest
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from validators.data_completeness import pandas_completeness_checks, ge_completeness_checks


@pytest.fixture
def clean_df():
    return pd.DataFrame(
        {
            "Transaction ID": ["TXN_001", "TXN_002"],
            "Item": ["Coffee", "Cake"],
            "Quantity": [2.0, 3.0],
            "Price Per Unit": [2.0, 3.0],
            "Total Spent": [4.0, 9.0],
            "Payment Method": ["Cash", "Credit Card"],
            "Location": ["In-store", "Takeaway"],
            "Transaction Date": ["2023-01-01", "2023-01-02"],
        }
    )


@pytest.fixture
def critical_null_df(clean_df):
    df = clean_df.copy()
    df.loc[0, "Transaction ID"] = None
    return df


@pytest.fixture
def high_null_df():
    # Item null % far above 5%
    return pd.DataFrame(
        {
            "Transaction ID": [f"TXN_{i:03d}" for i in range(1, 6)],
            "Item": [None, None, None, "Coffee", None],  # 80% null
            "Quantity": [2.0, 3.0, 1.0, 2.0, 4.0],
            "Price Per Unit": [2.0, 3.0, 1.0, 2.0, 4.0],
            "Total Spent": [4.0, 9.0, 1.0, 4.0, 16.0],
            "Payment Method": ["Cash"] * 5,
            "Location": ["In-store"] * 5,
            "Transaction Date": ["2023-01-01"] * 5,
        }
    )


@pytest.fixture
def low_null_df():
    # Payment Method null % far above 35%
    return pd.DataFrame(
        {
            "Transaction ID": [f"TXN_{i:03d}" for i in range(1, 6)],
            "Item": ["Coffee"] * 5,
            "Quantity": [2.0] * 5,
            "Price Per Unit": [2.0] * 5,
            "Total Spent": [4.0] * 5,
            "Payment Method": [None, None, None, "Cash", None],  # 80% null
            "Location": ["In-store"] * 5,
            "Transaction Date": ["2023-01-01"] * 5,
        }
    )


def test_pandas_returns_expected_keys(clean_df):
    r = pandas_completeness_checks(clean_df)
    assert set(r.keys()) == {
        "null_counts",
        "null_pct",
        "critical_failures",
        "high_failures",
        "low_failures",
        "passed_columns",
    }


def test_pandas_clean_df_has_no_failures(clean_df):
    r = pandas_completeness_checks(clean_df)
    assert r["critical_failures"] == []
    assert r["high_failures"] == []
    assert r["low_failures"] == []
    assert len(r["passed_columns"]) == 8


def test_pandas_critical_null_triggers_critical_failure(critical_null_df):
    r = pandas_completeness_checks(critical_null_df)
    assert len(r["critical_failures"]) == 1
    assert r["critical_failures"][0]["critical_column"] == "Transaction ID"


def test_pandas_high_null_triggers_high_failure(high_null_df):
    r = pandas_completeness_checks(high_null_df)
    assert len(r["high_failures"]) >= 1
    assert any(f.get("high_column") == "Item" for f in r["high_failures"])


def test_pandas_low_null_triggers_low_failure(low_null_df):
    r = pandas_completeness_checks(low_null_df)
    assert len(r["low_failures"]) >= 1
    assert any(f.get("low_column") == "Payment Method" for f in r["low_failures"])


def test_pandas_null_pct_computation_simple():
    df = pd.DataFrame({"Transaction ID": [None, "TXN_2"]})
    r = pandas_completeness_checks(df)
    assert r["null_counts"]["Transaction ID"] == 1
    assert r["null_pct"]["Transaction ID"] == 50.0


def test_pandas_missing_optional_columns_does_not_crash():
    # Missing most configured columns; function should skip non-existing columns
    df = pd.DataFrame({"Transaction ID": ["TXN_1", "TXN_2"]})
    r = pandas_completeness_checks(df)
    assert r["critical_failures"] == []


def test_ge_returns_expected_shape(clean_df):
    r = ge_completeness_checks(clean_df)
    assert isinstance(r, dict)
    assert "statistics" in r and "results" in r
    assert "evaluated_expectations" in r["statistics"]
    assert r["statistics"]["evaluated_expectations"] == 8


def test_ge_clean_df_success(clean_df):
    r = ge_completeness_checks(clean_df)
    assert r["success"] is True
    assert r["statistics"]["unsuccessful_expectations"] == 0


def test_ge_detects_critical_null(critical_null_df):
    r = ge_completeness_checks(critical_null_df)
    assert r["success"] is False
    assert r["statistics"]["unsuccessful_expectations"] >= 1

