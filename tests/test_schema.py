import os
import sys
import pytest
import pandas as pd

# Allow running this file directly: `python tests/test_schema.py`
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from validators.schema_validator import pandas_schema_checks, ge_schema_checks, EXPECTED_COLUMNS
import logging

logger = logging.getLogger(__name__)



@pytest.fixture
def clean_df():
    return pd.DataFrame({
        "Transaction ID"  : ["TXN_001", "TXN_002"],
        "Item"            : ["Coffee", "Cake"],
        "Quantity"        : [2.0, 3.0],
        "Price Per Unit"  : [2.0, 3.0],
        "Total Spent"     : [4.0, 9.0],
        "Payment Method"  : ["Cash", "Credit Card"],
        "Location"        : ["In-store", "Takeaway"],
        "Transaction Date": ["2023-01-01", "2023-01-02"]
    })

@pytest.fixture
def missing_col_df():
    return pd.DataFrame({
        "Transaction ID"  : ["TXN_001"],
        "Item"            : ["Coffee"],
        "Quantity"        : [2.0],
        # Price Per Unit missing intentionally
        "Total Spent"     : [4.0],
        "Payment Method"  : ["Cash"],
        "Location"        : ["In-store"],
        "Transaction Date": ["2023-01-01"]
    })

@pytest.fixture
def wrong_dtype_df():
    return pd.DataFrame({
        "Transaction ID"  : ["TXN_001"],
        "Item"            : ["Coffee"],
        "Quantity"        : ["two"],        # wrong dtype
        "Price Per Unit"  : [2.0],
        "Total Spent"     : [4.0],
        "Payment Method"  : ["Cash"],
        "Location"        : ["In-store"],
        "Transaction Date": ["2023-01-01"]
    })

@pytest.fixture
def extra_col_df():
    return pd.DataFrame({
        "Transaction ID"  : ["TXN_001"],
        "Item"            : ["Coffee"],
        "Quantity"        : [2.0],
        "Price Per Unit"  : [2.0],
        "Total Spent"     : [4.0],
        "Payment Method"  : ["Cash"],
        "Location"        : ["In-store"],
        "Transaction Date": ["2023-01-01"],
        "Notes"           : ["unexpected"]  # extra column
    })

@pytest.fixture
def wrong_order_df():
    return pd.DataFrame({
        "Item"            : ["Coffee"],     # wrong order
        "Transaction ID"  : ["TXN_001"],
        "Quantity"        : [2.0],
        "Price Per Unit"  : [2.0],
        "Total Spent"     : [4.0],
        "Payment Method"  : ["Cash"],
        "Location"        : ["In-store"],
        "Transaction Date": ["2023-01-01"]
    })



def test_no_missing_columns(clean_df):
    result = pandas_schema_checks(clean_df)
    try:
        assert result["missing_columns"] == []
        assert result["extra_columns"]   == []
        assert result["dtype_issues"]    == []
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info(f"test_no_missing_columns passed: {result}")

def test_missing_column(missing_col_df):
    result = pandas_schema_checks(missing_col_df)
    try:
        assert "Price Per Unit" in result["missing_columns"]
        assert result["column_count_match"] == False
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info(f"test_missing_column passed: {result}")

def test_extra_column(extra_col_df):
    result = pandas_schema_checks(extra_col_df)
    try:
        assert "Notes" in result["extra_columns"]
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info(f"test_extra_column passed: {result}")

def test_wrong_dtype(wrong_dtype_df):
    result    = pandas_schema_checks(wrong_dtype_df)
    bad_cols  = [d["column"] for d in result["dtype_issues"]]
    try:
        assert "Quantity" in bad_cols
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info(f"test_wrong_dtype passed: {result}")

def test_column_count_match(clean_df):
    result = pandas_schema_checks(clean_df)
    try:
        assert result["column_count_match"] == True
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info(f"test_column_count_match passed: {result}")

def test_expected_columns_constant():
    try:
        assert len(EXPECTED_COLUMNS) == 8
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info("test_expected_columns_constant passed")


def test_ge_returns_result(clean_df):
    result = ge_schema_checks(clean_df)
    try:
        assert result is not None
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info("test_ge_returns_result passed")

def test_ge_passes_on_clean_data(clean_df):
    result = ge_schema_checks(clean_df)
    try:
        assert result["statistics"]["unsuccessful_expectations"] == 0
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info(f"test_ge_passes_on_clean_data passed: {result['statistics']}")

def test_ge_evaluated_all_expectations(clean_df):
    result    = ge_schema_checks(clean_df)
    evaluated = result["statistics"]["evaluated_expectations"]
    try:
        assert evaluated == 14
    except AssertionError as e:
        pytest.fail(f"Test failed: expected 14 expectations, got {evaluated}")
    logger.info(f"test_ge_evaluated_all_expectations passed: {evaluated}")

def test_ge_detects_missing_column(missing_col_df):
    result = ge_schema_checks(missing_col_df)
    try:
        assert result["statistics"]["unsuccessful_expectations"] > 0
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info("test_ge_detects_missing_column passed")

def test_ge_column_existence_rules(clean_df):
    result     = ge_schema_checks(clean_df)
    exist_rules = [
        r for r in result["results"]
        if r["expectation_config"]["expectation_type"] == "expect_column_to_exist"
    ]
    try:
        assert len(exist_rules) == 8
        for r in exist_rules:
            assert r["success"], \
                f"Column existence failed: {r['expectation_config']['kwargs']['column']}"
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info("test_ge_column_existence_rules passed")

def test_ge_column_count_rule(clean_df):
    result      = ge_schema_checks(clean_df)
    count_rules = [
        r for r in result["results"]
        if r["expectation_config"]["expectation_type"] == "expect_table_column_count_to_equal"
    ]
    try:
        assert len(count_rules) == 1
        assert count_rules[0]["success"]
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info("test_ge_column_count_rule passed")

def test_ge_column_order_rule(clean_df):
    result      = ge_schema_checks(clean_df)
    order_rules = [
        r for r in result["results"]
        if r["expectation_config"]["expectation_type"] == "expect_table_columns_to_match_ordered_list"
    ]
    try:
        assert len(order_rules) == 1
        assert order_rules[0]["success"]
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info("test_ge_column_order_rule passed")

def test_ge_fails_on_wrong_column_order(wrong_order_df):
    result      = ge_schema_checks(wrong_order_df)
    order_rules = [
        r for r in result["results"]
        if r["expectation_config"]["expectation_type"] == "expect_table_columns_to_match_ordered_list"
    ]
    try:
        assert not order_rules[0]["success"]
    except AssertionError as e:
        pytest.fail(f"Test failed: {e}")
    logger.info("test_ge_fails_on_wrong_column_order passed")
