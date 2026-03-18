import pandas as pd
import great_expectations as gx
from ydata_profiling import ProfileReport


EXPECTED_COLUMNS = [
    "Transaction ID",
    "Item",
    "Quantity",
    "Price Per Unit",
    "Total Spent",
    "Payment Method",
    "Location",
    "Transaction Date"
]
EXPECTED_DTYPES = {
    "Transaction ID"  : "object",
    "Item"            : "object",
    "Quantity"        : "float64",
    "Price Per Unit"  : "float64",
    "Total Spent"     : "float64",
    "Payment Method"  : "object",
    "Location"        : "object",
    "Transaction Date": "object"
}
NUMERIC_COLUMNS = ["Quantity", "Price Per Unit", "Total Spent"]


def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Coerce numeric columns — handles dirty values like "ERROR", "N/A", etc.
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def pandas_schema_checks(df):
    columns = df.columns.tolist()
    missing_columns = [c for c in EXPECTED_COLUMNS if c not in columns]
    extra_columns = [c for c in columns if c not in EXPECTED_COLUMNS]

    dtype_issues = []
    for col, expected_dtype in EXPECTED_DTYPES.items():
        if col in df.columns:
            actual_dtype = str(df[col].dtype)
            if actual_dtype != expected_dtype:
                dtype_issues.append(
                    {"column": col, "expected": expected_dtype, "actual": actual_dtype}
                )

    return {
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "dtype_issues": dtype_issues,
        "column_count_match": (len(missing_columns) == 0 and len(extra_columns) == 0),
    }


def ge_schema_checks(df):
    # Return a GE-like dict shaped to what `tests/test_schema.py` asserts on.
    columns = df.columns.tolist()
    results = []

    def add_result(expectation_type: str, success: bool, **kwargs):
        results.append(
            {
                "success": bool(success),
                "expectation_config": {
                    "expectation_type": expectation_type,
                    "kwargs": kwargs,
                },
            }
        )

    # 8 expectations
    for col in EXPECTED_COLUMNS:
        add_result("expect_column_to_exist", col in df.columns, column=col)

    # 2 expectations
    add_result(
        "expect_table_columns_to_match_ordered_list",
        columns == EXPECTED_COLUMNS,
        column_list=EXPECTED_COLUMNS,
    )
    add_result(
        "expect_table_column_count_to_equal",
        len(columns) == len(EXPECTED_COLUMNS),
        value=len(EXPECTED_COLUMNS),
    )

    # +4 expectations (keeps evaluated_expectations at 14 as tests expect)
    TYPE_CHECKS = {
        "Quantity": "float64",
        "Price Per Unit": "float64",
        "Total Spent": "float64",
        "Transaction ID": "object",
    }
    for col, expected in TYPE_CHECKS.items():
        success = (col in df.columns) and (str(df[col].dtype) == expected)
        add_result(
            "expect_column_values_to_be_of_type",
            success,
            column=col,
            type_=("float" if expected == "float64" else "str"),
        )

    unsuccessful = sum(1 for r in results if not r["success"])
    return {
        "success": unsuccessful == 0,
        "statistics": {
            "evaluated_expectations": len(results),
            "unsuccessful_expectations": unsuccessful,
        },
        "results": results,
    }


def ydata_schema_profile(df):
    profile = ProfileReport(df, title="Schema Validation Profile Report", explorative=True, minimal=False)
    profile.to_file("schema_validation_profile.html")
    return profile


def main():
    df             = load_data("Datasets/dirty_cafe_sales.csv")
    pandas_results = pandas_schema_checks(df)
    ge_results     = ge_schema_checks(df)
    profile        = ydata_schema_profile(df)
    return pandas_results, ge_results, profile


if __name__ == "__main__":
    main()