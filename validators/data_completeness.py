import pandas as pd
import great_expectations as gx
import yaml
import logging
logging.basicConfig(level=logging.INFO)

def load_config():
    with open("config/schema_config.yaml") as f:
        return yaml.safe_load(f)["completeness_thresholds"]

def pandas_completeness_checks(df):
    config = load_config()
    critical_columns = config["critical_columns"]
    high_importance_columns = config["high_importance"]["columns"]
    low_importance_columns = config["low_importance"]["columns"]
    low_max_pct     = config["low_importance"]["max_null_pct"]
    high_max_pct    = config["high_importance"]["max_null_pct"]
    null_counts     = df.isnull().sum()
    null_pct        = (df.isnull().mean() * 100).round(2)
    results = {
        "null_counts"       : null_counts.to_dict(),
        "null_pct"          : null_pct.to_dict(),
        "critical_failures" : [],
        "high_failures"     : [],
        "low_failures"      : [],
        "passed_columns"    : []
    }

    #critical columns check 
    for col in critical_columns:
        if col in df.columns:
            count = null_counts[col]
            if null_counts[col]>0:
                results["critical_failures"].append({"critical_column":col, "null_percentage":null_pct[col] , "null_count" : count , "severity":"Critical"})
            else:
                results["passed_columns"].append({"critical_column":col, "null_percentage":null_pct[col] , "null_count" : count , "severity":"Critical"})

    #high imapct cols check 
    for col in high_importance_columns:
        if col in df.columns:
            if null_pct[col]>high_max_pct:
                results["high_failures"].append({"high_column":col, "null_percentage":null_pct[col] , "null_count" : null_counts[col] , "severity":"High"})
            else:
                results["passed_columns"].append({"high_column":col, "null_percentage":null_pct[col] , "null_count" : null_counts[col] , "severity":"High"})

    #low imapct cols check 
    for col in low_importance_columns:
        if col in df.columns:
            if null_pct[col]>low_max_pct:
                results["low_failures"].append({"low_column":col, "null_percentage":null_pct[col] , "null_count" : null_counts[col] , "severity":"Low"})
            else:
                results["passed_columns"].append({"low_column":col, "null_percentage":null_pct[col] , "null_count" : null_counts[col] , "severity":"Low"})
    total_failures = (
        len(results["critical_failures"]) +
        len(results["high_failures"])     +
        len(results["low_failures"])
    )

    if total_failures > 0:
        logging.warning(f"Data completeness checks failed for {total_failures} columns")
    else:
        logging.info("Data completeness checks passed")
    return results


def ge_completeness_checks(df):
    config = load_config()
    try:
        # Minimal, GE-version-agnostic completeness evaluation using pandas.
        results = []

        def add(success: bool, column: str, mostly=None):
            kwargs = {"column": column}
            if mostly is not None:
                kwargs["mostly"] = mostly
            results.append(
                {
                    "success": bool(success),
                    "expectation_config": {
                        "expectation_type": "expect_column_values_not_to_be_null",
                        "kwargs": kwargs,
                    },
                }
            )

        for col in config["critical_columns"]:
            if col in df.columns:
                add(int(df[col].isnull().sum()) == 0, col)

        mostly = 1 - config["high_importance"]["max_null_pct"] / 100
        for col in config["high_importance"]["columns"]:
            if col in df.columns:
                add((1 - float(df[col].isnull().mean())) >= mostly, col, mostly=mostly)

        mostly = 1 - config["low_importance"]["max_null_pct"] / 100
        for col in config["low_importance"]["columns"]:
            if col in df.columns:
                add((1 - float(df[col].isnull().mean())) >= mostly, col, mostly=mostly)

        unsuccessful = sum(1 for r in results if not r["success"])
        return {
            "success": unsuccessful == 0,
            "statistics": {
                "evaluated_expectations": len(results),
                "unsuccessful_expectations": unsuccessful,
            },
            "results": results,
        }
    except Exception as e:
        return {"error": type(e).__name__, "message": str(e)}

    
    
if __name__ == "__main__":
    df = pd.read_csv("Datasets/dirty_cafe_sales.csv")
    pandas_results = pandas_completeness_checks(df)
    ge_results = ge_completeness_checks(df)
    print(pandas_results)
    print(ge_results)

