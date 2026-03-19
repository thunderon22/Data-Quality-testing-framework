import pandas as pd
import great_expectations as gx
import yaml
import logging
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

with open("config/duplicate_config.yaml" , "r") as f:
    duplicate_config = yaml.safe_load(f)
    # print(duplicate_config)
unique_columns = duplicate_config["duplicate_detection"]["unique_columns"]
near_duplicate_subsets = duplicate_config["duplicate_detection"]["near_duplicate_subsets"]
semantic_check_columns = duplicate_config["duplicate_detection"]["semantic_check_columns"]
semantic_detection = duplicate_config["duplicate_detection"]["semantic_detection"]
sentinel_values = duplicate_config["duplicate_detection"]["sentinel_values"]
print(near_duplicate_subsets)

def pandas_duplicate_detection(df):
    # define the result format we return from this function 
    results = {
        "exact_duplicates"    : {},
        "near_duplicates"     : {},
        "semantic_duplicates" : {},
        "sentinel_findings"   : {},
        "status"              : "PASS"
    }

    # 1. exact duplicates detection 
    doop_count = df.duplicated().sum()
    if doop_count>0:
        results["exact_duplicates"] = doop_count,
        results["status"] = "FAIL"
    else:
        results["exact_duplicates"] = doop_count
        results["status"] = "PASS"
    logging.info(f"The given data frame has passed the exact duplicated rows testing")

    # 2 . near duplicates detection
    for subset in near_duplicate_subsets:
        subset_name = subset["name"]
        subset_columns = subset["columns"]
        near_duplicate_count = df.duplicated(subset=subset_columns, keep=False).sum()
        if near_duplicate_count>0:
            results["near_duplicates"][subset_name] = near_duplicate_count
            results["status"] = "FAIL"
        else:
            results["near_duplicates"][subset_name] = near_duplicate_count
            results["status"] = "PASS"
    logging.info(f"The given data frame has passed the near duplicated rows testing")
    
    # 3. semantic duplicates detection
    for column in semantic_check_columns:
        semantic_duplicate_count = df[column].value_counts().sum()
        if semantic_duplicate_count>0:
            results["semantic_duplicates"][column] = semantic_duplicate_count
            results["status"] = "FAIL"
        else:
            results["semantic_duplicates"][column] = semantic_duplicate_count
            results["status"] = "PASS"
    logging.info(f"The given data frame has passed the semantic duplicated rows testing")
    
    # 4. sentinel values detection
    for value in sentinel_values:
        sentinel_duplicate_count = int((df == value).sum().sum())
        if sentinel_duplicate_count > 0:
            results["sentinel_findings"][value] = sentinel_duplicate_count
            results["status"] = "FAIL"
        else:
            results["sentinel_findings"][value] = sentinel_duplicate_count
            results["status"] = "PASS"
    logging.info(f"The given data frame has passed the sentinel values testing")
    
    return results

def ge_duplicate_detection(df):
    context = gx.get_context()

    suite_name = "ge_duplicate_detection"

    # GX 1.x — suites managed via context.suites
    try:
        suite = context.suites.get(suite_name)
    except Exception:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    # GX 1.x datasource API
    data_source = context.data_sources.add_pandas(name="pandas_datasource")
    asset = data_source.add_dataframe_asset(name="duplicate_detection")
    batch_definition = asset.add_batch_definition_whole_dataframe("batch_def")

    # Add expectations directly to suite
    for col in unique_columns:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeUnique(column=col)
        )

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1,
            max_value=len(df)
        )
    )

    for subset_config in near_duplicate_subsets:
        cols = [c for c in subset_config["columns"] if c in df.columns]
        if cols:
            suite.add_expectation(
                gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=cols)
            )

    # Create validation definition and run
    try:
        validation_def = context.validation_definitions.get("duplicate_validation")
    except Exception:
        validation_def = context.validation_definitions.add(
            gx.ValidationDefinition(
                name="duplicate_validation",
                data=batch_definition,
                suite=suite,
            )
        )

    results = validation_def.run(batch_parameters={"dataframe": df})

    passed = sum(1 for r in results.results if r.success)
    failed = sum(1 for r in results.results if not r.success)

    logger.info(
        f"GE duplicate checks done — "
        f"passed: {passed} "
        f"failed: {failed}"
    )
    return results

if __name__ == "__main__":
    df = pd.read_csv("Datasets/dirty_cafe_sales.csv")
    results = pandas_duplicate_detection(df)
    ge_results = ge_duplicate_detection(df)
    print(ge_results)
    print(ge_results["statistics"]["successful_expectations"])
    print(ge_results["statistics"]["unsuccessful_expectations"])





