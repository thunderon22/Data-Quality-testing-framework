dq_framework/
│
├── data/
│   ├── cafe_sales_dirty.csv          # raw dirty dataset
│   ├── reference.csv                 # first half — baseline for drift
│   └── current.csv                   # second half — new batch for drift
│
├── config/
│   ├── schema_config.yaml            # expected columns, dtypes, constraints
│   ├── ge_expectations.json          # great expectations suite file
│   └── drift_config.yaml             # evidently drift thresholds
│
├── validators/
│   ├── schema_validator.py           # metric 1 — pandas + GE
│   ├── completeness_validator.py     # metric 2 — pandas + GE + evidently
│   ├── duplicate_detector.py         # metric 3 — pandas + GE
│   ├── text_quality_validator.py     # metric 4 — pandas + GE
│   ├── length_validator.py           # metric 5 — pandas + GE
│   ├── metadata_validator.py         # metric 6 — pandas + GE + evidently
│   ├── parsing_validator.py          # metric 7 — pandas + GE
│   └── drift_detector.py             # metric 8 — pandas + evidently
│
├── profiler/
│   ├── ydata_profiler.py             # generates full HTML profile report
│   └── compare_profiles.py           # side-by-side comparison of two datasets
│
├── pipeline/
│   ├── run_all_checks.py             # master runner — calls all validators
│   └── generate_summary.py          # aggregates all results into pass/fail summary
│
├── reports/                          # auto-generated, do not edit manually
│   ├── profile_report.html           # ydata profiling output
│   ├── drift_report.html             # evidently drift report
│   ├── quality_report.html           # evidently quality report
│   └── ge_validation_results.json    # great expectations run log
│
├── tests/
│   ├── conftest.py                   # shared pytest fixtures (loads df once)
│   ├── test_schema.py                # pytest — tests schema_validator.py
│   ├── test_completeness.py          # pytest — tests completeness_validator.py
│   ├── test_duplicates.py            # pytest — tests duplicate_detector.py
│   ├── test_text_quality.py          # pytest — tests text_quality_validator.py
│   ├── test_length.py                # pytest — tests length_validator.py
│   ├── test_metadata.py              # pytest — tests metadata_validator.py
│   ├── test_parsing.py               # pytest — tests parsing_validator.py
│   └── test_drift.py                 # pytest — tests drift_detector.py
│
├── requirements.txt                  # all dependencies
├── README.md                         # setup and usage guide
└── .env                              # api keys, file paths (never commit this)