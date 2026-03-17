import shutil
from pathlib import Path

import kagglehub
import pandas as pd


def main() -> None:
    dataset_path = Path(
        kagglehub.dataset_download(
            "ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training"
        )
    )

    src_csv = dataset_path / "dirty_cafe_sales.csv"
    if not src_csv.exists():
        raise FileNotFoundError(f"Expected file not found: {src_csv}")

    out_dir = Path(__file__).resolve().parent
    dst_csv = out_dir / "dirty_cafe_sales.csv"
    shutil.copyfile(src_csv, dst_csv)

    df = pd.read_csv(dst_csv)
    print(df.shape)
    print(df.head())


if __name__ == "__main__":
    main()

