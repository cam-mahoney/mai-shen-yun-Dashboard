import pandas as pd
from pathlib import Path
from etl.utils import setup_logger, resolve_path, read_file
from etl.schemas import SalesSchema

logger = setup_logger("sales_etl")

def stage_sales():
    raw_dir = resolve_path("data/raw/")
    output_path = resolve_path("data/staged/sales_staged.csv")

    files = sorted(Path(raw_dir).glob("*Data_Matrix*.xlsx"))
    all_data = []

    for file in files:
        try:
            df = read_file(file)
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            month = file.stem.split("_")[0].lower()
            df["month"] = month
            all_data.append(df)
            logger.info(f"Processed {file.name} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"Failed {file.name}: {e}")

    combined = pd.concat(all_data, ignore_index=True)
    SalesSchema.validate(combined)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_path, index=False)
    logger.info(f"Staged sales → {output_path}")
    return combined

if __name__ == "__main__":
    stage_sales()
