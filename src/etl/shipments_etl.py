import pandas as pd
from etl.utils import setup_logger, resolve_path, read_file
from etl.schemas import ShipmentSchema

logger = setup_logger("shipments_etl")

def stage_shipments():
    raw_path = resolve_path("data/raw/MSY Data - Shipment.csv")
    output_path = resolve_path("data/staged/shipments_staged.csv")

    df = read_file(raw_path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "expected_date" in df.columns and "arrival_date" in df.columns:
        df["expected_date"] = pd.to_datetime(df["expected_date"], errors="coerce")
        df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce")
        df["delay_days"] = (df["arrival_date"] - df["expected_date"]).dt.days

    ShipmentSchema.validate(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Staged shipments → {output_path}")
    return df

if __name__ == "__main__":
    stage_shipments()
