"""
MSY Inventory Intelligence Internal API
---------------------------------------
Purpose:
    Serve processed inventory, shipment, and forecast data to internal dashboards.

Design Notes:
    - Internal-only service, no public exposure.
    - Stateless; reads from /data/processed/ and /data/staged/.
    - Typed endpoints for predictable JSON schemas.
    - Logging and error handling standardized.
    - Ready for containerization (uvicorn entrypoint).
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from pathlib import Path
import pandas as pd
import logging

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
APP_NAME = "MSY Inventory Intelligence API"
DATA_DIR = Path(__file__).resolve().parents[2] / "data"

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logger = logging.getLogger(APP_NAME)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# FastAPI App Initialization
# -----------------------------------------------------------------------------
app = FastAPI(title=APP_NAME, version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # internal; restrict to intranet subnets for production
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Data Models (Response Schemas)
# -----------------------------------------------------------------------------
class IngredientSummary(BaseModel):
    ingredient_name: str
    total_quantity: float
    month: Optional[str] = None


class ForecastEntry(BaseModel):
    ingredient_name: str
    predicted_usage_next_month: float
    reorder_date: Optional[str] = None
    confidence_interval: Optional[float] = None


class ShipmentEntry(BaseModel):
    ingredient_name: str
    expected_date: Optional[str] = None
    arrival_date: Optional[str] = None
    delay_days: Optional[int] = None
    quantity: Optional[float] = None


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def _read_dataset(filename: str) -> pd.DataFrame:
    """Safely read a CSV file from the data directory."""
    path = DATA_DIR / "processed" / filename
    if not path.exists():
        path = DATA_DIR / "staged" / filename
    if not path.exists():
        logger.error(f"Dataset not found: {filename}")
        raise HTTPException(status_code=404, detail=f"Dataset {filename} not found.")
    df = pd.read_csv(path)
    logger.info(f"Loaded {filename} ({len(df)} rows).")
    return df


# -----------------------------------------------------------------------------
# API Routes
# -----------------------------------------------------------------------------
@app.get("/inventory", response_model=List[IngredientSummary])
def get_inventory(month: Optional[str] = Query(None, description="Filter by month name.")):
    """
    Return ingredient usage summaries.
    Example: /inventory?month=october
    """
    df = _read_dataset("monthly_operations.csv")

    if "ingredient_name" not in df.columns or "quantity" not in df.columns:
        raise HTTPException(status_code=422, detail="monthly_operations.csv missing required columns.")

    if month:
        df = df[df["month"].str.lower() == month.lower()]

    summary = (
        df.groupby(["ingredient_name", "month"], as_index=False)["quantity"].sum()
        .rename(columns={"quantity": "total_quantity"})
    )

    return summary.to_dict(orient="records")


@app.get("/forecast", response_model=List[ForecastEntry])
def get_forecast():
    """
    Return forecasted ingredient usage and reorder insights.
    """
    df = _read_dataset("forecast_ingredients.csv")

    if "ingredient_name" not in df.columns or "predicted_usage_next_month" not in df.columns:
        raise HTTPException(status_code=422, detail="forecast_ingredients.csv missing required columns.")

    return df.to_dict(orient="records")


@app.get("/shipments", response_model=List[ShipmentEntry])
def get_shipments():
    """
    Return shipment and delay data for inventory tracking.
    """
    df = _read_dataset("shipments_clean.csv")

    if "ingredient_name" not in df.columns:
        raise HTTPException(status_code=422, detail="shipments_clean.csv missing required columns.")

    return df.to_dict(orient="records")


@app.get("/health")
def health_check():
    """Simple liveness probe."""
    return {"status": "healthy", "message": "MSY Inventory Intelligence API operational"}


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
    )
