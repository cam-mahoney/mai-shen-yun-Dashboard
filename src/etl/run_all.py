from etl.ingredients_etl import stage_ingredients
from etl.shipments_etl import stage_shipments
from etl.sales_etl import stage_sales
from etl.forecast_etl import stage_forecast
from etl.utils import setup_logger

logger = setup_logger("run_all_etl")

def main():
    logger.info("Starting ETL pipeline...")
    stage_ingredients()
    stage_shipments()
    stage_sales()
    stage_forecast()
    logger.info("ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()