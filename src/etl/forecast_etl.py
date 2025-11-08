import pandas as pd
from etl.utils import setup_logger, resolve_path
from etl.schemas import IngredientSchema

logger = setup_logger("forecast_etl")

def stage_forecast():
    sales_path = resolve_path("data/staged/sales_staged.csv")
    recipe_path = resolve_path("data/staged/ingredients_staged.csv")
    output_path = resolve_path("data/staged/forecast_ingredients.csv")

    sales = pd.read_csv(sales_path)
    recipe = pd.read_csv(recipe_path)

    df = sales.merge(recipe, how="left", left_on="menu_item", right_on="menu_item")
    df["forecast_usage"] = df["quantity"] * df["quantity_y"]  # naive projection
    forecast = df.groupby("ingredient_name", as_index=False)["forecast_usage"].sum()
    forecast.rename(columns={"forecast_usage": "predicted_usage_next_month"}, inplace=True)

    IngredientSchema.validate(forecast)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    forecast.to_csv(output_path, index=False)
    logger.info(f"Staged forecast → {output_path}")
    return forecast

if __name__ == "__main__":
    stage_forecast()
