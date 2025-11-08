import pandas as pd
from etl.utils import setup_logger, resolve_path, read_file
from etl.schemas import IngredientSchema

logger = setup_logger("ingredients_etl")

def stage_ingredients():
    raw_path = resolve_path("data/raw/MSY Data - Ingredient.csv")
    output_path = resolve_path("data/staged/ingredients_staged.csv")

    df = read_file(raw_path)
    df.rename(columns={"Item name": "menu_item"}, inplace=True)

    df_long = df.melt(id_vars=["menu_item"], var_name="ingredient_name", value_name="quantity")
    df_long.dropna(subset=["quantity"], inplace=True)
    df_long["ingredient_name"] = df_long["ingredient_name"].str.strip().str.lower()
    df_long["quantity"] = pd.to_numeric(df_long["quantity"], errors="coerce").fillna(0)

    IngredientSchema.validate(df_long)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_long.to_csv(output_path, index=False)
    logger.info(f"Staged ingredients → {output_path}")
    return df_long

if __name__ == "__main__":
    stage_ingredients()
