import pandera as pa
from pandera import Column, DataFrameSchema

IngredientSchema = DataFrameSchema({
    "menu_item": Column(str),
    "ingredient_name": Column(str),
    "quantity": Column(float)
})

ShipmentSchema = DataFrameSchema({
    "ingredient_name": Column(str),
    "expected_date": Column(pa.DateTime, nullable=True),
    "arrival_date": Column(pa.DateTime, nullable=True),
    "quantity": Column(float, nullable=True),
    "delay_days": Column(float, nullable=True)
})

SalesSchema = DataFrameSchema({
    "month": Column(str),
    "menu_item": Column(str),
    "ingredient_name": Column(str),
    "quantity": Column(float)
})
