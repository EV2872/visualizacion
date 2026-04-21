from dagster import AssetIn, AssetsDefinition, asset
from typing import Callable
import pandas as pd
from common_data import DATASET

def exploratory_analysis(df: pd.DataFrame, name: str) -> None:
    print(f'============== Exploratory analysis of dataset: {name} ==============')
    print('Description')
    print(df.describe())
    print('Tipos de las variables')
    print(df.dtypes)
    for column in df.columns:
        print(f"Unique values in '{column}': {df[column].unique()}")   
    print('Valores nulos')
    print(df.isnull())

def make_eda_asset(asset_name: str, dataset_name: str) -> AssetsDefinition:
    @asset(name=asset_name, ins={"df": AssetIn(dataset_name)})
    def _eda(df: pd.DataFrame) -> None:
        exploratory_analysis(df, dataset_name)
    return _eda

eda_assets = [
    make_eda_asset("eda_actividad",    "actividad_sc"),
    make_eda_asset("eda_distribucion", "distribucion_renta"),
    make_eda_asset("eda_ocupacion",    "ocupacion_sc"),
    make_eda_asset("eda_renta",        "renta_media_sc"),
]