from pathlib import Path
import pandas as pd
from dagster import asset, AssetsDefinition
from common_data import DIR, DATASET

def load_dataset(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        DIR.PROJECT_ROOT / path,
        sep=",",
        encoding="latin-1"
    )

def make_load_asset(asset_name: str, path: Path) -> AssetsDefinition:
    @asset(name=asset_name)
    def _load() -> pd.DataFrame:
        return load_dataset(path)
    return _load

load_assets = [
    make_load_asset("actividad_sc",       DIR.DATA / DATASET.ACTIVIDAD_SC),
    make_load_asset("distribucion_renta", DIR.DATA / DATASET.DISTRIBUCIÓN_RENTA_INGRESOS),
    make_load_asset("ocupacion_sc",       DIR.DATA / DATASET.OCUPACION_SC),
    make_load_asset("renta_media_sc",     DIR.DATA / DATASET.RENTA_MEDIA_SC),
]