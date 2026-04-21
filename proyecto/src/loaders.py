from pathlib import Path
import pandas as pd
import geopandas as gpd
from dagster import asset, AssetsDefinition
from common_data import DIR, DATASET, FORMAT

def load_dataset(path: Path, format: FORMAT) -> pd.DataFrame:
    full_path = DIR.PROJECT_ROOT / path
    if format == FORMAT.GEOJSON:
        return gpd.read_file(full_path)
    elif format == FORMAT.JSON:
        return pd.read_json(full_path)
    elif format == FORMAT.CSV:
        return pd.read_csv(full_path, sep=",", encoding="utf-8-sig")
    else:
        raise ValueError(f"Formato no soportado: {format}")

def make_load_asset(asset_name: str, path: Path, format: FORMAT) -> AssetsDefinition:
    @asset(name=asset_name)
    def _load() -> pd.DataFrame:
        return load_dataset(path, format)
    return _load

load_assets = [
    make_load_asset("actividad_sc",       DIR.DATA / DATASET.ACTIVIDAD_SC,                  FORMAT.CSV),
    make_load_asset("distribucion_renta", DIR.DATA / DATASET.DISTRIBUCIÓN_RENTA_INGRESOS,   FORMAT.CSV),
    make_load_asset("ocupacion_sc",       DIR.DATA / DATASET.OCUPACION_SC,                  FORMAT.CSV),
    make_load_asset("renta_media_sc",     DIR.DATA / DATASET.RENTA_MEDIA_SC,                FORMAT.CSV),
    make_load_asset("secciones_2021",     DIR.DATA_CATOGRAFIA / DATASET.CARTOGRAFIA_TENERIFE_2021,     FORMAT.GEOJSON),
    make_load_asset("secciones_2022",     DIR.DATA_CATOGRAFIA / DATASET.CARTOGRAFIA_TENERIFE_2022,     FORMAT.GEOJSON),
    make_load_asset("secciones_2023",     DIR.DATA_CATOGRAFIA / DATASET.CARTOGRAFIA_TENERIFE_2023,     FORMAT.GEOJSON),
    make_load_asset("secciones_2024",     DIR.DATA_CATOGRAFIA / DATASET.CARTOGRAFIA_TENERIFE_2024,     FORMAT.GEOJSON),
]