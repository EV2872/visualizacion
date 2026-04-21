from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DIR:
    PROJECT_ROOT: Path = Path(__file__).parent.parent  # proyecto
    DATA: Path = Path(PROJECT_ROOT / 'data/')
    DATA_CATOGRAFIA: Path = Path(DATA / 'cartografia-secciones/')
    IMAGES: Path = Path(PROJECT_ROOT / 'graficos/')

@dataclass(frozen=True)
class FORMAT:
    CSV: str = 'csv'
    JSON: str = 'json'
    GEOJSON = "geojson"

@dataclass(frozen=True)
class DATASET:
    ACTIVIDAD_SC: str = 'actividad-sc-3.csv'
    DISTRIBUCIÓN_RENTA_INGRESOS: str = 'distribucion-renta-ingresos.csv'
    OCUPACION_SC: str = 'ocupacion-sc-3.csv'
    RENTA_MEDIA_SC: str = 'rentamedia-sc-3.csv'
    CARTOGRAFIA_TENERIFE_2021: str = 'secciones_20210101_tenerife.json'
    CARTOGRAFIA_TENERIFE_2022: str = 'secciones_20220101_tenerife.json'
    CARTOGRAFIA_TENERIFE_2023: str = 'secciones_20230101_tenerife.json'
    CARTOGRAFIA_TENERIFE_2024: str = 'secciones_20240101_tenerife.json'