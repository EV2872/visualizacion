from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DIR:
    PROJECT_ROOT: Path = Path(__file__).parent.parent  # proyecto
    DATA: Path = Path(PROJECT_ROOT / 'data/')
    IMAGES: Path = Path(PROJECT_ROOT / 'graficos')

@dataclass(frozen=True)
class DATASET:
    ACTIVIDAD_SC: str = 'actividad-sc-3.csv'
    DISTRIBUCIÓN_RENTA_INGRESOS: str = 'distribucion-renta-ingresos.csv'
    OCUPACION_SC: str = 'ocupacion-sc-3.csv'
    RENTA_MEDIA_SC: str = 'rentamedia-sc-3.csv'