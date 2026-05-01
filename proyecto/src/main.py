from loaders import load_assets, load_checks, load_dataset
from eda import eda_assets, exploratory_analysis
from preprocessing import preprocess_assets, preprocess_checks
from graphics import graphics_assets
from graphics_checks import graphics_checks
from job import pipeline_job, sensors_array
from dagster import Definitions
from common_data import *
from preprocessing import *

#exploratory_analysis(actividad_sc_clean(load_dataset(DIR.DATA / DATASET.ACTIVIDAD_SC, FORMAT.CSV)), DATASET.ACTIVIDAD_SC)
#exploratory_analysis(distribucion_renta_clean(load_dataset(DIR.DATA / DATASET.DISTRIBUCIÓN_RENTA_INGRESOS, FORMAT.CSV)), DATASET.DISTRIBUCIÓN_RENTA_INGRESOS)
#exploratory_analysis(ocupacion_sc_clean(load_dataset(DIR.DATA / DATASET.OCUPACION_SC, FORMAT.CSV)), DATASET.OCUPACION_SC)
#exploratory_analysis(renta_media_clean(load_dataset(DIR.DATA / DATASET.RENTA_MEDIA_SC, FORMAT.CSV)), DATASET.RENTA_MEDIA_SC)

# Cargamos todos los assets, checks y sensores
defs = Definitions(
    assets=[*load_assets, *eda_assets, *preprocess_assets, *graphics_assets],
    asset_checks=[*load_checks, *preprocess_checks, *graphics_checks],
    jobs=[pipeline_job],
    sensors=[*sensors_array]
)