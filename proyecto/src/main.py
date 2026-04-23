from loaders import load_assets, load_dataset
from eda import eda_assets, exploratory_analysis
from preprocessing import preprocess_assets, preprocess_checks
from dagster import (
    sensor, RunRequest, define_asset_job, AssetSelection, SensorEvaluationContext, 
    asset, Output, Definitions, load_assets_from_current_module, load_asset_checks_from_current_module
)

from common_data import *
from preprocessing import *
exploratory_analysis(actividad_sc_clean(load_dataset(DIR.DATA / DATASET.ACTIVIDAD_SC, FORMAT.CSV)), DATASET.ACTIVIDAD_SC)
exploratory_analysis(distribucion_renta_clean(load_dataset(DIR.DATA / DATASET.DISTRIBUCIÓN_RENTA_INGRESOS, FORMAT.CSV)), DATASET.DISTRIBUCIÓN_RENTA_INGRESOS)
exploratory_analysis(ocupacion_sc_clean(load_dataset(DIR.DATA / DATASET.OCUPACION_SC, FORMAT.CSV)), DATASET.OCUPACION_SC)
exploratory_analysis(renta_media_clean(load_dataset(DIR.DATA / DATASET.RENTA_MEDIA_SC, FORMAT.CSV)), DATASET.RENTA_MEDIA_SC)

# JOB: todos los assets
pipeline_job = define_asset_job(
    name="pipeline_completo",
    selection=AssetSelection.all()
)

# Cargamos todos los assets y checks
defs = Definitions(
    assets=[*load_assets, *eda_assets, *preprocess_assets],
    asset_checks=[*preprocess_checks],
    jobs=[pipeline_job],
    #sensors=[sensor_cambios_data]
)