from loaders import load_assets
from eda import eda_assets
from dagster import (
    sensor, RunRequest, define_asset_job, AssetSelection, SensorEvaluationContext, 
    asset, Output, Definitions, load_assets_from_current_module, load_asset_checks_from_current_module
)

# JOB: todos los assets
pipeline_job = define_asset_job(
    name="pipeline_completo",
    selection=AssetSelection.all()
)

# Cargamos todos los assets y checks
defs = Definitions(
    assets=[*load_assets, *eda_assets],
    asset_checks=load_asset_checks_from_current_module(),
    jobs=[pipeline_job],
    #sensors=[sensor_cambios_data]
)