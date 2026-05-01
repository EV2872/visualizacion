import os
from dagster import AssetSelection, RunRequest, SensorEvaluationContext, define_asset_job, sensor

# JOB: todos los assets
pipeline_job = define_asset_job(
    name="pipeline_completo",
    selection=AssetSelection.all()
)

# vigila cambios en la carpeta data/
@sensor(job=pipeline_job, minimum_interval_seconds=30)
def sensor_cambios_data(context: SensorEvaluationContext):
    data_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../data")
    )
    # Calcular el mtime más reciente de todos los ficheros en data/
    mtimes = []
    for fichero in os.listdir(data_dir):
        ruta = os.path.join(data_dir, fichero)
        if os.path.isfile(ruta):
            mtimes.append(os.path.getmtime(ruta))
    if not mtimes:
        return
    ultimo_cambio = str(max(mtimes))
    ultimo_conocido = context.cursor or "0"
    if ultimo_cambio != ultimo_conocido:
        context.update_cursor(ultimo_cambio)
        yield RunRequest(run_key=ultimo_cambio)

sensors_array = [sensor_cambios_data]