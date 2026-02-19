import pandas as pd
from dagster import asset
from src.data_loading import *
from src.graphics import *

@asset(deps=[renta_canarias])
def grafico_lineas_canarias(renta_canarias: pd.DataFrame) -> None:
    crearGraficoDeLineas(
        renta_canarias,
        ruta="evolucion_renta_canarias",
        titulo="Evolución de la composición de la renta en Canarias",
        subtitulo="Por tipo de ingreso (porcentaje sobre la renta total)",
        x=("anio", "Año"),
        y=("porcentaje", "Porcentaje (%)"),
        color=("medida", "Tipo de ingreso"),
        group="medida"
    )

@asset(deps=[renta_canarias])
def grafico_areas_canarias(renta_canarias: pd.DataFrame) -> None:
    crearGraficoDeAreas(
        renta_canarias,
        ruta="area_apilada_renta_canarias",
        titulo="Evolución de la composición de la renta en Canarias",
        subtitulo="Por tipo de ingreso (porcentaje sobre la renta total)",
        x=("anio", "Año"),
        y=("porcentaje", "Porcentaje (%)"),
        fill=('medida', 'Tipo de ingreso')
    )

@asset(deps=[renta_provincias])
def grafico_lineas_gran_canaria(renta_provincias: pd.DataFrame) -> None:
    df = renta_provincias[
        renta_provincias["TERRITORIO_CODE"] == "ES701"
    ]

    crearGraficoDeLineas(
        df,
        ruta="evolucion_renta_gran_canaria",
        titulo="Evolución de la composición de la renta en la provincia Gran Canaria",
        subtitulo="Por tipo de ingreso (porcentaje sobre la renta total)",
        x=("anio", "Año"),
        y=("porcentaje", "Porcentaje (%)"),
        color=("medida", "Tipo de ingreso"),
        group="medida"
    )

@asset(deps=[renta_provincias])
def grafico_areas_gran_canaria(renta_provincias: pd.DataFrame) -> None:
    df = renta_provincias[
        renta_provincias["TERRITORIO_CODE"] == "ES701"
    ]

    crearGraficoDeAreas(
        df,
        ruta="area_apilada_renta_grancanaria",
        titulo="Evolución de la composición de la renta en la provincia de Gran Canaria",
        subtitulo="Por tipo de ingreso (porcentaje sobre la renta total)",
        x=("anio", "Año"),
        y=("porcentaje", "Porcentaje (%)"),
        fill=('medida', 'Tipo de ingreso')
    )

@asset(deps=[renta_provincias])
def grafico_lineas_tenerife(renta_provincias: pd.DataFrame) -> None:
    df = renta_provincias[
        renta_provincias["TERRITORIO_CODE"] == "ES702"
    ]

    crearGraficoDeLineas(
        df,
        ruta="evolucion_renta_tenerife",
        titulo="Evolución de la composición de la renta en la provincia de Tenerife",
        subtitulo="Por tipo de ingreso (porcentaje sobre la renta total)",
        x=("anio", "Año"),
        y=("porcentaje", "Porcentaje (%)"),
        color=("medida", "Tipo de ingreso"),
        group="medida"
    )

@asset(deps=[renta_provincias])
def grafico_areas_tenerife(renta_provincias: pd.DataFrame) -> None:
    df = renta_provincias[
        renta_provincias["TERRITORIO_CODE"] == "ES702"
    ]

    crearGraficoDeAreas(
        df,
        ruta="area_apilada_renta_tenerife",
        titulo="Evolución de la composición de la renta en la provincia de Tenerife",
        subtitulo="Por tipo de ingreso (porcentaje sobre la renta total)",
        x=("anio", "Año"),
        y=("porcentaje", "Porcentaje (%)"),
        fill=('medida', 'Tipo de ingreso')
    )

# Mapa de calor sobre año y municipios donde cada cuadrado representa
# como varía en el tiempo el porcentaje de Sueldos y salarios en La Gomera
@asset(deps=[renta_municipios_enriquecida])
def grafico_heatmap_municipios_gomera(renta_municipios_enriquecida: pd.DataFrame) -> None:
    df_gomera = renta_municipios_enriquecida[
        renta_municipios_enriquecida["isla_code"].astype(str) == "381"
    ].copy()
    df_sueldos = df_gomera[df_gomera["medida"] == "Sueldos y salarios"].copy()
    crearMapaDeCalor(
        df_sueldos,
        ruta="heatmap_sueldos_municipios_gomera",
        titulo="Mapa de calor: Sueldos y salarios en La Gomera",
        subtitulo="Evolución por municipio (porcentaje sobre renta total)",
        x=("anio", "Año"),
        y=("nombre_municipio", "Municipio"),
        fill=("porcentaje", "Porcentaje (%)")
    )

@asset(deps=[nivel_estudios_df])
def grafico_nivel_estudios_mujeres(nivel_estudios_df: pd.DataFrame) -> None:
    crearGraficoBarrasNivelEstudiosMujeres(
        nivel_estudios_df,
        ruta="barras_nivel_estudios_mujeres",
        titulo="Nivel de estudios en mujeres en Canarias",
        subtitulo="Evolución por año"
    )