import pandas as pd
from dagster import asset, Output, Definitions, load_assets_from_current_module, load_asset_checks_from_current_module
from src.data_loading import *
from src.graphics import *
from src.graphics_generator import *

@asset_check(asset=renta_canarias)
def check_datos_para_graficos_canarias(df: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed=len(df) > 0,
        metadata={"num_filas": len(df)}
    )

@asset(deps=[renta_canarias])
def grafico_lineas_canarias(renta_canarias: pd.DataFrame) -> ggplot:
    return crearGraficoDeLineas(
        renta_canarias,
        ruta="evolucion_renta_canarias",
        titulo="Evolución de la composición de la renta en Canarias",
        subtitulo="Por tipo de ingreso (porcentaje sobre la renta total)",
        x=("anio", "Año"),
        y=("porcentaje", "Porcentaje (%)"),
        color=("medida", "Tipo de ingreso"),
        group="medida"
    )

@asset_check(asset=grafico_lineas_canarias)
def check_eje_y_desde_cero(grafico) -> AssetCheckResult:
    y_scale = grafico.scales.get_scales('y')
    y_min = y_scale.limits[0] if y_scale and y_scale.limits else None
    passed = y_min is not None and y_min >= 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "valor_inicio_eje_y": float(y_min) if y_min is not None else "no definido",
            "error_perceptivo": "Nulo" if passed else "Alto"
        }
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

@asset_check(asset=renta_municipios_enriquecida)
def check_datos_gomera(df: pd.DataFrame) -> AssetCheckResult:
    df_gomera = df[df["isla_code"].astype(str) == "381"]
    municipios = df_gomera["nombre_municipio"].nunique()
    return AssetCheckResult(
        passed=municipios > 1,
        metadata={"municipios_distintos": municipios}
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
def grafico_nivel_estudios_mujeres(nivel_estudios_df: pd.DataFrame) -> ggplot:
    return crearGraficoBarrasNivelEstudiosMujeres(
        nivel_estudios_df,
        ruta="barras_nivel_estudios_mujeres",
        titulo="Nivel de estudios en mujeres en Canarias",
        subtitulo="Evolución por año"
    )

@asset_check(asset=grafico_nivel_estudios_mujeres)
def check_orden_magnitud_barras(grafico) -> AssetCheckResult:
    data = grafico.data
    valores = (
        data.groupby("Nivel de estudios en curso")["Total"]
        .sum()
        .tolist()
    )
    sorted_vals = sorted(valores, reverse=True)
    return AssetCheckResult(
        passed=valores == sorted_vals,
        metadata={
            "orden_detectado": [int(v) for v in valores],
            "orden_esperado": [int(v) for v in sorted_vals],
            "is_sorted": bool(valores == sorted_vals)
        }
    )


# ── GRÁFICOS GENERADOS POR IA ──────────────────────────────────
@asset
def islas_raw() -> pd.DataFrame:
    return pd.read_csv("./data/pwbi-1.csv")

@asset
def template_ia_islas(islas_raw):
    return generic_template_ia(
        df=islas_raw,
        descripcion=(
            "Gráfico de líneas de la evolución del gasto por isla. "
            "IMPORTANTE: NO uses coord_flip(). "
            "Resalta Tenerife con color destacado y el resto en gris (#D3D3D3) "
            "usando scale_color_manual."
        )
    )

@asset
def codigo_generado_ia_islas(context, template_ia_islas):
    return llamar_ia_y_limpiar(context, template_ia_islas)

@asset
def visualizacion_islas(context, codigo_generado_ia_islas, islas_raw):
    return ejecutar_y_guardar(context, codigo_generado_ia_islas, islas_raw, "visualizacion_ia_islas.png")


@asset
def template_ia_canarias(renta_canarias):
    return generic_template_ia(
        df=renta_canarias,
        descripcion=(
            "Gráfico de líneas con puntos de la evolución de la renta en Canarias "
            "por tipo de ingreso. "
            "REGLAS ESTRICTAS: "
            "1. NO uses coord_flip(). "
            "2. El theme() SIEMPRE va dentro del paréntesis de ggplot con +, NUNCA solo. "
            "3. element_text() solo se usa DENTRO de theme(), jamás como capa separada. "
            "4. La estructura debe ser: ggplot(...) + geom_line() + labs() + theme_minimal() + theme(...). "
            "5. Usa scale_color_brewer() para los colores. "
            "6. Rota el eje X con: theme(axis_text_x=element_text(rotation=45)). "
        )
    )

@asset
def codigo_generado_ia_canarias(context, template_ia_canarias):
    return llamar_ia_y_limpiar(context, template_ia_canarias)

@asset
def visualizacion_canarias(context, codigo_generado_ia_canarias, renta_canarias):
    return ejecutar_y_guardar(context, codigo_generado_ia_canarias, renta_canarias, "visualizacion_ia_canarias.png")


# ── PROVINCIAS (IA) ────────────────────────────────────────────
@asset
def template_ia_provincias(renta_provincias):
    return generic_template_ia(
        df=renta_provincias,
        descripcion=(
            "Gráfico de áreas apiladas que muestra la evolución de la composición "
            "de la renta por tipo de ingreso, separando las dos provincias canarias. "
            "REGLAS ESTRICTAS: "
            "1. NO uses coord_flip(). "
            "2. Usa facet_wrap con la columna 'municipio' para separar provincias. "
            "3. Mapea 'anio' al eje X, 'porcentaje' al eje Y, 'medida' al fill. "
            "4. Usa geom_area(alpha=0.85, position='stack'). "
            "5. Estructura: ggplot(...) + geom_area(...) + labs(...) + theme_minimal() + theme(...). "
            "6. theme(axis_text_x=element_text(rotation=45)) para rotar eje X. "
            "7. Usa scale_fill_brewer(type='qual', palette='Set2') para los colores. "
        )
    )

@asset
def codigo_generado_ia_provincias(context, template_ia_provincias):
    return llamar_ia_y_limpiar(context, template_ia_provincias)

@asset
def visualizacion_provincias(context, codigo_generado_ia_provincias, renta_provincias):
    return ejecutar_y_guardar(
        context,
        codigo_generado_ia_provincias,
        renta_provincias,
        "visualizacion_ia_provincias.png"
    )


# ── NIVEL ESTUDIOS (IA) ────────────────────────────────────────
@asset
def template_ia_estudios(nivel_estudios_df):
    return generic_template_ia(
        df=nivel_estudios_df,
        descripcion=(
            "Gráfico de barras agrupadas que muestra la evolución del nivel de estudios "
            "en Canarias por año, diferenciando por sexo. "
            "REGLAS ESTRICTAS: "
            "1. NO uses coord_flip(). "
            "2. Extrae el año de la columna 'Periodo' con: df['Año'] = df['Periodo'].dt.year. "
            "3. Mapea 'factor(Año)' al eje X, 'Total' al eje Y, 'Sexo' al fill. "
            "4. Usa geom_col(position='dodge', alpha=0.85). "
            "5. Estructura: ggplot(...) + geom_col(...) + labs(...) + theme_minimal() + theme(...). "
            "6. theme(axis_text_x=element_text(rotation=45)) para rotar eje X. "
            "7. Usa scale_fill_brewer(type='qual', palette='Set1') para los colores. "
        )
    )

@asset
def codigo_generado_ia_estudios(context, template_ia_estudios):
    return llamar_ia_y_limpiar(context, template_ia_estudios)

@asset
def visualizacion_estudios(context, codigo_generado_ia_estudios, nivel_estudios_df):
    return ejecutar_y_guardar(
        context,
        codigo_generado_ia_estudios,
        nivel_estudios_df,
        "visualizacion_ia_estudios.png"
    )

# Cargamos todos los assets y checks
defs = Definitions(
    assets=load_assets_from_current_module(),
    asset_checks=load_asset_checks_from_current_module()
)