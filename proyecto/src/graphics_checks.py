from dagster import asset_check, AssetCheckResult
from plotnine import geom_line, geom_col, geom_boxplot, geom_tile, geom_point
from graphics import (
    grafico_evolucion_actividad_total,
    grafico_actividad_por_isla,
    grafico_evolucion_ocupacion_total,
    grafico_ocupacion_por_isla,
    grafico_boxplot_renta_islas,
    grafico_ranking_renta_islas,
    grafico_fuentes_ingresos_islas,
    grafico_heatmap_ingresos_islas,
    grafico_heatmap_ingresos_tenerife,
    grafico_scatter_renta_ocupacion,
)

def _geom_ok(g, geom_tipo) -> bool:
    return any(isinstance(layer.geom, geom_tipo) for layer in g.layers)

def _data_ok(g) -> bool:
    return g.data is not None and len(g.data) > 0

def _has_all_islands(g) -> bool:
    islas_esperadas = {"Tenerife", "La Palma", "La Gomera", "El Hierro"}
    if "isla" not in g.data.columns:
        return False
    islas_presentes = set(g.data["isla"].unique())
    return islas_esperadas.issubset(islas_presentes)

def _is_mapped(g, aesthetic, column_name) -> bool:
    # Comprobar mapeo global
    if aesthetic in g.mapping and g.mapping[aesthetic] == column_name:
        return True
    # Comprobar mapeo en la primera capa
    if len(g.layers) > 0 and aesthetic in g.layers[0].mapping:
        return g.layers[0].mapping[aesthetic] == column_name
    return False

def _axis_starts_at_zero(g, aesthetic="y") -> bool:
    for scale in g.scales:
        if aesthetic in scale.aesthetics:
            if scale.limits is not None:
                return scale.limits[0] == 0
    return True

# ── actividad ─────────────────────────────────────────────────────────────────

@asset_check(asset=grafico_evolucion_actividad_total)
def check_evolucion_actividad_geom(grafico_evolucion_actividad_total) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_evolucion_actividad_total, geom_line),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_evolucion_actividad_total.layers]}
    )

@asset_check(asset=grafico_evolucion_actividad_total)
def check_evolucion_actividad_data(grafico_evolucion_actividad_total) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_data_ok(grafico_evolucion_actividad_total),
        metadata={"filas": len(grafico_evolucion_actividad_total.data)}
    )

@asset_check(asset=grafico_actividad_por_isla)
def check_actividad_isla_geom(grafico_actividad_por_isla) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_actividad_por_isla, geom_col),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_actividad_por_isla.layers]}
    )

@asset_check(asset=grafico_actividad_por_isla)
def check_actividad_isla_data(grafico_actividad_por_isla) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_data_ok(grafico_actividad_por_isla),
        metadata={"filas": len(grafico_actividad_por_isla.data)}
    )

@asset_check(asset=grafico_actividad_por_isla)
def check_actividad_isla_integridad(grafico_actividad_por_isla) -> AssetCheckResult:
    passed = _has_all_islands(grafico_actividad_por_isla)
    return AssetCheckResult(
        passed=passed,
        metadata={"islas_encontradas": list(grafico_actividad_por_isla.data["isla"].unique())}
    )

@asset_check(asset=grafico_actividad_por_isla)
def check_actividad_isla_eje_y_cero(grafico_actividad_por_isla) -> AssetCheckResult:
    passed = _axis_starts_at_zero(grafico_actividad_por_isla, "y")
    return AssetCheckResult(
        passed=passed,
        metadata={"mensaje": "El eje Y debe empezar en 0 para gráficos de barras para mantener la integridad visual"}
    )
        

# ── ocupacion ─────────────────────────────────────────────────────────────────

@asset_check(asset=grafico_evolucion_ocupacion_total)
def check_evolucion_ocupacion_geom(grafico_evolucion_ocupacion_total) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_evolucion_ocupacion_total, geom_line),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_evolucion_ocupacion_total.layers]}
    )

@asset_check(asset=grafico_evolucion_ocupacion_total)
def check_evolucion_ocupacion_data(grafico_evolucion_ocupacion_total) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_data_ok(grafico_evolucion_ocupacion_total),
        metadata={"filas": len(grafico_evolucion_ocupacion_total.data)}
    )

@asset_check(asset=grafico_evolucion_ocupacion_total)
def check_ocupacion_eje_x_is_ano(grafico_evolucion_ocupacion_total) -> AssetCheckResult:
    passed = _is_mapped(grafico_evolucion_ocupacion_total, "x", "año")
    return AssetCheckResult(
        passed=passed,
        metadata={"columna_mapeada_en_x": str(grafico_evolucion_ocupacion_total.mapping.get("x", "ninguna"))}
    )

@asset_check(asset=grafico_ocupacion_por_isla)
def check_ocupacion_isla_geom(grafico_ocupacion_por_isla) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_ocupacion_por_isla, geom_col),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_ocupacion_por_isla.layers]}
    )

@asset_check(asset=grafico_ocupacion_por_isla)
def check_ocupacion_isla_data(grafico_ocupacion_por_isla) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_data_ok(grafico_ocupacion_por_isla),
        metadata={"filas": len(grafico_ocupacion_por_isla.data)}
    )

@asset_check(asset=grafico_ocupacion_por_isla)
def check_ocupacion_isla_eje_y_cero(grafico_ocupacion_por_isla) -> AssetCheckResult:
    passed = _axis_starts_at_zero(grafico_ocupacion_por_isla, "y")
    return AssetCheckResult(
        passed=passed,
        metadata={"mensaje": "Eje Y verificado en el origen (0)"}
    )


# ── renta ─────────────────────────────────────────────────────────────────────

@asset_check(asset=grafico_boxplot_renta_islas)
def check_boxplot_renta_geom(grafico_boxplot_renta_islas) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_boxplot_renta_islas, geom_boxplot),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_boxplot_renta_islas.layers]}
    )

@asset_check(asset=grafico_boxplot_renta_islas)
def check_boxplot_renta_data(grafico_boxplot_renta_islas) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_data_ok(grafico_boxplot_renta_islas),
        metadata={"filas": len(grafico_boxplot_renta_islas.data)}
    )

@asset_check(asset=grafico_boxplot_renta_islas)
def check_renta_valores_realistas(grafico_boxplot_renta_islas) -> AssetCheckResult:
    valores = grafico_boxplot_renta_islas.data["OBS_VALUE"]
    fuera_de_rango = valores[(valores < 5000) | (valores > 60000)]
    passed = len(fuera_de_rango) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "min_detectado": float(valores.min()),
            "max_detectado": float(valores.max()),
            "puntos_fuera_rango": len(fuera_de_rango)
        }
    )

@asset_check(asset=grafico_ranking_renta_islas)
def check_ranking_renta_geom(grafico_ranking_renta_islas) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_ranking_renta_islas, geom_col),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_ranking_renta_islas.layers]}
    )

@asset_check(asset=grafico_ranking_renta_islas)
def check_ranking_renta_data(grafico_ranking_renta_islas) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_data_ok(grafico_ranking_renta_islas),
        metadata={"filas": len(grafico_ranking_renta_islas.data)}
    )

# ── distribucion ──────────────────────────────────────────────────────────────

@asset_check(asset=grafico_fuentes_ingresos_islas)
def check_fuentes_ingresos_geom(grafico_fuentes_ingresos_islas) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_fuentes_ingresos_islas, geom_col),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_fuentes_ingresos_islas.layers]}
    )

@asset_check(asset=grafico_fuentes_ingresos_islas)
def check_fuentes_ingresos_data(grafico_fuentes_ingresos_islas) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_data_ok(grafico_fuentes_ingresos_islas),
        metadata={"filas": len(grafico_fuentes_ingresos_islas.data)}
    )

@asset_check(asset=grafico_fuentes_ingresos_islas)
def check_fuentes_ingresos_eje_y_cero(grafico_fuentes_ingresos_islas) -> AssetCheckResult:
    passed = _axis_starts_at_zero(grafico_fuentes_ingresos_islas, "y")
    return AssetCheckResult(
        passed=passed,
        metadata={"mensaje": "Garantiza que la comparación de porcentajes sea honesta"}
    )

@asset_check(asset=grafico_heatmap_ingresos_islas)
def check_heatmap_islas_geom(grafico_heatmap_ingresos_islas) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_heatmap_ingresos_islas, geom_tile),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_heatmap_ingresos_islas.layers]}
    )

@asset_check(asset=grafico_heatmap_ingresos_tenerife)
def check_heatmap_tenerife_geom(grafico_heatmap_ingresos_tenerife) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_heatmap_ingresos_tenerife, geom_tile),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_heatmap_ingresos_tenerife.layers]}
    )

@asset_check(asset=grafico_heatmap_ingresos_tenerife)
def check_heatmap_tenerife_municipios(grafico_heatmap_ingresos_tenerife) -> AssetCheckResult:
    n = grafico_heatmap_ingresos_tenerife.data["municipio"].nunique()
    return AssetCheckResult(
        passed=n > 0,
        metadata={"municipios": int(n)}
    )

@asset_check(asset=grafico_heatmap_ingresos_tenerife)
def check_heatmap_tenerife_completitud(grafico_heatmap_ingresos_tenerife) -> AssetCheckResult:
    municipios_count = grafico_heatmap_ingresos_tenerife.data["municipio"].nunique()
    passed = municipios_count == 31
    return AssetCheckResult(
        passed=passed,
        metadata={"municipios_en_grafico": int(municipios_count)}
    )

# ── combinados ────────────────────────────────────────────────────────────────

@asset_check(asset=grafico_scatter_renta_ocupacion)
def check_scatter_geom(grafico_scatter_renta_ocupacion) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_geom_ok(grafico_scatter_renta_ocupacion, geom_point),
        metadata={"geoms": [type(l.geom).__name__ for l in grafico_scatter_renta_ocupacion.layers]}
    )

@asset_check(asset=grafico_scatter_renta_ocupacion)
def check_scatter_data(grafico_scatter_renta_ocupacion) -> AssetCheckResult:
    return AssetCheckResult(
        passed=_data_ok(grafico_scatter_renta_ocupacion),
        metadata={"filas": len(grafico_scatter_renta_ocupacion.data)}
    )

@asset_check(asset=grafico_scatter_renta_ocupacion)
def check_scatter_correlacion_logica(grafico_scatter_renta_ocupacion) -> AssetCheckResult:
    df = grafico_scatter_renta_ocupacion.data
    # Correlación de Pearson entre X e Y
    correlacion = df["OBS_VALUE"].corr(df["pct_cualificado"])
    # Esperamos una correlación positiva (> 0.2)
    passed = correlacion > 0.2
    return AssetCheckResult(
        passed=bool(passed),
        metadata={"coeficiente_correlacion": float(correlacion)}
    )

# ── Exports ───────────────────────────────────────────────────────────────────

graphics_checks = [
    check_evolucion_actividad_geom,
    check_evolucion_actividad_data,
    check_actividad_isla_geom,
    check_actividad_isla_data,
    check_evolucion_ocupacion_geom,
    check_evolucion_ocupacion_data,
    check_ocupacion_isla_geom,
    check_ocupacion_isla_data,
    check_boxplot_renta_geom,
    check_boxplot_renta_data,
    check_ranking_renta_geom,
    check_ranking_renta_data,
    check_fuentes_ingresos_geom,
    check_fuentes_ingresos_data,
    check_heatmap_islas_geom,
    check_heatmap_tenerife_geom,
    check_heatmap_tenerife_municipios,
    check_scatter_geom,
    check_scatter_data,
    check_actividad_isla_integridad,
    check_ocupacion_eje_x_is_ano,
    check_renta_valores_realistas,
    check_scatter_correlacion_logica,
    check_heatmap_tenerife_completitud,
    check_actividad_isla_eje_y_cero,
    check_ocupacion_isla_eje_y_cero,
    check_fuentes_ingresos_eje_y_cero,
]