from dagster import asset
import pandas as pd
import geopandas as gpd
from plotnine import ggplot
from common_data import DIR, MUNICIPIO_ISLA
from graphics_templates import (
    bars_graphic, boxplot_graphic, choropleth_map_graphic,
    heatmap_graphic, lines_graphic,
    save_graphic, save_grid_graphic, scatter_graphic, stacked_bars_graphic
)

# ── actividad_sc_clean ────────────────────────────────────────────────────────

@asset
def grafico_evolucion_actividad_total(actividad_sc_clean: pd.DataFrame) -> ggplot:
    df = actividad_sc_clean.groupby(["año", "actividad"])["num_casos"].sum().reset_index()
    g = lines_graphic(
        df=df,
        x="año", y="num_casos", color="actividad",
        titulo="Evolución de la actividad económica — Provincia de Santa Cruz de Tenerife",
        subtitulo="2021-2023 por sector",
        xlabel="Año", ylabel="Número de casos"
    )
    save_graphic(g, DIR.GRAPHS / 'actividad_sc', "evolucion_actividad_total")
    return g

@asset
def grafico_evolucion_actividad_grid(actividad_sc_clean: pd.DataFrame) -> None:
    df = actividad_sc_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    graficos_islas = []
    for isla in sorted(df["isla"].unique()):
        df_isla = (
            df[df["isla"] == isla]
            .groupby(["año", "actividad"])["num_casos"]
            .sum()
            .reset_index()
        )
        g_isla = lines_graphic(
            df=df_isla,
            x="año", y="num_casos", color="actividad",
            titulo=f"Evolución de la actividad económica — {isla}",
            subtitulo="2021-2023 por sector",
            xlabel="Año", ylabel="Número de casos"
        )
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'actividad_sc',
        name="evolucion_actividad_grid",
        cols=2
    )

@asset
def grafico_actividad_por_isla(actividad_sc_clean: pd.DataFrame) -> ggplot:
    df = actividad_sc_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df_islas = df.groupby(["isla", "actividad"])["num_casos"].sum().reset_index()
    g = stacked_bars_graphic(
        df=df_islas,
        x="isla", y="num_casos", fill="actividad",
        titulo="Actividad económica por isla",
        subtitulo="Provincia de Santa Cruz de Tenerife — Total 2021-2023",
        xlabel="Isla", ylabel="Número de casos",
        position="fill",
    )
    save_graphic(g, DIR.GRAPHS / 'actividad_sc', "actividad_por_isla")
    return g

@asset
def grafico_actividad_municipios_grid(actividad_sc_clean: pd.DataFrame) -> None:
    df = actividad_sc_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    graficos_islas = []
    for isla in sorted(df["isla"].unique()):
        df_isla = (
            df[df["isla"] == isla]
            .groupby(["municipio", "actividad"])["num_casos"]
            .sum()
            .reset_index()
        )
        n_municipios = df_isla["municipio"].nunique()
        x_text_size = 7 if n_municipios > 15 else 9
        legend_position = "bottom" if isla == "Tenerife" else "right"
        g_isla = stacked_bars_graphic(
            df=df_isla,
            x="municipio", y="num_casos", fill="actividad",
            titulo=f"Actividad económica — {isla}",
            subtitulo="Total 2021-2023",
            xlabel="Municipio", ylabel="Número de casos",
            position="fill",
            x_text_size=x_text_size,
            x_text_rotation=45,
            legend_position=legend_position,
        )
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'actividad_sc',
        name="actividad_municipios_grid",
        cols=2
    )

# ── ocupacion_sc_clean ────────────────────────────────────────────────────────

@asset
def grafico_evolucion_ocupacion_total(ocupacion_sc_clean: pd.DataFrame) -> ggplot:
    df = ocupacion_sc_clean.groupby(["año", "ocupacion"])["num_casos"].sum().reset_index()
    g = lines_graphic(
        df=df,
        x="año", y="num_casos", color="ocupacion",
        titulo="Evolución del tipo de ocupación — Provincia de Santa Cruz de Tenerife",
        subtitulo="2021-2023",
        xlabel="Año", ylabel="Número de casos"
    )
    save_graphic(g, DIR.GRAPHS / 'ocupacion_sc', "evolucion_ocupacion_total")
    return g

@asset
def grafico_ocupacion_por_isla(ocupacion_sc_clean: pd.DataFrame) -> ggplot:
    df = ocupacion_sc_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df_islas = df.groupby(["isla", "ocupacion"])["num_casos"].sum().reset_index()
    g = stacked_bars_graphic(
        df=df_islas,
        x="isla", y="num_casos", fill="ocupacion",
        titulo="Tipo de ocupación por isla — Provincia de Santa Cruz de Tenerife",
        subtitulo="Total 2021-2023",
        xlabel="Isla", ylabel="Número de casos",
        position="fill",
    )
    save_graphic(g, DIR.GRAPHS / 'ocupacion_sc', "ocupacion_por_isla")
    return g

@asset
def grafico_ocupacion_municipios_grid(ocupacion_sc_clean: pd.DataFrame) -> None:
    df = ocupacion_sc_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    print('===========================================')
    municipios_ocupacion = set(df["municipio"].unique())
    municipios_mapeados_palma = {m for m, isla in MUNICIPIO_ISLA.items() if isla == "La Palma"}
    print("Faltan en MUNICIPIO_ISLA:", municipios_ocupacion - set(MUNICIPIO_ISLA.keys()))
    print("Mapeados como La Palma:", municipios_mapeados_palma)
    print('===========================================')

    graficos_islas = []
    for isla in sorted(df["isla"].unique()):
        df_isla = (
            df[df["isla"] == isla]
            .groupby(["municipio", "ocupacion"])["num_casos"]
            .sum()
            .reset_index()
        )
        n_municipios = df_isla["municipio"].nunique()
        x_text_size = 7 if n_municipios > 15 else 9
        g_isla = stacked_bars_graphic(
            df=df_isla,
            x="municipio", y="num_casos", fill="ocupacion",
            titulo=f"Tipo de ocupación por municipio — {isla}",
            subtitulo="Total 2021-2023",
            xlabel="Municipio", ylabel="Número de casos",
            position="fill",
            x_text_size=x_text_size,
            x_text_rotation=45,
            legend_position="bottom",
        )
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'ocupacion_sc',
        name="ocupacion_municipios_grid",
        cols=2
    )

# ── renta_media_clean ─────────────────────────────────────────────────────────

@asset
def grafico_boxplot_renta_islas(renta_media_clean: pd.DataFrame) -> ggplot:
    df = renta_media_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df_filtrado = df[df["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA"]
    g = boxplot_graphic(
        df=df_filtrado,
        x="isla", y="OBS_VALUE",
        titulo="Distribución de renta neta media por persona por isla",
        subtitulo="Provincia de Santa Cruz de Tenerife 2021-2023",
        xlabel="Isla", ylabel="Renta (€)",
        x_text_rotation=0,
    )
    save_graphic(g, DIR.GRAPHS / 'renta_media', "boxplot_renta_islas")
    return g

@asset
def grafico_boxplot_renta_municipios_grid(renta_media_clean: pd.DataFrame) -> None:
    df = renta_media_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df_filtrado = df[df["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA"]
    graficos_islas = []
    for isla in sorted(df_filtrado["isla"].unique()):
        df_isla = df_filtrado[df_filtrado["isla"] == isla]
        n_municipios = df_isla["municipio"].nunique()
        x_text_size = 7 if n_municipios > 15 else 9
        g_isla = boxplot_graphic(
            df=df_isla,
            x="municipio", y="OBS_VALUE",
            titulo=f"Distribución de renta neta media por persona — {isla}",
            subtitulo="Por municipio 2021-2023",
            xlabel="Municipio", ylabel="Renta (€)",
            x_text_size=x_text_size,
            title_size=10
        )
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'renta_media',
        name="boxplot_renta_municipios_grid",
        cols=2
    )

@asset
def grafico_ranking_renta_islas(renta_media_clean: pd.DataFrame) -> ggplot:
    df = renta_media_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df_islas = (
        df[
            (df["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA") &
            (df["año"] == 2023)
        ]
        .groupby("isla")["OBS_VALUE"].mean()
        .reset_index()
        .sort_values("OBS_VALUE")
    )
    g = bars_graphic(
        df=df_islas,
        x="isla", y="OBS_VALUE",
        titulo="Renta neta media por persona por isla",
        subtitulo="Provincia de Santa Cruz de Tenerife — Año 2023",
        xlabel="Isla", ylabel="Renta (€)",
        horizontal=False,
        x_text_rotation=0,
    )
    save_graphic(g, DIR.GRAPHS / 'renta_media', "ranking_renta_islas")
    return g

@asset
def grafico_ranking_renta_municipios_grid(renta_media_clean: pd.DataFrame) -> None:
    df = renta_media_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    graficos_islas = []
    for isla in sorted(df["isla"].unique()):
        df_isla = (
            df[
                (df["isla"] == isla) &
                (df["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA") &
                (df["año"] == 2023)
            ]
            .groupby("municipio")["OBS_VALUE"].mean()
            .reset_index()
            .sort_values("OBS_VALUE")
        )
        n_municipios = df_isla["municipio"].nunique()
        x_text_size = 7 if n_municipios > 15 else 9
        g_isla = bars_graphic(
            df=df_isla,
            x="municipio", y="OBS_VALUE",
            titulo=f"Renta neta media por persona — {isla}",
            subtitulo="Año 2023, ordenado de menor a mayor",
            xlabel="Municipio", ylabel="Renta (€)",
            horizontal=False,
            x_text_size=x_text_size,
            x_text_rotation=45,
        )
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'renta_media',
        name="ranking_renta_municipios_grid",
        cols=2
    )

# ── distribucion_renta_clean ──────────────────────────────────────────────────

@asset
def grafico_fuentes_ingresos_islas(distribucion_renta_clean: pd.DataFrame) -> ggplot:
    df = distribucion_renta_clean.copy()
    df = df[df["año"] == 2023]
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df = df[df["isla"].isin(["Tenerife", "La Palma", "La Gomera", "El Hierro"])]
    df_islas = df.groupby(["isla", "MEDIDAS#es"])["OBS_VALUE"].mean().reset_index()
    g = stacked_bars_graphic(
        df=df_islas,
        x="isla", y="OBS_VALUE", fill="MEDIDAS#es",
        fill_label='Fuente de ingresos',
        titulo="Composición de fuentes de ingresos por isla",
        subtitulo="Porcentaje sobre el total — 2023",
        xlabel="Isla", ylabel="%",
        position="fill",
        legend_position="right"
    )
    save_graphic(g, DIR.GRAPHS / "distribucion_renta", "fuentes_ingresos_islas")
    return g

@asset
def grafico_fuentes_ingresos_municipios_grid(distribucion_renta_clean: pd.DataFrame) -> None:
    df = distribucion_renta_clean.copy()
    df = df[df["año"] == 2023]
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df = df[df["isla"].isin(["Tenerife", "La Palma", "La Gomera", "El Hierro"])]
    graficos_islas = []
    for isla in sorted(df["isla"].unique()):
        df_isla = (
            df[df["isla"] == isla]
            .groupby(["municipio", "MEDIDAS#es"])["OBS_VALUE"]
            .mean()
            .reset_index()
        )
        n_municipios = df_isla["municipio"].nunique()
        x_text_size = 7 if n_municipios > 15 else 9
        legend_position = "bottom" if isla == "Tenerife" else "right"
        g_isla = stacked_bars_graphic(
            df=df_isla,
            x="municipio", y="OBS_VALUE", fill="MEDIDAS#es",
            fill_label='Fuente de ingresos',
            titulo=f"Fuentes de ingresos por municipio — {isla}",
            subtitulo="Porcentaje sobre el total — 2023",
            xlabel="Municipio", ylabel="%",
            position="fill",
            x_text_size=x_text_size,
            x_text_rotation=45,
            legend_position=legend_position,
        )
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / "distribucion_renta",
        name="fuentes_ingresos_municipios_grid",
        cols=2
    )

@asset
def grafico_heatmap_ingresos_islas(distribucion_renta_clean: pd.DataFrame) -> ggplot:
    df = distribucion_renta_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df = df[df["año"] == 2023]
    df = df[df["isla"].isin(["Tenerife", "La Palma", "La Gomera", "El Hierro"])]
    df_islas = df.groupby(["isla", "MEDIDAS#es"])["OBS_VALUE"].mean().reset_index()
    g = heatmap_graphic(
        df=df_islas,
        x="MEDIDAS#es", y="isla", fill="OBS_VALUE",
        fill_label='% Ingresos',
        titulo="Distribución de fuentes de ingresos por isla",
        subtitulo="Porcentaje (%) — 2023",
        xlabel="Fuente de ingresos", ylabel="Isla",
        midpoint=25.0
    )
    save_graphic(g, DIR.GRAPHS / "distribucion_renta", "heatmap_ingresos_islas")
    return g

@asset
def grafico_heatmap_ingresos_tenerife(distribucion_renta_clean: pd.DataFrame) -> ggplot:
    df = distribucion_renta_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df = df[(df["isla"] == "Tenerife") & (df["año"] == 2023)]
    df_tenerife = df.groupby(["municipio", "MEDIDAS#es"])["OBS_VALUE"].mean().reset_index()
    g = heatmap_graphic(
        df=df_tenerife,
        x="MEDIDAS#es", y="municipio", fill="OBS_VALUE",
        fill_label='% Ingresos',
        titulo="Distribución de fuentes de ingresos — Tenerife",
        subtitulo="Porcentaje (%) — 2023",
        xlabel="Fuente de ingresos", ylabel="Municipio",
        midpoint=25.0
    )
    save_graphic(g, DIR.GRAPHS / "distribucion_renta", "heatmap_ingresos_tenerife")
    return g

@asset
def grafico_heatmap_ingresos_otras_islas_grid(distribucion_renta_clean: pd.DataFrame) -> None:
    df = distribucion_renta_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df = df[(df["isla"] != "Tenerife") & (df["año"] == 2023)]
    df = df[df["isla"].isin(["La Palma", "La Gomera", "El Hierro"])]
    graficos_islas = []
    for isla in sorted(df["isla"].unique()):
        df_isla = (
            df[df["isla"] == isla]
            .groupby(["municipio", "MEDIDAS#es"])["OBS_VALUE"]
            .mean()
            .reset_index()
        )
        g_isla = heatmap_graphic(
            df=df_isla,
            x="MEDIDAS#es", y="municipio", fill="OBS_VALUE",
            fill_label='% Ingresos',
            titulo=f"Distribución de fuentes de ingresos — {isla}",
            subtitulo="Porcentaje (%) — 2023",
            xlabel="Fuente de ingresos", ylabel="Municipio",
            midpoint=25.0
        )
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / "distribucion_renta",
        name="heatmap_ingresos_otras_islas_grid",
        cols=2
    )

# ── combinados ────────────────────────────────────────────────────────────────

@asset
def grafico_scatter_renta_ocupacion(
    ocupacion_sc_clean: pd.DataFrame,
    renta_media_clean: pd.DataFrame
) -> ggplot:
    AÑO = 2023
    col_val = "num_casos" if "num_casos" in ocupacion_sc_clean.columns else "num_cases"
    filtro_ocupacion = "Directores/gerentes y profesionales/técnicos de nivel medio o alto"

    df_ocu = ocupacion_sc_clean[ocupacion_sc_clean["año"] == AÑO].copy()
    totales = df_ocu.groupby("municipio")[col_val].sum().reset_index()
    totales.columns = ["municipio", "total"]
    cualificados = (
        df_ocu[df_ocu["ocupacion"] == filtro_ocupacion]
        .groupby("municipio")[col_val].sum()
        .reset_index()
    )
    cualificados.columns = ["municipio", "num_cualificados"]
    stats = totales.merge(cualificados, on="municipio", how="left").fillna(0)
    stats["pct_cualificado"] = stats["num_cualificados"] / stats["total"] * 100

    renta = (
        renta_media_clean[
            (renta_media_clean["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA") &
            (renta_media_clean["año"] == AÑO)
        ]
        .groupby("municipio")["OBS_VALUE"].mean()
        .reset_index()
    )

    df = stats.merge(renta, on="municipio")
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")

    g = scatter_graphic(
        df=df,
        x="OBS_VALUE", y="pct_cualificado",
        color="isla",
        titulo="Renta media vs trabajo cualificado por municipio",
        subtitulo=f"Cada punto es un municipio — {AÑO}",
        xlabel="Renta neta media por persona (€)",
        ylabel="% trabajo cualificado"
    )
    # save_graphic(g, DIR.GRAPHS / "combinados", f"scatter_renta_ocupacion_{AÑO}")
    return g

@asset
def grafico_scatter_renta_ocupacion_grid(
    ocupacion_sc_clean: pd.DataFrame,
    renta_media_clean: pd.DataFrame
) -> None:
    anos_comunes = sorted(set(ocupacion_sc_clean["año"]) & set(renta_media_clean["año"]))
    col_val = "num_casos" if "num_casos" in ocupacion_sc_clean.columns else "num_cases"
    filtro_ocupacion = "Directores/gerentes y profesionales/técnicos de nivel medio o alto"
    graficos_anuales = []
    for ano in anos_comunes:
        df_ocu = ocupacion_sc_clean[ocupacion_sc_clean["año"] == ano].copy()
        renta = (
            renta_media_clean[
                (renta_media_clean["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA") &
                (renta_media_clean["año"] == ano)
            ]
            .groupby("municipio")["OBS_VALUE"].mean()
            .reset_index()
        )
        totales = df_ocu.groupby("municipio")[col_val].sum().reset_index()
        totales.columns = ["municipio", "total"]
        cualificados = (
            df_ocu[df_ocu["ocupacion"] == filtro_ocupacion]
            .groupby("municipio")[col_val].sum()
            .reset_index()
        )
        cualificados.columns = ["municipio", "num_cualificados"]
        stats = totales.merge(cualificados, on="municipio", how="left").fillna(0)
        stats["pct_cualificado"] = stats["num_cualificados"] / stats["total"] * 100
        df = stats.merge(renta, on="municipio")
        df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
        g = scatter_graphic(
            df=df,
            x="OBS_VALUE", y="pct_cualificado",
            color="isla",
            titulo=f"Año {ano}",
            subtitulo="Renta media vs. % Trabajo cualificado",
            xlabel="Renta (€)", ylabel="% Cualificado"
        )
        graficos_anuales.append((str(ano), g))
    save_grid_graphic(
        graficos=graficos_anuales,
        path=DIR.GRAPHS / "combinados",
        name="scatter_renta_ocupacion_grid",
        cols=2, width=18, height=12
    )

# ── GeoJSON + renta_media_clean ───────────────────────────────────────────────

@asset
def grafico_mapa_renta_evolucion_grid(
    secciones_2021: gpd.GeoDataFrame,
    secciones_2022: gpd.GeoDataFrame,
    secciones_2023: gpd.GeoDataFrame,
    renta_media_clean: pd.DataFrame
) -> None:
    periodos = [(2021, secciones_2021), (2022, secciones_2022), (2023, secciones_2023)]
    graficos_mapas = []
    for ano, gdf_base in periodos:
        renta_ano = (
            renta_media_clean[
                (renta_media_clean["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA") &
                (renta_media_clean["año"] == ano)
            ]
            .groupby("seccion_key")["OBS_VALUE"].mean()
            .reset_index()
        )
        gdf = gdf_base.copy()
        gdf["seccion_key"] = gdf["geocode"].str[9:]
        gdf = gdf.merge(renta_ano, on="seccion_key", how="left")
        g = choropleth_map_graphic(
            gdf=gdf,
            fill="OBS_VALUE",
            fill_label='Renta (€)',
            titulo=f"Año {ano}",
            subtitulo="Renta neta media por persona",
            low="#013468", mid="#f7f7f7", high="#b2182b"
        )
        graficos_mapas.append((str(ano), g))
    save_grid_graphic(
        graficos=graficos_mapas,
        path=DIR.GRAPHS,
        name="mapa_renta_evolucion_grid",
        cols=2, width=25
    )

# ── Exports ───────────────────────────────────────────────────────────────────

graphics_assets = [
    grafico_evolucion_actividad_total,
    grafico_evolucion_actividad_grid,
    grafico_actividad_por_isla,
    grafico_actividad_municipios_grid,
    grafico_evolucion_ocupacion_total,
    grafico_ocupacion_por_isla,
    grafico_ocupacion_municipios_grid,
    grafico_boxplot_renta_islas,
    grafico_boxplot_renta_municipios_grid,
    grafico_ranking_renta_islas,
    grafico_ranking_renta_municipios_grid,
    grafico_fuentes_ingresos_islas,
    grafico_fuentes_ingresos_municipios_grid,
    grafico_heatmap_ingresos_islas,
    grafico_heatmap_ingresos_tenerife,
    grafico_heatmap_ingresos_otras_islas_grid,
    grafico_scatter_renta_ocupacion,
    grafico_scatter_renta_ocupacion_grid,
    grafico_mapa_renta_evolucion_grid,
]