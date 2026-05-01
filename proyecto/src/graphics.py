from dagster import asset
import pandas as pd
import geopandas as gpd
from common_data import DIR, MUNICIPIO_ISLA
from graphics_templates import (
    bars_graphic, boxplot_graphic, choropleth_map_graphic, 
    heatmap_graphic, lines_graphic, 
    save_graphic, save_grid_graphic, scatter_graphic, stacked_bars_graphic
)

# ── actividad_sc_clean ────────────────────────────────────────────────────────

@asset
def grafico_evolucion_actividad(actividad_sc_clean: pd.DataFrame) -> dict:
    df = actividad_sc_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    # Gráfico total de todas las islas agrupadas
    df_total = df.groupby(["año", "actividad"])["num_casos"].sum().reset_index()
    g_total = lines_graphic(
        df=df_total,
        x="año", y="num_casos", color="actividad",
        titulo="Evolución de la actividad económica — Provincia de Santa Cruz de Tenerife",
        subtitulo="2021-2023 por sector",
        xlabel="Año", ylabel="Número de casos"
    )
    save_graphic(g_total, DIR.GRAPHS / 'actividad_sc', "evolucion_actividad_total")
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
        nombre = isla.lower().replace(" ", "_")
        #save_graphic(g_isla, DIR.GRAPHS / 'actividad_sc', f"evolucion_actividad_{nombre}")
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'actividad_sc',
        name="evolucion_actividad_grid",
        cols=2
    )
    return {
        'islas_total' : g_total,
        'grid' : graficos_islas
    }

@asset
def grafico_actividad_por_municipio(actividad_sc_clean: pd.DataFrame) -> dict:
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
        nombre = isla.lower().replace(" ", "_")
        # Guardar individualmente
        #save_graphic(g_isla, DIR.GRAPHS / 'actividad_sc', f"actividad_municipios_{nombre}")
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'actividad_sc',
        name="actividad_municipios_grid",
        cols=2
    )
    # Gráfico por provincia
    df_islas = df.groupby(["isla", "actividad"])["num_casos"].sum().reset_index()
    g_provincia = stacked_bars_graphic(
        df=df_islas,
        x="isla", y="num_casos", fill="actividad",
        titulo="Actividad económica por isla",
        subtitulo="Provincia de Santa Cruz de Tenerife — Total 2021-2023",
        xlabel="Isla", ylabel="Número de casos",
        position="fill",
    )
    save_graphic(g_provincia, DIR.GRAPHS / 'actividad_sc', "actividad_por_isla")
    return {
        'actividad_por_isla' : g_provincia,
        'grid' : graficos_islas
    }

# ── ocupacion_sc_clean ────────────────────────────────────────────────────────

@asset
def grafico_evolucion_ocupacion(ocupacion_sc_clean: pd.DataFrame) -> dict:
    df = ocupacion_sc_clean.groupby(["año", "ocupacion"])["num_casos"].sum().reset_index()
    g = lines_graphic(
        df=df,
        x="año", y="num_casos", color="ocupacion",
        titulo="Evolución del tipo de ocupación en Tenerife",
        subtitulo="2021-2023",
        xlabel="Año", ylabel="Número de casos"
    )
    save_graphic(g, DIR.GRAPHS / 'ocupacion_sc', "evolucion_ocupacion")
    return {
        'evolucion_ocupacion' : g
    }

@asset
def grafico_ocupacion_por_municipio(ocupacion_sc_clean: pd.DataFrame) -> dict:
    df = ocupacion_sc_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    graficos_islas = []
    # Gráfico por isla de municipios de cada isla
    for isla in sorted(df["isla"].unique()):
        df_isla = (
            df[df["isla"] == isla]
            .groupby(["municipio", "ocupacion"])["num_casos"]
            .sum()
            .reset_index()
        )
        n_municipios = df_isla["municipio"].nunique()
        x_text_size = 7 if n_municipios > 15 else 9
        legend_position = "bottom"
        g_isla = stacked_bars_graphic(
            df=df_isla,
            x="municipio", y="num_casos", fill="ocupacion",
            titulo=f"Tipo de ocupación por municipio — {isla}",
            subtitulo="Total 2021-2023",
            xlabel="Municipio", ylabel="Número de casos",
            position="fill",
            x_text_size=x_text_size,
            x_text_rotation=45,
            legend_position=legend_position,
        )
        nombre = isla.lower().replace(" ", "_")
        # save_graphic(g_isla, DIR.GRAPHS / 'ocupacion_sc', f"ocupacion_municipios_{nombre}")
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'ocupacion_sc',
        name="ocupacion_municipios_grid",
        cols=2
    )
    # Gráfico por isla
    df_islas = (
        df.groupby(["isla", "ocupacion"])["num_casos"]
        .sum()
        .reset_index()
    )
    g_provincia = stacked_bars_graphic(
        df=df_islas,
        x="isla", y="num_casos", fill="ocupacion",
        titulo="Tipo de ocupación por isla — Provincia de Santa Cruz de Tenerife",
        subtitulo="Total 2021-2023",
        xlabel="Isla", ylabel="Número de casos",
        position="fill",
    )
    save_graphic(
        g_provincia,
        DIR.GRAPHS / 'ocupacion_sc',
        "ocupacion_por_isla"
    )
    return {
        'ocupacion_por_isla' : g_provincia,
        'grid' : graficos_islas
    }

# ── renta_media_clean ─────────────────────────────────────────────────────────

@asset
def grafico_distribucion_renta_municipios(renta_media_clean: pd.DataFrame) -> dict:
    df = renta_media_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df_filtrado = df[df["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA"]
    graficos_islas = []
    # Boxplot por isla de municipios de cada isla
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
        nombre = isla.lower().replace(" ", "_")
        # save_graphic(g_isla, DIR.GRAPHS / 'renta_media', f"boxplot_renta_{nombre}")
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'renta_media',
        name="boxplot_renta_municipios_grid",
        cols=2
    )
    # Boxplot entre islas
    g_islas = boxplot_graphic(
        df=df_filtrado,
        x="isla", y="OBS_VALUE",
        titulo="Distribución de renta neta media por persona por isla",
        subtitulo="Provincia de Santa Cruz de Tenerife 2021-2023",
        xlabel="Isla", ylabel="Renta (€)",
        x_text_rotation=0,
    )
    save_graphic(
        g_islas,
        DIR.GRAPHS / 'renta_media',
        "boxplot_renta_islas"
    )
    return {
        'boxplot_renta_islas' : g_islas,
        'grid' : graficos_islas
    }

@asset
def grafico_ranking_renta_municipios(renta_media_clean: pd.DataFrame) -> dict:
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
            .groupby("municipio")["OBS_VALUE"]
            .mean()
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
        nombre = isla.lower().replace(" ", "_")
        # save_graphic(g_isla, DIR.GRAPHS / 'renta_media', f"ranking_renta_{nombre}")
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / 'renta_media',
        name="ranking_renta_municipios_grid",
        cols=2
    )
    # Ranking por islas
    df_islas = (
        df[
            (df["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA") &
            (df["año"] == 2023)
        ]
        .groupby("isla")["OBS_VALUE"]
        .mean()
        .reset_index()
        .sort_values("OBS_VALUE")
    )
    g_islas = bars_graphic(
        df=df_islas,
        x="isla", y="OBS_VALUE",
        titulo="Renta neta media por persona por isla",
        subtitulo="Provincia de Santa Cruz de Tenerife — Año 2023",
        xlabel="Isla", ylabel="Renta (€)",
        horizontal=False,
        x_text_rotation=0,
    )
    save_graphic(
        g_islas,
        DIR.GRAPHS / 'renta_media',
        "ranking_renta_islas"
    )
    return {
        'ranking_renta_islas' : g_islas,
        'grid' : graficos_islas
    }

# ── distribucion_renta_clean ──────────────────────────────────────────────────

@asset
def grafico_fuentes_ingresos_municipio(
    distribucion_renta_clean: pd.DataFrame
) -> dict:
    df = distribucion_renta_clean.copy()
    df = df[df["año"] == 2023]
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df = df[df["isla"].isin(["Tenerife", "La Palma", "La Gomera", "El Hierro"])]
    graficos_islas = []
    # Gráfico por isla
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
            x="municipio",
            y="OBS_VALUE",
            fill="MEDIDAS#es",
            titulo=f"Fuentes de ingresos por municipio — {isla}",
            subtitulo="Porcentaje sobre el total — 2023",
            xlabel="Municipio",
            ylabel="%",
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
    # Gráfico agregado por isla
    df_islas = (
        df.groupby(["isla", "MEDIDAS#es"])["OBS_VALUE"]
        .mean()
        .reset_index()
    )
    g_islas = stacked_bars_graphic(
        df=df_islas,
        x="isla",
        y="OBS_VALUE",
        fill="MEDIDAS#es",
        titulo="Composición de fuentes de ingresos por isla",
        subtitulo="Porcentaje sobre el total — 2023",
        xlabel="Isla",
        ylabel="%",
        position="fill",
        legend_position="right"
    )
    save_graphic(
        g_islas,
        DIR.GRAPHS / "distribucion_renta",
        "fuentes_ingresos_islas"
    )
    return {
        'fuentes_ingresos_islas' : g_islas,
        'grid' : graficos_islas
    }

@asset
def grafico_heatmap_ingresos(distribucion_renta_clean: pd.DataFrame) -> dict:
    df = distribucion_renta_clean.copy()
    df["isla"] = df["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
    df = df[df["isla"].isin(["Tenerife", "La Palma", "La Gomera", "El Hierro"])]
    df = df[df["año"] == 2023]
    # TENERIFE FUERA DEL GRID (por densidad)
    df_tenerife = df[df["isla"] == "Tenerife"]
    df_tenerife = (
        df_tenerife
        .groupby(["municipio", "MEDIDAS#es"])["OBS_VALUE"]
        .mean()
        .reset_index()
    )
    g_tenerife = heatmap_graphic(
        df=df_tenerife,
        x="MEDIDAS#es",
        y="municipio",
        fill="OBS_VALUE",
        titulo="Distribución de fuentes de ingresos — Tenerife",
        subtitulo="Porcentaje (%) — 2023",
        xlabel="Fuente de ingresos",
        ylabel="Municipio",
        midpoint=25.0
    )
    save_graphic(
        g_tenerife,
        DIR.GRAPHS / "distribucion_renta",
        "heatmap_ingresos_tenerife"
    )
    # RESTO DE ISLAS EN GRID
    df_otros = df[df["isla"] != "Tenerife"]
    graficos_islas = []
    for isla in sorted(df_otros["isla"].unique()):
        df_isla = (
            df_otros[df_otros["isla"] == isla]
            .groupby(["municipio", "MEDIDAS#es"])["OBS_VALUE"]
            .mean()
            .reset_index()
        )
        g_isla = heatmap_graphic(
            df=df_isla,
            x="MEDIDAS#es",
            y="municipio",
            fill="OBS_VALUE",
            titulo=f"Distribución de fuentes de ingresos — {isla}",
            subtitulo="Porcentaje (%) — 2023",
            xlabel="Fuente de ingresos",
            ylabel="Municipio",
            midpoint=25.0
        )
        graficos_islas.append((isla, g_isla))
    save_grid_graphic(
        graficos=graficos_islas,
        path=DIR.GRAPHS / "distribucion_renta",
        name="heatmap_ingresos_otras_islas_grid",
        cols=2
    )
    # HEATMAP GLOBAL POR ISLA
    df_islas = (
        df.groupby(["isla", "MEDIDAS#es"])["OBS_VALUE"]
        .mean()
        .reset_index()
    )
    g_islas = heatmap_graphic(
        df=df_islas,
        x="MEDIDAS#es",
        y="isla",
        fill="OBS_VALUE",
        titulo="Distribución de fuentes de ingresos por isla",
        subtitulo="Porcentaje (%) — 2023",
        xlabel="Fuente de ingresos",
        ylabel="Isla",
        midpoint=25.0
    )
    save_graphic(
        g_islas,
        DIR.GRAPHS / "distribucion_renta",
        "heatmap_ingresos_islas"
    )
    return {
        'tenerife' : g_tenerife,
        'heatmap_ingresos_islas' : g_islas,
        'grid' : graficos_islas
    }

# ── ocupacion_sc_clean + renta_media_clean ────────────────────────────────────

@asset
def grafico_scatter_renta_ocupacion_grid(
    ocupacion_sc_clean: pd.DataFrame,
    renta_media_clean: pd.DataFrame
) -> dict:
    # Identificar años comunes y columna de valor
    anos_comunes = sorted(list(set(ocupacion_sc_clean["año"]).intersection(set(renta_media_clean["año"]))))
    col_val = "num_casos" if "num_casos" in ocupacion_sc_clean.columns else "num_cases"
    filtro_ocupacion = "Directores/gerentes y profesionales/técnicos de nivel medio o alto"
    graficos_anuales = []
    for ano in anos_comunes:
        df_ocu_ano = ocupacion_sc_clean[ocupacion_sc_clean["año"] == ano].copy()
        df_renta_ano = renta_media_clean[
            (renta_media_clean["MEDIDAS_CODE"] == "RENTA_NETA_MEDIA_PERSONA") & 
            (renta_media_clean["año"] == ano)
        ].copy()
        if df_ocu_ano.empty or df_renta_ano.empty:
            continue
        # Total por municipio
        totales = df_ocu_ano.groupby("municipio")[col_val].sum().reset_index()
        totales.columns = ["municipio", "total_municipio"]
        # Cualificados por municipio (con precaución si el filtro es vacío)
        df_filtro = df_ocu_ano[df_ocu_ano["ocupacion"] == filtro_ocupacion]
        if not df_filtro.empty:
            cualificados = df_filtro.groupby("municipio")[col_val].sum().reset_index()
            cualificados.columns = ["municipio", "num_cualificados"]
        else:
            # Si no hay nadie cualificado ese año, creamos df vacío con las columnas correctas
            cualificados = pd.DataFrame(columns=["municipio", "num_cualificados"])
        # Unir totales con cualificados
        stats_ocupacion = totales.merge(cualificados, on="municipio", how="left")
        # Asegurar que la columna existe 
        if "num_cualificados" not in stats_ocupacion.columns:
            stats_ocupacion["num_cualificados"] = 0.0
        stats_ocupacion["num_cualificados"] = stats_ocupacion["num_cualificados"].fillna(0)
        stats_ocupacion["pct_cualificado"] = (stats_ocupacion["num_cualificados"] / stats_ocupacion["total_municipio"]) * 100
        renta_mun = df_renta_ano.groupby("municipio")["OBS_VALUE"].mean().reset_index()
        df_plot = stats_ocupacion.merge(renta_mun, on="municipio", how="inner")
        if df_plot.empty:
            continue
        df_plot["isla"] = df_plot["municipio"].map(MUNICIPIO_ISLA).fillna("Tenerife")
        df_plot = df_plot.dropna(subset=["OBS_VALUE", "pct_cualificado", "isla"])
        df_plot["isla"] = df_plot["isla"].astype(str)
        g = scatter_graphic(
            df=df_plot,
            x="OBS_VALUE", 
            y="pct_cualificado",
            color="isla",
            titulo=f"Año {ano}",
            subtitulo="Renta media vs. % Trabajo cualificado",
            xlabel="Renta (€)",
            ylabel="% Cualificado"
        )
        graficos_anuales.append((str(ano), g))
    if graficos_anuales:
        save_grid_graphic(
            graficos=graficos_anuales,
            path=DIR.GRAPHS / "combinados",
            name="scatter_renta_ocupacion_grid",
            cols=2,
            width=18,
            height=12
        )
        # Guardar el último año disponible por separado
        #ultimo_ano, ultimo_g = graficos_anuales[-1]
        #save_graphic(ultimo_g, DIR.GRAPHS / "combinados", f"scatter_renta_ocupacion_{ultimo_ano}")
    return {
        #f"scatter_renta_ocupacion_{ultimo_ano}" : ultimo_g,
        'grid' : graficos_anuales
    }

# ── GeoJSON + renta_media_clean ───────────────────────────────────────────────

@asset
def grafico_mapa_renta_evolucion_grid(
    secciones_2021: gpd.GeoDataFrame,
    secciones_2022: gpd.GeoDataFrame,
    secciones_2023: gpd.GeoDataFrame,
    renta_media_clean: pd.DataFrame
) -> dict:
    periodos = [
        (2021, secciones_2021),
        (2022, secciones_2022),
        (2023, secciones_2023)
    ]
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
            titulo=f"Año {ano}",
            subtitulo="Renta neta media por persona",
            low="#013468", # Azul oscuro
            mid="#f7f7f7", # Gris claro
            high="#b2182b"  # Rojo
        )
        graficos_mapas.append((str(ano), g))
    save_grid_graphic(
        graficos=graficos_mapas,
        path=DIR.GRAPHS,
        name="mapa_renta_evolucion_grid",
        cols=2,
        width=25
    )
    return {
        'grid' : graficos_mapas
    }

# ── Exports ───────────────────────────────────────────────────────────────────

graphics_assets = [
    grafico_evolucion_actividad,
    grafico_actividad_por_municipio,
    grafico_evolucion_ocupacion,
    grafico_ocupacion_por_municipio,
    grafico_distribucion_renta_municipios,
    grafico_ranking_renta_municipios,
    grafico_fuentes_ingresos_municipio,
    grafico_heatmap_ingresos,
    grafico_scatter_renta_ocupacion_grid,
    grafico_mapa_renta_evolucion_grid,
]