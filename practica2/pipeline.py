import pandas as pd
from plotnine import (
    geom_area, ggplot, aes, geom_col, coord_flip,
    labs, scale_y_continuous, theme_minimal, theme,
    element_text, geom_line, geom_point, element_line
)
from dagster import asset

def crearGraficoDeLineas(df, ruta: str, titulo: str, subtitulo: str, 
                          x: tuple[str, str], y: tuple[str, str], 
                          color: tuple[str, str], group: str):
    y_min = df[y[0]].min()
    y_max = df[y[0]].max()
    
    grafico_lineas = (
        ggplot(
            df,
            aes(
                x=x[0],
                y=y[0],
                color=color[0],
                group=group
            )
        )
        + geom_line(size=1.2)
        + geom_point(size=2)
        + scale_y_continuous(
            breaks=range(int(y_min) - 5, int(y_max) + 10, 5),
            limits=[max(0, int(y_min) - 5), int(y_max) + 5]
        )
        + labs(
            title=titulo,
            subtitle=subtitulo,
            x=x[1],
            y=y[1],
            color=color[1],
            caption="Fuente: ISTAC"
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 6),
            legend_position="right",
            axis_text_x=element_text(rotation=45),
            panel_grid_major_y=element_line(color="#E0E0E0"),
            panel_grid_minor_y=element_line(color="#F5F5F5")
        )
    )

    grafico_lineas.save(f"graficos/{ruta}.png", dpi=150)


def crearGraficoDeAreas(df, ruta: str, titulo: str, subtitulo: str,
                        x: tuple[str, str], y: tuple[str, str], 
                        fill: tuple[str, str]):
    grafico_area = (
        ggplot(
            df,
            aes(
                x=x[0],
                y=y[0],
                fill=fill[0]
            )
        )
        + geom_area(alpha=0.85, position="stack")
        + labs(
            title=titulo,
            subtitle=subtitulo,
            x=x[1],
            y=y[1],
            fill=fill[1],
            caption="Fuente: ISTAC"
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 6),
            axis_text_x=element_text(rotation=45),
            legend_position="right"
        )
    )

    grafico_area.save(f"graficos/{ruta}.png", dpi=150)

@asset
def renta_canarias_df() -> pd.DataFrame:
    df = pd.read_csv(
        "data/distribucion-renta-canarias.csv",
        sep=",",
        encoding="utf-8"
    )

    df.columns = df.columns.str.strip()

    df = df.dropna(subset=["OBS_VALUE"])
    df["TIME_PERIOD_CODE"] = df["TIME_PERIOD_CODE"].astype(int)

    df = df.rename(columns={
        "TERRITORIO#es": "municipio",
        "TIME_PERIOD_CODE": "anio",
        "OBS_VALUE": "porcentaje",
        "MEDIDAS#es": "medida"
    })

    return df

@asset(deps=[renta_canarias_df])
def renta_canarias(renta_canarias_df: pd.DataFrame) -> pd.DataFrame:
    return renta_canarias_df[
        renta_canarias_df["TERRITORIO_CODE"] == "ES70"
    ].copy()


@asset(deps=[renta_canarias_df])
def renta_provincias(renta_canarias_df: pd.DataFrame) -> pd.DataFrame:
    return renta_canarias_df[
        renta_canarias_df["TERRITORIO_CODE"].isin(["ES701", "ES702"])
    ].copy()


@asset(deps=[renta_canarias_df])
def renta_municipios(renta_canarias_df: pd.DataFrame) -> pd.DataFrame:
    return renta_canarias_df[
        renta_canarias_df["TERRITORIO_CODE"].str.match(r"^\d{5}$")
    ].copy()

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