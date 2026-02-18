import pandas as pd
from plotnine import (
    geom_area, ggplot, aes, geom_col, coord_flip,
    labs, scale_y_continuous, theme_minimal, theme,
    element_text, geom_line, geom_point
)

def crearGraficoDeLineas(df, x_="anio", y_="porcentaje", color_="medida", group__="medida", ruta: str = "default"):
    y_min = df[y_].min()
    y_max = df[y_].max()
    
    grafico_lineas = (
        ggplot(
            df,
            aes(
                x=x_,
                y=y_,
                color=color_,
                group=group__
            )
        )
        + geom_line(size=1.2)
        + geom_point(size=2)
        + scale_y_continuous(
            breaks=range(int(y_min) - 5, int(y_max) + 10, 5),
            limits=[max(0, int(y_min) - 5), int(y_max) + 5]
        )
        + labs(
            title="Evolución de la composición de la renta en Canarias",
            subtitle="Por tipo de ingreso (porcentaje sobre la renta total)",
            x="Año",
            y="Porcentaje (%)",
            color="Tipo de ingreso",
            caption="Fuente: ISTAC"
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 6),
            legend_position="right",
            axis_text_x=element_text(rotation=45),
            panel_grid_major_y=element_text(color="#E0E0E0"),  # Grid más visible
            panel_grid_minor_y=element_text(color="#F5F5F5")   # Grid secundario
        )
    )

    print(grafico_lineas)
    grafico_lineas.save(f"graficos/{ruta}.png", dpi=150)

def crearGraficoDeAreas(df, x_="anio", y_="porcentaje", fill_="medida", ruta: str = "default"):
    grafico_area = (
        ggplot(
            df,
            aes(
                x=x_,
                y=y_,
                fill=fill_
            )
        )
        + geom_area(alpha=0.85, position="stack")
        + labs(
            title="Evolución de la composición de la renta",
            subtitle="Por tipo de ingreso (porcentaje sobre la renta total)",
            x="Año",
            y="Porcentaje (%)",
            fill="Tipo de ingreso",
            caption="Fuente: ISTAC"
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 6),
            axis_text_x=element_text(rotation=45),
            legend_position="right"
        )
    )

    print(grafico_area)
    grafico_area.save(f"graficos/{ruta}.png", dpi=150)

# Cargamos los datos
df = pd.read_csv("data/distribucion-renta-canarias.csv", sep=",", encoding="utf-8")
print("Columnas:", df.columns.tolist())
print(df.head())

# Eliminamos caracteres en blanco
df.columns = df.columns.str.strip()

# Separamos en comunidad autonoma, provincia y municipio
df_municipios = df[df["TERRITORIO_CODE"].str.match(r"^\d{5}$")].copy()
df_provincias = df[df["TERRITORIO_CODE"].isin(["ES701", "ES702"])].copy()
df_canarias = df[df["TERRITORIO_CODE"] == "ES70"].copy()

# Filtrar el año más reciente disponible
anio_max = df["TIME_PERIOD_CODE"].max()
df_filtrado = df[df["TIME_PERIOD_CODE"] == anio_max].copy()

# Eliminar filas sin valor
df_filtrado = df_filtrado.dropna(subset=["OBS_VALUE"])

df_filtrado = df_filtrado.rename(columns={
    "TERRITORIO#es": "municipio",
    "OBS_VALUE": "renta"
})

grafico = (
    ggplot(df_filtrado, aes(x="reorder(municipio, -renta)", y="renta"))
    + geom_col(fill="#2196F3", alpha=0.85)
    + coord_flip()
    + labs(
        title=f"Distribución de Renta en Canarias ({anio_max})",
        x="Municipio",
        y="Renta media (€)"
    )
    + theme_minimal()
    + theme(figure_size=(12, 18))
)

print(grafico)
grafico.save("graficos/grafico_prototipo.png", dpi=150)


# Limpieza
df_canarias = df_canarias.dropna(subset=["OBS_VALUE"])
df_provincias = df_provincias.dropna(subset=["OBS_VALUE"])
df_municipios = df_municipios.dropna(subset=["OBS_VALUE"])

df_canarias["TIME_PERIOD_CODE"] = df_canarias["TIME_PERIOD_CODE"].astype(int)
df_provincias["TIME_PERIOD_CODE"] = df_provincias["TIME_PERIOD_CODE"].astype(int)
df_municipios["TIME_PERIOD_CODE"] = df_municipios["TIME_PERIOD_CODE"].astype(int)

df_canarias = df_canarias.rename(columns={
    "TIME_PERIOD_CODE": "anio",
    "OBS_VALUE": "porcentaje",
    "MEDIDAS#es": "medida"
})

df_provincias = df_provincias.rename(columns={
    "TIME_PERIOD_CODE": "anio",
    "OBS_VALUE": "porcentaje",
    "MEDIDAS#es": "medida"
})

df_municipios = df_municipios.rename(columns={
    "TIME_PERIOD_CODE": "anio",
    "OBS_VALUE": "porcentaje",
    "MEDIDAS#es": "medida"
})

# Comunidad
# Gráfico de líneas
crearGraficoDeLineas(df_canarias, ruta='evolucion_renta_canarias')

# Gráfico de áreas apiladas
crearGraficoDeAreas(df_canarias, ruta='area_apilada_renta_canarias')

# Provincias
# Tenerife
# Gráfico de líneas
crearGraficoDeLineas(df_provincias[df_provincias["TERRITORIO_CODE"].isin(["ES702"])].copy(), ruta='evolucion_renta_tenerife')
# Gráfico de áreas apiladas
crearGraficoDeAreas(df_provincias[df_provincias["TERRITORIO_CODE"].isin(["ES702"])].copy(), ruta='area_apilada_renta_tenerife')

# Gran canaria
# Gráfico de líneas
crearGraficoDeLineas(df_provincias[df_provincias["TERRITORIO_CODE"].isin(["ES701"])].copy(), ruta='evolucion_renta_grancanaria')
# Gráfico de áreas apiladas
crearGraficoDeAreas(df_provincias[df_provincias["TERRITORIO_CODE"].isin(["ES701"])].copy(), ruta='area_apilada_renta_grancanaria')

# Municipios