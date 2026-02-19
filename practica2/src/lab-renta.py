import pandas as pd
from plotnine import (
    geom_area, ggplot, aes,
    labs, scale_y_continuous, theme_minimal, theme,
    element_text, geom_line, geom_point
)

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
            panel_grid_major_y=element_text(color="#E0E0E0"),
            panel_grid_minor_y=element_text(color="#F5F5F5")
        )
    )

    print(grafico_lineas)
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

    print(grafico_area)
    grafico_area.save(f"graficos/{ruta}.png", dpi=150)

# Cargamos los datos
df = pd.read_csv("data/distribucion-renta-canarias.csv", sep=",", encoding="utf-8")
print("Columnas:", df.columns.tolist())
print(df.head())

# Eliminamos caracteres en blanco
df.columns = df.columns.str.strip()

# Eliminar filas sin valor
porcentaje = 'OBS_VALUE'
df = df.dropna(subset=[porcentaje])

year = 'TIME_PERIOD_CODE'
df[year] = df[year].astype(int)

df = df.rename(columns={
    "TERRITORIO#es": "municipio",
    year: "anio",
    porcentaje: "porcentaje",
    "MEDIDAS#es": "medida"
})

# Separamos en comunidad autonoma, provincia y municipio
territorio_code = 'TERRITORIO_CODE'
df_municipios = df[df[territorio_code].str.match(r"^\d{5}$")].copy()
df_provincias = df[df[territorio_code].isin(["ES701", "ES702"])].copy()
df_canarias = df[df[territorio_code] == "ES70"].copy()

subtitulo = 'Por tipo de ingreso (porcentaje sobre la renta total)'
x = ['anio', 'Año']
y = ['porcentaje', 'Porcentaje (%)']
color = ['medida', 'Tipo de ingreso']
fill = ['medida', 'Tipo de ingreso']
group = 'medida'

# Comunidad
# Gráfico de líneas
crearGraficoDeLineas(df_canarias, 
                      ruta='evolucion_renta_canarias',
                      titulo='Evolución de la composición de la renta en Canarias',
                      subtitulo=subtitulo,
                      x=x,
                      y=y,
                      color=color,
                      group=group)

# Gráfico de áreas apiladas
crearGraficoDeAreas(df_canarias, 
                    ruta='area_apilada_renta_canarias',
                    titulo="Evolución de la composición de la renta en Canarias",
                    subtitulo=subtitulo,
                    x=x,
                    y=y,
                    fill=fill)

# Provincias
# Tenerife
# Gráfico de líneas
df_provincia_tenerife = df_provincias[df_provincias["TERRITORIO_CODE"].isin(["ES702"])].copy()
crearGraficoDeLineas(df_provincia_tenerife, 
                      ruta='evolucion_renta_tenerife',
                      titulo='Evolución de la composición de la renta en la provincia de Tenerife',
                      subtitulo=subtitulo,
                      x=x,
                      y=y,
                      color=color,
                      group=group)
# Gráfico de áreas apiladas
crearGraficoDeAreas(df_provincia_tenerife, 
                    ruta='area_apilada_renta_tenerife',
                    titulo="Evolución de la composición de la renta en la provincia de Tenerife",
                    subtitulo=subtitulo,
                    x=x,
                    y=y,
                    fill=fill)

# Gran canaria
# Gráfico de líneas
df_provincia_gran_canaria = df_provincias[df_provincias["TERRITORIO_CODE"].isin(["ES701"])].copy()
crearGraficoDeLineas(df_provincia_gran_canaria, 
                      ruta='evolucion_renta_grancanaria',
                      titulo='Evolución de la composición de la renta en la provincia de Gran Canaria',
                      subtitulo=subtitulo,
                      x=x,
                      y=y,
                      color=color,
                      group=group)
# Gráfico de áreas apiladas
crearGraficoDeAreas(df_provincia_gran_canaria, 
                    ruta='area_apilada_renta_grancanaria',
                    titulo="Evolución de la composición de la renta en la provincia de Gran Canaria",
                    subtitulo=subtitulo,
                    x=x,
                    y=y,
                    fill=fill)