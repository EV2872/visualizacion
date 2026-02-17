import pandas as pd
from plotnine import (
    ggplot, aes, geom_col, geom_text, coord_flip,
    scale_fill_brewer, labs, theme_minimal, theme,
    element_text, scale_x_continuous
)

# Cargamos los datos
df = pd.read_csv("data/distribucion-renta-canarias.csv", sep=",", encoding="utf-8")
print("Columnas:", df.columns.tolist())
print(df.head())

# Eliminamos caracteres en blanco
df.columns = df.columns.str.strip()

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