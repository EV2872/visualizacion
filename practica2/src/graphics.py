from pathlib import Path
import pandas as pd
from plotnine import (
    geom_area, geom_tile, ggplot, aes, geom_col,
    labs, scale_fill_brewer, scale_fill_gradient2, 
    scale_y_continuous, theme_minimal, theme,
    element_text, geom_line, geom_point, element_line
)

PROJECT_ROOT = Path(__file__).parent.parent
GRAFICOS_DIR = PROJECT_ROOT / "graficos"

def crearGraficoDeLineas(df: pd.DataFrame, ruta: str, titulo: str, subtitulo: str, 
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

    grafico_lineas.save(GRAFICOS_DIR / f"{ruta}.png", dpi=150)


def crearGraficoDeAreas(df: pd.DataFrame, ruta: str, titulo: str, subtitulo: str,
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

    grafico_area.save(GRAFICOS_DIR / f"{ruta}.png", dpi=150)

def crearMapaDeCalor(df: pd.DataFrame, ruta: str, titulo: str, subtitulo: str,
                     x: tuple[str, str], y: tuple[str, str], 
                     fill: tuple[str, str]):
    grafico_heatmap = (
        ggplot(
            df,
            aes(
                x=x[0],
                y=y[0],
                fill=fill[0]
            )
        )
        + geom_tile(color="white", size=0.5)
        + scale_fill_gradient2(
            low="#2166ac",
            mid="#f7f7f7",
            high="#b2182b",
            midpoint=df[fill[0]].median()
        )
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
            figure_size=(14, 8),
            axis_text_x=element_text(rotation=45, hjust=1),
            axis_text_y=element_text(size=9),
            legend_position="right",
            panel_grid_major=element_line(color="none"),
            panel_grid_minor=element_line(color="none")
        )
    )
    grafico_heatmap.save(GRAFICOS_DIR / f"{ruta}.png", dpi=150)

def crearGraficoBarrasNivelEstudiosMujeres(df: pd.DataFrame, ruta: str, titulo: str, subtitulo: str):
    # Filtrar solo mujeres
    df_mujeres = df[df['Sexo'] == 'Mujeres'].copy()
    # Extraer año del periodo
    df_mujeres['Año'] = df_mujeres['Periodo'].dt.year
    # Agrupar por año y nivel de estudios
    df_agrupado = df_mujeres.groupby(['Año', 'Nivel de estudios en curso'])['Total'].sum().reset_index()
    # Filtrar solo registros con datos
    df_agrupado = df_agrupado[df_agrupado['Total'] > 0]
    grafico = (
        ggplot(
            df_agrupado,
            aes(
                x='factor(Año)',
                y='Total',
                fill='Nivel de estudios en curso'
            )
        )
        + geom_col(position='dodge', alpha=0.85)
        + scale_fill_brewer(type='qual', palette='Set2')
        + labs(
            title=titulo,
            subtitle=subtitulo,
            x='Año',
            y='Frecuencia (Total mujeres)',
            fill='Nivel de estudios',
            caption='Fuente: ISTAC'
        )
        + theme_minimal()
        + theme(
            figure_size=(14, 8),
            axis_text_x=element_text(rotation=45, hjust=1),
            legend_position='right',
            legend_text=element_text(size=8)
        )
    )
    grafico.save(GRAFICOS_DIR / f"{ruta}.png", dpi=150)