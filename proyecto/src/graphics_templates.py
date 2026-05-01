from pathlib import Path
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from plotnine import (
    coord_flip, geom_blank, geom_boxplot, ggplot, aes, geom_col, geom_line, geom_point, geom_tile, geom_map,
    labs, scale_color_brewer, scale_fill_brewer, scale_fill_gradient, scale_fill_gradient2,
    scale_y_continuous, scale_x_continuous, theme_minimal, theme,
    element_text, element_line, coord_fixed, geom_area
)

def save_graphic(graphic, path: Path, name: str, width=10, height=6) -> None:
    path.mkdir(parents=True, exist_ok=True)
    graphic.save(path / f"{name}.png", dpi=150, width=width, height=height)

def save_grid_graphic(
    graficos: list[tuple[str, ggplot]],
    path: Path,
    name: str,
    cols: int = 2,
    width: int = 20,
    height: int = 16,
) -> None:
    path.mkdir(parents=True, exist_ok=True)
    rows = -(-len(graficos) // cols)
    fig, axes = plt.subplots(rows, cols, figsize=(width, height))
    axes = axes.flatten()
    for i, (titulo, g) in enumerate(graficos):
        # Renderizar cada ggplot en su eje
        fig_g = g.draw()
        fig_g.canvas.draw()
        buf = fig_g.canvas.buffer_rgba()
        import numpy as np
        img = np.asarray(buf)
        axes[i].imshow(img)
        axes[i].axis("off")
        plt.close(fig_g)
    # Ocultar ejes sobrantes si graficos no llena el grid
    for j in range(len(graficos), len(axes)):
        axes[j].axis("off")
    plt.tight_layout()
    fig.savefig(path / f"{name}.png", dpi=150)
    plt.close(fig)

def lines_graphic(df, x, y, color, titulo, subtitulo="", xlabel="", ylabel=""):
    return (
        ggplot(df, aes(x=x, y=y, color=color, group=color))
        + geom_line(size=1)
        + geom_point(size=2)
        + labs(title=titulo, subtitle=subtitulo, x=xlabel, y=ylabel, color="")
        + scale_color_brewer(type="qual", palette="Set1")
        + theme_minimal()
        + theme(
            plot_title=element_text(size=14, face="bold"),
            plot_subtitle=element_text(size=10),
            axis_text_x=element_text(rotation=45, hjust=1),
            legend_position="bottom",
            legend_direction="vertical",
            legend_text=element_text(size=8),
        )
    )

def bars_graphic(
    df: pd.DataFrame,
    x: str,
    y: str,
    titulo: str,
    subtitulo: str = "",
    xlabel: str = "",
    ylabel: str = "",
    fill: str = "#2196F3",
    horizontal: bool = True,
    x_text_size: int = 9,
    x_text_rotation: int = 45,
) -> ggplot:
    if horizontal:
        mapping = aes(x=f"reorder({x}, {y})", y=y)
    else:
        mapping = aes(x=f"reorder({x}, {y})", y=y)
    return (
        ggplot(df, mapping)
        + geom_col(fill=fill)
        + labs(title=titulo, subtitle=subtitulo, x=xlabel, y=ylabel)
        + theme_minimal()
        + theme(
            plot_title=element_text(size=14, face="bold"),
            plot_subtitle=element_text(size=10),
            axis_text_x=element_text(rotation=x_text_rotation, hjust=1, size=x_text_size),
            axis_text_y=element_text(size=9),
        )
        + (coord_flip() if horizontal else geom_blank())
    )

def stacked_bars_graphic(
    df, x, y, fill, titulo, subtitulo="", xlabel="", ylabel="",
    palette="Set2", position="stack",
    x_text_size=9, x_text_rotation=45,
    legend_position="right",
):
    return (
        ggplot(df, aes(x=x, y=y, fill=fill))
        + geom_col(position=position)
        + labs(title=titulo, subtitle=subtitulo, x=xlabel, y=ylabel, fill=fill)
        + scale_fill_brewer(type="qual", palette=palette)
        + scale_y_continuous(expand=(0, 0))
        + theme_minimal()
        + theme(
            plot_title=element_text(size=14, face="bold"),
            plot_subtitle=element_text(size=10),
            axis_text_x=element_text(rotation=x_text_rotation, hjust=1, size=x_text_size),
            legend_position=legend_position,
            legend_direction="vertical",
            legend_text=element_text(size=8),
        )
    )

def boxplot_graphic(
    df: pd.DataFrame,
    x: str,
    y: str,
    titulo: str,
    subtitulo: str = "",
    xlabel: str = "",
    ylabel: str = "",
    fill: str = "#2196F3",
    x_text_size: int = 9,
    x_text_rotation: int = 45,
    title_size=10
) -> ggplot:
    return (
        ggplot(df, aes(x=f"reorder({x}, {y})", y=y))
        + geom_boxplot(fill=fill, alpha=0.7)
        + labs(title=titulo, subtitle=subtitulo, x=xlabel, y=ylabel)
        + theme_minimal()
        + theme(
            plot_title=element_text(size=title_size, face="bold"),
            plot_subtitle=element_text(size=10),
            axis_text_x=element_text(rotation=x_text_rotation, hjust=1, size=x_text_size),
        )
    )

def heatmap_graphic(
    df: pd.DataFrame,
    x: str,
    y: str,
    fill: str,
    titulo: str,
    subtitulo: str = "",
    xlabel: str = "",
    ylabel: str = "",
    low: str = "#2166ac",
    mid: str = "#f7f7f7",
    high: str = "#b2182b",
    midpoint: float = 50.0,
    width: int = 10,
    height: int = 8
) -> ggplot:
    n_x = df[x].nunique()
    n_y = df[y].nunique()
    width = max(width, n_x * 0.8)
    height = max(height, n_y * 0.3)
    return (
        ggplot(df, aes(x=x, y=y, fill=fill))
        + geom_tile(color="white", size=0.1)
        + labs(
            title=titulo,
            subtitle=subtitulo,
            x=xlabel,
            y=ylabel,
            fill=fill
        )
        + scale_fill_gradient2(
            low=low,
            mid=mid,
            high=high,
            midpoint=midpoint
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=11, face="bold"),
            plot_subtitle=element_text(size=10),
            axis_text_x=element_text(rotation=45, hjust=1),
            axis_text_y=element_text(size=8),
            legend_position="right",
        )
        + theme(figure_size=(width, height))
    )

def scatter_graphic(
    df: pd.DataFrame,
    x: str,
    y: str,
    titulo: str,
    color: str | None = None,
    size: str | None = None,
    subtitulo: str = "",
    xlabel: str = "",
    ylabel: str = "",
    palette: str = "Set1",
) -> ggplot:
    mapping = aes(x=x, y=y)
    if color:
        mapping.update({'color': color})
    if size:
        mapping.update({'size': size})
    plot = (
        ggplot(df, mapping)
        + geom_point(alpha=0.7, size=3)
        + labs(
            title=titulo,
            subtitle=subtitulo,
            x=xlabel,
            y=ylabel,
            color="Isla" if color == "isla" else color
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=14, face="bold"),
            plot_subtitle=element_text(size=10),
            legend_position="right",
            figure_size=(10, 6)
        )
    )
    if color and df[color].nunique() > 0:
        plot += scale_color_brewer(type="qual", palette=palette)
    return plot

def choropleth_map_graphic(
    gdf: gpd.GeoDataFrame,
    fill: str,
    titulo: str,
    subtitulo: str = "",
    low: str = "#FFF9C4",
    mid: str = "#f7f7f7",
    high: str = "#E53935",
) -> ggplot:
    return (
        ggplot(gdf)
        + geom_map(aes(fill=fill), color="white", size=0.1)
        + labs(title=titulo, subtitle=subtitulo, fill=fill)
        +  scale_fill_gradient2(low=low, mid=mid, high=high, midpoint=gdf[fill].median())
        + coord_fixed()
        + theme_minimal()
        + theme(
            plot_title=element_text(size=14, face="bold"),
            plot_subtitle=element_text(size=10),
            axis_text_x=element_text(size=7),
            axis_text_y=element_text(size=7),
            legend_position="right",
        )
    )