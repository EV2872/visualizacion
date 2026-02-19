from pathlib import Path
import pandas as pd
from dagster import asset

PROJECT_ROOT = Path(__file__).parent.parent  # practica2/
DATA_DIR = PROJECT_ROOT / "data"

@asset
def renta_canarias_df() -> pd.DataFrame:
    df = pd.read_csv(
        DATA_DIR / "distribucion-renta-canarias.csv",
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

@asset
def codislas_df() -> pd.DataFrame:
    df = pd.read_csv(
        DATA_DIR / "codislas.csv",
        sep=";",
        encoding="latin1"
    )

    df.columns = df.columns.str.strip().str.lower()

    df = df.rename(columns={
        "cpro": "provincia_code",
        "cisla": "isla_code",
        "cmun": "municipio_code",
        "nombre": "nombre_municipio"
    })

    df["municipio_code"] = (
        df["provincia_code"].astype(str).str.zfill(2)
        + df["municipio_code"].astype(str).str.zfill(3)
    )

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

@asset(deps=[renta_municipios, codislas_df])
def renta_municipios_enriquecida(
    renta_municipios: pd.DataFrame,
    codislas_df: pd.DataFrame
) -> pd.DataFrame:

    df = renta_municipios.copy()

    df = df.merge(
        codislas_df[["municipio_code", "nombre_municipio", "isla_code"]],
        left_on="TERRITORIO_CODE",
        right_on="municipio_code",
        how="left"
    )

    return df

@asset
def nivel_estudios_df() -> pd.DataFrame:
    df = pd.read_excel(DATA_DIR / "nivelestudios.xlsx")
    df.columns = df.columns.str.strip()
    df['Periodo'] = pd.to_datetime(df['Periodo'])
    return df