from pathlib import Path
import pandas as pd
from dagster import asset, asset_check, AssetCheckResult

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

@asset_check(asset=renta_canarias_df)
def check_columnas_renta_canarias(df: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = {
        "TERRITORIO_CODE",
        "municipio",
        "anio",
        "porcentaje",
        "medida"
    }
    faltantes = columnas_esperadas - set(df.columns)

    return AssetCheckResult(
        passed=len(faltantes) == 0,
        metadata={"columnas_faltantes": list(faltantes)}
    )

@asset_check(asset=renta_canarias_df)
def check_tipos_renta_canarias(df: pd.DataFrame) -> AssetCheckResult:
    passed = (
        pd.api.types.is_integer_dtype(df["anio"]) and
        pd.api.types.is_numeric_dtype(df["porcentaje"])
    )

    return AssetCheckResult(passed=passed)

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

@asset_check(asset=renta_canarias)
def check_sin_nulos_renta_canarias(df: pd.DataFrame) -> AssetCheckResult:
    nulos = df[["anio", "porcentaje", "medida"]].isna().sum().to_dict()

    return AssetCheckResult(
        passed=all(v == 0 for v in nulos.values()),
        metadata=nulos
    )

@asset_check(asset=renta_canarias)
def check_porcentajes_validos(df: pd.DataFrame) -> AssetCheckResult:
    fuera_rango = df[
        (df["porcentaje"] < 0) | (df["porcentaje"] > 100)
    ]

    return AssetCheckResult(
        passed=fuera_rango.empty,
        metadata={"filas_fuera_rango": len(fuera_rango)}
    )

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

@asset_check(asset=renta_municipios_enriquecida)
def check_merge_municipios(df: pd.DataFrame) -> AssetCheckResult:
    sin_nombre = int(df["nombre_municipio"].isna().sum())

    return AssetCheckResult(
        passed=sin_nombre == 0,
        metadata={"municipios_sin_nombre": sin_nombre}
    )

@asset
def nivel_estudios_df() -> pd.DataFrame:
    df = pd.read_excel(DATA_DIR / "nivelestudios.xlsx")
    df.columns = df.columns.str.strip()
    df['Periodo'] = pd.to_datetime(df['Periodo'])
    return df