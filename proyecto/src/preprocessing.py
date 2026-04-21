# preprocessing.py
import pandas as pd
from dagster import asset

NOMBRES_MUNICIPIO = {
    'Guancha, La': 'La Guancha',
    'Orotava, La': 'La Orotava',
    'Realejos, Los': 'Los Realejos',
    'Silos, Los': 'Los Silos',
    'Llanos de Aridane, Los': 'Los Llanos de Aridane',
    'Matanza de Acentejo, La': 'La Matanza de Acentejo',
    'Paso, El': 'El Paso',
    'Rosario, El': 'El Rosario',
    'Sauzal, El': 'El Sauzal',
    'Tanque, El': 'El Tanque',
    'Victoria de Acentejo, La': 'La Victoria de Acentejo',
    'Pinar de El Hierro, El': 'El Pinar de El Hierro',
}

@asset
def actividad_sc_clean(actividad_sc: pd.DataFrame) -> pd.DataFrame:
    df = actividad_sc.rename(columns={
        actividad_sc.columns[0]: "actividad",
        "cod_municipio": "municipio_cod",
        "Periodo": "año",
        "Sexo": "sexo",
    })
    df["municipio"] = df["municipio"].str.strip().replace(NOMBRES_MUNICIPIO)
    df["num_casos"] = df["num_casos"].fillna(0).astype(int)
    df["seccion_key"] = df["geocode"].str[9:]
    return df

@asset
def ocupacion_sc_clean(ocupacion_sc: pd.DataFrame) -> pd.DataFrame:
    df = ocupacion_sc.rename(columns={
        ocupacion_sc.columns[0]: "ocupacion",
        "code_municipio": "municipio_cod",
        "code_distrito": "distrito",
        "code_seccion": "seccion_cod",
    })
    df.columns = [c.encode("latin-1").decode("utf-8") if "Ã" in c else c for c in df.columns]
    df["municipio"] = df["municipio"].str.strip().replace(NOMBRES_MUNICIPIO)
    df["seccion_key"] = df["geocode"].str[9:]
    return df

@asset
def renta_media_clean(renta_media_sc: pd.DataFrame) -> pd.DataFrame:
    df = renta_media_sc.rename(columns={renta_media_sc.columns[0]: "año"})
    df["municipio"] = df["municipio"].str.strip()
    df = df.dropna(subset=["OBS_VALUE"])
    df["seccion_key"] = df["TERRITORIO_CODE"].str[9:]
    return df

@asset
def distribucion_renta_clean(distribucion_renta: pd.DataFrame) -> pd.DataFrame:
    df = distribucion_renta.rename(columns={distribucion_renta.columns[0]: "año"})
    df["municipio"] = df["municipio"].str.strip()
    #df = df.dropna(subset=["OBS_VALUE"])
    df["OBS_VALUE"] = (df["OBS_VALUE"]
                       .str.replace(",", ".", regex=False)
                       .astype(float))
    df["OBS_VALUE"] = df["OBS_VALUE"].fillna(df["OBS_VALUE"].mean())
    df["seccion_key"] = df["TERRITORIO_CODE"].str[9:]
    return df

preprocess_assets = [
    actividad_sc_clean,
    ocupacion_sc_clean,
    renta_media_clean,
    distribucion_renta_clean
]