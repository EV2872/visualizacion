import pandas as pd
from dagster import AssetCheckResult, asset, asset_check

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

#============== PREPROCESSING ASSETS ==============
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
    df["OBS_VALUE"] = (df["OBS_VALUE"]
                       .str.replace(",", ".", regex=False)
                       .astype(float))
    df = df.dropna(subset=["OBS_VALUE"])
    df["seccion_key"] = df["TERRITORIO_CODE"].str[9:]
    return df

#============== PREPROCESSING CHECKS ==============
@asset_check(asset=actividad_sc_clean)
def check_actividad_columna_renombrada(actividad_sc_clean: pd.DataFrame) -> AssetCheckResult:
    existe = "actividad" in actividad_sc_clean.columns
    return AssetCheckResult(
        passed=existe,
        metadata={"columnas": list(actividad_sc_clean.columns)}
    )

# rename: cod_municipio → municipio_cod
@asset_check(asset=actividad_sc_clean)
def check_actividad_municipio_cod_renombrado(actividad_sc_clean: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed="municipio_cod" in actividad_sc_clean.columns and "cod_municipio" not in actividad_sc_clean.columns,
        metadata={"columnas": list(actividad_sc_clean.columns)}
    )

# rename: Periodo → año
@asset_check(asset=actividad_sc_clean)
def check_actividad_anio_renombrado(actividad_sc_clean: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed="año" in actividad_sc_clean.columns and "Periodo" not in actividad_sc_clean.columns,
        metadata={"columnas": list(actividad_sc_clean.columns)}
    )

# rename: Sexo → sexo
@asset_check(asset=actividad_sc_clean)
def check_actividad_sexo_renombrado(actividad_sc_clean: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed="sexo" in actividad_sc_clean.columns and "Sexo" not in actividad_sc_clean.columns,
        metadata={"columnas": list(actividad_sc_clean.columns)}
    )

# municipio strip + normalizado
@asset_check(asset=actividad_sc_clean)
def check_actividad_municipios_normalizados(actividad_sc_clean: pd.DataFrame) -> AssetCheckResult:
    invertidos = [v for v in actividad_sc_clean["municipio"].unique() if "," in v]
    espacios = [v for v in actividad_sc_clean["municipio"].unique() if v != v.strip()]
    return AssetCheckResult(
        passed=len(invertidos) == 0 and len(espacios) == 0,
        metadata={"municipios_invertidos": invertidos, "municipios_con_espacios": espacios}
    )

# num_casos fillna(0) + astype(int)
@asset_check(asset=actividad_sc_clean)
def check_actividad_num_casos_sin_nulos(actividad_sc_clean: pd.DataFrame) -> AssetCheckResult:
    nulos = actividad_sc_clean["num_casos"].isnull().sum()
    es_int = actividad_sc_clean["num_casos"].dtype == "int64"
    return AssetCheckResult(
        passed=nulos == 0 and es_int,
        metadata={"nulos": int(nulos), "dtype": str(actividad_sc_clean["num_casos"].dtype)}
    )

# seccion_key creada correctamente
@asset_check(asset=actividad_sc_clean)
def check_actividad_seccion_key(actividad_sc_clean: pd.DataFrame) -> AssetCheckResult:
    existe = "seccion_key" in actividad_sc_clean.columns
    formato_valido = actividad_sc_clean["seccion_key"].str.match(r"^\d{5}_D\d+_S\d+$").all() if existe else False
    return AssetCheckResult(
        passed=bool(existe and formato_valido),
        metadata={"existe": existe, "ejemplo": actividad_sc_clean["seccion_key"].iloc[0] if existe else None}
    )

# ── ocupacion_sc_clean ────────────────────────────────────────────────────────
# rename: columna BOM → "ocupacion"
@asset_check(asset=ocupacion_sc_clean)
def check_ocupacion_columna_renombrada(ocupacion_sc_clean: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed="ocupacion" in ocupacion_sc_clean.columns,
        metadata={"columnas": list(ocupacion_sc_clean.columns)}
    )

# rename: code_municipio → municipio_cod, code_distrito → distrito, code_seccion → seccion_cod
@asset_check(asset=ocupacion_sc_clean)
def check_ocupacion_columnas_renombradas(ocupacion_sc_clean: pd.DataFrame) -> AssetCheckResult:
    esperadas = {"municipio_cod", "distrito", "seccion_cod"}
    no_esperadas = {"code_municipio", "code_distrito", "code_seccion"}
    cols = set(ocupacion_sc_clean.columns)
    return AssetCheckResult(
        passed=esperadas.issubset(cols) and len(no_esperadas & cols) == 0,
        metadata={"columnas": list(cols)}
    )

# encoding arreglado
@asset_check(asset=ocupacion_sc_clean)
def check_ocupacion_encoding(ocupacion_sc_clean: pd.DataFrame) -> AssetCheckResult:
    columnas_rotas = [c for c in ocupacion_sc_clean.columns if "Ã" in c]
    return AssetCheckResult(
        passed=len(columnas_rotas) == 0,
        metadata={"columnas_encoding_roto": columnas_rotas}
    )

# municipio strip + normalizado
@asset_check(asset=ocupacion_sc_clean)
def check_ocupacion_municipios_normalizados(ocupacion_sc_clean: pd.DataFrame) -> AssetCheckResult:
    invertidos = [v for v in ocupacion_sc_clean["municipio"].unique() if "," in v]
    espacios = [v for v in ocupacion_sc_clean["municipio"].unique() if v != v.strip()]
    return AssetCheckResult(
        passed=len(invertidos) == 0 and len(espacios) == 0,
        metadata={"municipios_invertidos": invertidos, "municipios_con_espacios": espacios}
    )

# seccion_key creada correctamente
@asset_check(asset=ocupacion_sc_clean)
def check_ocupacion_seccion_key(ocupacion_sc_clean: pd.DataFrame) -> AssetCheckResult:
    existe = "seccion_key" in ocupacion_sc_clean.columns
    formato_valido = ocupacion_sc_clean["seccion_key"].str.match(r"^\d{5}_D\d+_S\d+$").all() if existe else False
    return AssetCheckResult(
        passed=bool(existe and formato_valido),
        metadata={"existe": existe, "ejemplo": ocupacion_sc_clean["seccion_key"].iloc[0] if existe else None}
    )

# ── renta_media_clean ─────────────────────────────────────────────────────────
# rename: columna BOM → "año"
@asset_check(asset=renta_media_clean)
def check_renta_anio_renombrado(renta_media_clean: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed="año" in renta_media_clean.columns,
        metadata={"columnas": list(renta_media_clean.columns)}
    )

# municipio strip
@asset_check(asset=renta_media_clean)
def check_renta_municipio_sin_espacios(renta_media_clean: pd.DataFrame) -> AssetCheckResult:
    espacios = [v for v in renta_media_clean["municipio"].unique() if v != v.strip()]
    return AssetCheckResult(
        passed=len(espacios) == 0,
        metadata={"municipios_con_espacios": espacios}
    )

# dropna OBS_VALUE
@asset_check(asset=renta_media_clean)
def check_renta_sin_nulos(renta_media_clean: pd.DataFrame) -> AssetCheckResult:
    nulos = renta_media_clean["OBS_VALUE"].isnull().sum()
    return AssetCheckResult(
        passed=bool(nulos == 0),
        metadata={"nulos_OBS_VALUE": int(nulos)}
    )

# OBS_VALUE positivo (euros)
@asset_check(asset=renta_media_clean)
def check_renta_obs_value_positivo(renta_media_clean: pd.DataFrame) -> AssetCheckResult:
    negativos = (renta_media_clean["OBS_VALUE"] < 0).sum()
    return AssetCheckResult(
        passed=bool(negativos == 0),
        metadata={"valores_negativos": int(negativos)}
    )

# seccion_key creada correctamente desde TERRITORIO_CODE
@asset_check(asset=renta_media_clean)
def check_renta_seccion_key(renta_media_clean: pd.DataFrame) -> AssetCheckResult:
    existe = "seccion_key" in renta_media_clean.columns
    formato_valido = renta_media_clean["seccion_key"].str.match(r"^\d{5}_D\d+_S\d+$").all() if existe else False
    return AssetCheckResult(
        passed=bool(existe and formato_valido),
        metadata={"existe": existe, "ejemplo": renta_media_clean["seccion_key"].iloc[0] if existe else None}
    )

# ── distribucion_renta_clean ──────────────────────────────────────────────────
# rename: columna BOM → "año"
@asset_check(asset=distribucion_renta_clean)
def check_distribucion_anio_renombrado(distribucion_renta_clean: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed="año" in distribucion_renta_clean.columns,
        metadata={"columnas": list(distribucion_renta_clean.columns)}
    )

# municipio strip
@asset_check(asset=distribucion_renta_clean)
def check_distribucion_municipio_sin_espacios(distribucion_renta_clean: pd.DataFrame) -> AssetCheckResult:
    espacios = [v for v in distribucion_renta_clean["municipio"].unique() if v != v.strip()]
    return AssetCheckResult(
        passed=len(espacios) == 0,
        metadata={"municipios_con_espacios": espacios}
    )

# OBS_VALUE convertido a float (coma → punto)
@asset_check(asset=distribucion_renta_clean)
def check_distribucion_obs_value_es_float(distribucion_renta_clean: pd.DataFrame) -> AssetCheckResult:
    es_float = distribucion_renta_clean["OBS_VALUE"].dtype == "float64"
    return AssetCheckResult(
        passed=bool(es_float),
        metadata={"dtype": str(distribucion_renta_clean["OBS_VALUE"].dtype)}
    )

# dropna OBS_VALUE
@asset_check(asset=distribucion_renta_clean)
def check_distribucion_sin_nulos(distribucion_renta_clean: pd.DataFrame) -> AssetCheckResult:
    nulos = distribucion_renta_clean["OBS_VALUE"].isnull().sum()
    return AssetCheckResult(
        passed=bool(nulos == 0),
        metadata={"nulos_OBS_VALUE": int(nulos)}
    )

# OBS_VALUE es porcentaje (0-100)
@asset_check(asset=distribucion_renta_clean)
def check_distribucion_obs_value_rango(distribucion_renta_clean: pd.DataFrame) -> AssetCheckResult:
    fuera_rango = ((distribucion_renta_clean["OBS_VALUE"] < 0) |
                   (distribucion_renta_clean["OBS_VALUE"] > 100)).sum()
    return AssetCheckResult(
        passed=bool(fuera_rango == 0),
        metadata={"valores_fuera_rango": int(fuera_rango)}
    )

# seccion_key creada correctamente desde TERRITORIO_CODE
@asset_check(asset=distribucion_renta_clean)
def check_distribucion_seccion_key(distribucion_renta_clean: pd.DataFrame) -> AssetCheckResult:
    existe = "seccion_key" in distribucion_renta_clean.columns
    formato_valido = distribucion_renta_clean["seccion_key"].str.match(r"^\d{5}_D\d+_S\d+$").all() if existe else False
    return AssetCheckResult(
        passed=bool(existe and formato_valido),
        metadata={"existe": existe, "ejemplo": distribucion_renta_clean["seccion_key"].iloc[0] if existe else None}
    )

preprocess_assets = [
    actividad_sc_clean,
    ocupacion_sc_clean,
    renta_media_clean,
    distribucion_renta_clean
]

preprocess_checks = [
    # actividad_sc_clean
    check_actividad_columna_renombrada,
    check_actividad_municipio_cod_renombrado,
    check_actividad_anio_renombrado,
    check_actividad_sexo_renombrado,
    check_actividad_municipios_normalizados,
    check_actividad_num_casos_sin_nulos,
    check_actividad_seccion_key,
    # ocupacion_sc_clean
    check_ocupacion_columna_renombrada,
    check_ocupacion_columnas_renombradas,
    check_ocupacion_encoding,
    check_ocupacion_municipios_normalizados,
    check_ocupacion_seccion_key,
    # renta_media_clean
    check_renta_anio_renombrado,
    check_renta_municipio_sin_espacios,
    check_renta_sin_nulos,
    check_renta_obs_value_positivo,
    check_renta_seccion_key,
    # distribucion_renta_clean
    check_distribucion_anio_renombrado,
    check_distribucion_municipio_sin_espacios,
    check_distribucion_obs_value_es_float,
    check_distribucion_sin_nulos,
    check_distribucion_obs_value_rango,
    check_distribucion_seccion_key,
]