import re, requests, pandas as pd, subprocess
from dagster import asset, asset_check, Output, AssetCheckResult, MetadataValue
from plotnine import *

# ----------------------------TEMPLATE GENERICO--------------------------------------------
def generic_template_ia(df: pd.DataFrame, descripcion: str) -> dict:
    columnas = ", ".join(df.columns)
    muestra = df.head(3).to_string(index=False)
    template_tecnico = """
def generar_plot(df):
    # plot = (ggplot(df, aes(...)) + geom_...)
    # return plot
"""
    system_content = (
    "Eres un experto en la gramática de gráficos y Plotnine. "
    "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
    f"Usa siempre este template: {template_tecnico}. "
    "El DataFrame 'df' ya está disponible, no lo cargues de fichero. "
    "SINTAXIS OBLIGATORIA: todas las capas se unen con + dentro de una expresión. "
    "theme() y element_text() NUNCA son capas independientes, siempre van dentro de theme(). "
    "Devuelve exclusivamente el código Python, sin explicaciones ni bloques markdown."
)
    user_content = f"""
El DataFrame tiene estas columnas: {columnas}
Muestra de los datos:
{muestra}
Descripción del gráfico:
{descripcion}
Completa el template para generar este gráfico con plotnine.
"""
    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user",   "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


def llamar_ia_y_limpiar(context, template: dict) -> str:
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234"}
    response = requests.post(url, json=template, headers=headers, timeout=60)
    response.raise_for_status()
    codigo_raw = response.json()['choices'][0]['message']['content']

    match = re.search(r"```python\s+(.*?)\s+```", codigo_raw, re.DOTALL)
    if match:
        codigo_final = match.group(1)
    else:
        lineas = codigo_raw.split("\n")
        lineas_validas = []
        for l in lineas:
            if not l.strip().startswith("###") and not l.strip().startswith("-"):
                lineas_validas.append(l)
        codigo_final = "\n".join(lineas_validas)

    return codigo_final.strip()

'''
def ejecutar_y_guardar(context, codigo: str, df: pd.DataFrame, ruta: str):
    import plotnine
    
    # LOG para ver qué generó la IA
    context.log.info(f"Código generado por la IA:\n{codigo}")
    
    entorno = globals().copy()
    entorno['plotnine'] = plotnine
    entorno.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')})
    entorno['pd'] = pd
    try:
        exec(codigo, entorno)
        grafico = entorno['generar_plot'](df)
        grafico.save(ruta, width=10, height=6, dpi=100)
        subprocess.run(["git", "add", ruta])
        subprocess.run(["git", "commit", "-m", f"Gráfico actualizado: {ruta}"])
        subprocess.run(["git", "push"])
        return Output(value=ruta, metadata={"ruta": ruta})
    except Exception as e:
        context.log.error(f"Error al renderizar: {e}")
        raise
'''

def ejecutar_y_guardar(context, codigo: str, df: pd.DataFrame, ruta: str):
    import plotnine, os

    context.log.info(f"Código generado por la IA:\n{codigo}")

    raiz_repo = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    docs_dir = os.path.join(raiz_repo, "docs")
    os.makedirs(docs_dir, exist_ok=True)  # crea docs/ si no existe
    ruta_completa = os.path.join(docs_dir, ruta)

    context.log.info(f"Raíz repo detectada: {raiz_repo}")
    context.log.info(f"Guardando en: {ruta_completa}")

    entorno = globals().copy()
    entorno['plotnine'] = plotnine
    entorno.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')})
    entorno['pd'] = pd
    try:
        exec(codigo, entorno)
        grafico = entorno['generar_plot'](df)
        grafico.save(ruta_completa, width=10, height=6, dpi=100)
        subprocess.run(["git", "add", ruta_completa], cwd=raiz_repo)
        subprocess.run(["git", "commit", "-m", f"Gráfico actualizado: {ruta}"], cwd=raiz_repo)
        subprocess.run(["git", "push"], cwd=raiz_repo)
        return Output(value=str(ruta_completa), metadata={"ruta": str(ruta_completa)})
    except Exception as e:
        context.log.error(f"Error al renderizar: {e}")
        raise