"""
Ejecucion historica 2021-2025 de los proyectos de MATRIZ, filtrando SOLO
el RUBRO 13 (DONACIONES Y TRANSFERENCIAS).

- Cruce: columna B (PROYECTO) de MATRIZ.xlsx contra PRODUCTO_PROYECTO.
- Filtro adicional: RUBRO = '13', aplicado ANTES de agregar, para que el
  PIM y el devengado correspondan solo a las lineas de ese rubro.
- Agregacion en dos pasos (MAX del PIM por linea, SUM del devengado,
  luego suma de lineas), igual que el script original.

Corre una sola vez (2021-2025 ya cerro).
"""

import duckdb
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
MATRIZ_PATH = "MATRIZ.xlsx"
ANIOS = range(2021, 2026)
RUBRO_FILTRO = "13"  # DONACIONES Y TRANSFERENCIAS
SALIDA_CSV = "ejecucion_historica_2021_2025.csv"

PATRONES_URL = [
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto-Diario.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto-Mensual.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto.csv",
]

COLUMNAS_DIM = [
    "ANO_EJE", "NIVEL_GOBIERNO", "NIVEL_GOBIERNO_NOMBRE",
    "SECTOR", "SECTOR_NOMBRE", "PLIEGO", "PLIEGO_NOMBRE", "SEC_EJEC",
    "EJECUTORA", "EJECUTORA_NOMBRE",
    "DEPARTAMENTO_EJECUTORA", "DEPARTAMENTO_EJECUTORA_NOMBRE",
    "PROVINCIA_EJECUTORA", "PROVINCIA_EJECUTORA_NOMBRE",
    "DISTRITO_EJECUTORA", "DISTRITO_EJECUTORA_NOMBRE",
    "PROGRAMA_PPTO", "PROGRAMA_PPTO_NOMBRE",
    "TIPO_ACT_PROY", "TIPO_ACT_PROY_NOMBRE",
    "PRODUCTO_PROYECTO", "PRODUCTO_PROYECTO_NOMBRE",
    "ACTIVIDAD_ACCION_OBRA", "ACTIVIDAD_ACCION_OBRA_NOMBRE",
    "DEPARTAMENTO_META", "DEPARTAMENTO_META_NOMBRE",
    "RUBRO", "RUBRO_NOMBRE",
]

COLUMNAS_MONETARIAS = [
    "MONTO_PIA", "MONTO_CERTIFICADO", "MONTO_COMPROMETIDO_ANUAL",
    "MONTO_COMPROMETIDO", "MONTO_GIRADO", "MONTO_PIM", "MONTO_DEVENGADO",
]

TIPOS_VARCHAR = [
    "NIVEL_GOBIERNO", "SECTOR", "PLIEGO", "SEC_EJEC", "EJECUTORA",
    "DEPARTAMENTO_EJECUTORA", "PROVINCIA_EJECUTORA", "DISTRITO_EJECUTORA",
    "PROGRAMA_PPTO", "TIPO_ACT_PROY", "PRODUCTO_PROYECTO",
    "ACTIVIDAD_ACCION_OBRA", "DEPARTAMENTO_META", "RUBRO",
]


def resolver_url_anio(anio):
    for patron in PATRONES_URL:
        url = patron.format(anio=anio)
        try:
            resp = requests.head(url, allow_redirects=True, timeout=30)
            if resp.status_code == 200:
                return url
        except requests.RequestException:
            continue
    raise FileNotFoundError(
        f"No se encontro ningun archivo valido para {anio}. "
        f"Patrones probados: {[p.format(anio=anio) for p in PATRONES_URL]}"
    )


def cargar_codigos_matriz(path):
    matriz = pd.read_excel(path, sheet_name=0, usecols=[0, 1],
                           names=["ACTIVIDAD", "PROYECTO"], header=0)
    codigos_proyecto, filas_sin_codigo = set(), []
    for _, fila in matriz.iterrows():
        proy = fila["PROYECTO"]
        if pd.notna(proy):
            codigos_proyecto.add(str(int(proy)))
        else:
            ref = fila["ACTIVIDAD"]
            filas_sin_codigo.append(str(ref)[:80] if pd.notna(ref) else "(fila vacia)")
    print(f"MATRIZ: {len(matriz)} filas -> {len(codigos_proyecto)} codigos PROYECTO, "
          f"{len(filas_sin_codigo)} sin codigo PROYECTO (se excluyen)")
    return codigos_proyecto


def consultar_anio(con, anio, tabla_proy):
    url = resolver_url_anio(anio)
    tipos = {c: "VARCHAR" for c in TIPOS_VARCHAR}
    tipos["ANO_EJE"] = "INTEGER"
    tipos["MES_EJE"] = "INTEGER"

    columnas_sql = ", ".join(COLUMNAS_DIM)
    excluir_sql = ", ".join(["MES_EJE"] + COLUMNAS_MONETARIAS)

    query = f"""
        WITH base AS (
            SELECT * FROM read_csv('{url}', header = true, types = {tipos})
            WHERE PRODUCTO_PROYECTO IN (SELECT code FROM {tabla_proy})
              AND TRIM(RUBRO) = '{RUBRO_FILTRO}'
        ),
        detalle AS (
            SELECT * EXCLUDE ({excluir_sql}),
                   MAX(MONTO_PIM) AS pim_linea,
                   SUM(MONTO_DEVENGADO) AS devengado_linea
            FROM base
            GROUP BY ALL
        )
        SELECT {columnas_sql},
               CAST(SUM(pim_linea) AS DOUBLE) AS MONTO_PIM,
               CAST(SUM(devengado_linea) AS DOUBLE) AS MONTO_DEVENGADO
        FROM detalle
        GROUP BY {columnas_sql}
    """
    print(f"-> {anio} (RUBRO {RUBRO_FILTRO}) desde {url} ...")
    df = con.execute(query).df()
    df["MONTO_PIM"] = pd.to_numeric(df["MONTO_PIM"], errors="coerce")
    df["MONTO_DEVENGADO"] = pd.to_numeric(df["MONTO_DEVENGADO"], errors="coerce")
    print(f"   {anio}: {len(df)} filas")
    return df


def main():
    codigos_proyecto = cargar_codigos_matriz(MATRIZ_PATH)

    con = duckdb.connect()
    con.register("tabla_proyecto", pd.DataFrame({"code": list(codigos_proyecto)}))

    resultados = []
    for anio in ANIOS:
        try:
            resultados.append(consultar_anio(con, anio, "tabla_proyecto"))
        except Exception as e:
            print(f"   ⚠ Error al procesar {anio}: {e}")

    columnas_final = COLUMNAS_DIM + ["MONTO_PIM", "MONTO_DEVENGADO"]
    resultado_final = (pd.concat(resultados, ignore_index=True)
                       if resultados else pd.DataFrame(columns=columnas_final))
    resultado_final.to_csv(SALIDA_CSV, index=False)
    print(f"\nGuardado: {SALIDA_CSV} ({len(resultado_final)} filas totales)")

    encontrados = set(resultado_final["PRODUCTO_PROYECTO"].dropna().astype(str))
    faltantes = codigos_proyecto - encontrados
    if faltantes:
        print(f"\n{len(faltantes)} proyectos de MATRIZ sin ejecucion en RUBRO {RUBRO_FILTRO} "
              f"en 2021-2025 (normal si se financian con otro rubro):")
        for c in sorted(faltantes):
            print(f"   PROYECTO {c}")


if __name__ == "__main__":
    main()
