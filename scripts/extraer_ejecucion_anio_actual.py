"""
Igual que extraer_ejecucion_anio_actual.py, pero filtrando SOLO el
RUBRO 13 (DONACIONES Y TRANSFERENCIAS).

- Cruce: columna B (PROYECTO) de MATRIZ.xlsx contra PRODUCTO_PROYECTO.
- Filtro adicional: RUBRO = '13', aplicado ANTES de agregar.
- Agregacion en dos pasos, igual que el script original.

Se re-ejecuta periodicamente.
"""

from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import duckdb
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
MATRIZ_PATH = "MATRIZ.xlsx"
ANIO = datetime.now().year
RUBRO_FILTRO = "13"  # DONACIONES Y TRANSFERENCIAS
URL = f"https://fs.datosabiertos.mef.gob.pe/datastorefiles/{ANIO}-Gasto-Diario.csv"
SALIDA_CSV = f"ejecucion_{ANIO}_rubro13.csv"

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


def fecha_actualizacion_archivo(url):
    resp = requests.head(url)
    last_modified_raw = resp.headers.get("Last-Modified")
    if not last_modified_raw:
        return "No disponible"
    fecha_gmt = parsedate_to_datetime(last_modified_raw)
    fecha_peru = fecha_gmt.astimezone(timezone(timedelta(hours=-5)))
    return fecha_peru.strftime("%Y-%m-%d %H:%M:%S")


def main():
    codigos_proyecto = cargar_codigos_matriz(MATRIZ_PATH)

    con = duckdb.connect()
    con.register("tabla_proyecto", pd.DataFrame({"code": list(codigos_proyecto)}))

    tipos = {c: "VARCHAR" for c in TIPOS_VARCHAR}
    tipos["ANO_EJE"] = "INTEGER"
    tipos["MES_EJE"] = "INTEGER"
    columnas_sql = ", ".join(COLUMNAS_DIM)
    excluir_sql = ", ".join(["MES_EJE"] + COLUMNAS_MONETARIAS)

    query = f"""
        WITH base AS (
            SELECT * FROM read_csv('{URL}', header = true, types = {tipos})
            WHERE PRODUCTO_PROYECTO IN (SELECT code FROM tabla_proyecto)
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
    print(f"-> {ANIO} (RUBRO {RUBRO_FILTRO}) desde {URL} ...")
    resultado = con.execute(query).df()
    resultado["MONTO_PIM"] = pd.to_numeric(resultado["MONTO_PIM"], errors="coerce")
    resultado["MONTO_DEVENGADO"] = pd.to_numeric(resultado["MONTO_DEVENGADO"], errors="coerce")
    resultado["FECHA_ACTUALIZACION_ARCHIVO"] = fecha_actualizacion_archivo(URL)
    resultado.to_csv(SALIDA_CSV, index=False)
    print(f"Guardado: {SALIDA_CSV} ({len(resultado)} filas)")

    encontrados = set(resultado["PRODUCTO_PROYECTO"].dropna().astype(str))
    faltantes = codigos_proyecto - encontrados
    if faltantes:
        print(f"\n{len(faltantes)} proyectos de MATRIZ sin ejecucion en RUBRO {RUBRO_FILTRO} "
              f"en {ANIO} (normal si se financian con otro rubro):")
        for c in sorted(faltantes):
            print(f"   PROYECTO {c}")


if __name__ == "__main__":
    main()
