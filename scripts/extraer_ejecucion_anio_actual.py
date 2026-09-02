"""
Extrae la ejecucion (MONTO_DEVENGADO) y el presupuesto (MONTO_PIM) del anio
EN CURSO (por defecto, el anio de hoy) de los PROYECTOS listados en
MATRIZ.xlsx (columna B), cruzando contra el CSV de "Gasto Diario" de datos
abiertos del MEF.

Este script SI se debe re-ejecutar periodicamente (a diferencia de
extraer_ejecucion_historica_2021_2025.py, que corre una sola vez porque
esos anios ya cerraron).

Logica de cruce:
  - Solo se usa la columna PROYECTO (numero, ej. 2486294) de MATRIZ.xlsx,
    cruzada contra PRODUCTO_PROYECTO. La columna ACTIVIDAD ya NO se usa
    para buscar.

Sobre la agregacion de montos (IMPORTANTE):
  El CSV trae, para cada "linea presupuestal" (una combinacion especifica
  de SEC_FUNC, fuente de financiamiento, clasificador de gasto, etc.), una
  fila por mes. El PIM se repite igual en cada una de esas filas
  mensuales, pero puede haber VARIAS lineas distintas bajo la misma
  Actividad/Proyecto (por ejemplo, si hubo una modificacion presupuestal
  que agrego una fuente de financiamiento nueva). Por eso la agregacion
  se hace en dos pasos:
    1) Se colapsan los meses de CADA linea individual: se toma el PIM de
       esa linea con MAX (no cambia entre meses) y se SUMA su devengado
       mensual.
    2) Recien ahi se suman todas las lineas para llegar al nivel de
       Actividad/Proyecto/Departamento que pediste. Asi el PIM incluye
       las modificaciones (lineas nuevas) sin duplicarse por mes.
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
ANIO = datetime.now().year  # cambiar manualmente si se necesita otro anio
URL = f"https://fs.datosabiertos.mef.gob.pe/datastorefiles/{ANIO}-Gasto-Diario.csv"
SALIDA_CSV = f"ejecucion_{ANIO}.csv"

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
]

COLUMNAS_MONETARIAS = [
    "MONTO_PIA", "MONTO_CERTIFICADO", "MONTO_COMPROMETIDO_ANUAL",
    "MONTO_COMPROMETIDO", "MONTO_GIRADO", "MONTO_PIM", "MONTO_DEVENGADO",
]

TIPOS_VARCHAR = [
    "NIVEL_GOBIERNO", "SECTOR", "PLIEGO", "SEC_EJEC", "EJECUTORA",
    "DEPARTAMENTO_EJECUTORA", "PROVINCIA_EJECUTORA", "DISTRITO_EJECUTORA",
    "PROGRAMA_PPTO", "TIPO_ACT_PROY", "PRODUCTO_PROYECTO",
    "ACTIVIDAD_ACCION_OBRA", "DEPARTAMENTO_META",
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
    if filas_sin_codigo:
        print("  Filas excluidas por no tener codigo en la columna PROYECTO:")
        for f in filas_sin_codigo:
            print("   -", f)

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
        ),
        detalle AS (
            -- Paso 1: colapsar los meses de CADA linea presupuestal individual
            SELECT * EXCLUDE ({excluir_sql}),
                   MAX(MONTO_PIM) AS pim_linea,
                   SUM(MONTO_DEVENGADO) AS devengado_linea
            FROM base
            GROUP BY ALL
        )
        -- Paso 2: sumar todas las lineas al nivel de agregacion pedido
        SELECT {columnas_sql},
               SUM(pim_linea) AS MONTO_PIM,
               SUM(devengado_linea) AS MONTO_DEVENGADO
        FROM detalle
        GROUP BY {columnas_sql}
    """
    print(f"-> Descargando y filtrando {ANIO} desde {URL} ...")
    resultado = con.execute(query).df()
    resultado["FECHA_ACTUALIZACION_ARCHIVO"] = fecha_actualizacion_archivo(URL)
    resultado.to_csv(SALIDA_CSV, index=False)
    print(f"Guardado: {SALIDA_CSV} ({len(resultado)} filas, agrupadas sin MES_EJE)")

    encontrados_proy = set(resultado["PRODUCTO_PROYECTO"].dropna().astype(str))
    faltantes_proy = codigos_proyecto - encontrados_proy

    if faltantes_proy:
        print(f"\n⚠ Codigos PROYECTO de MATRIZ sin ninguna coincidencia en {ANIO} "
              f"(puede ser normal si el proyecto aun no ejecuta este anio):")
        for c in sorted(faltantes_proy):
            print(f"   PROYECTO {c}")


if __name__ == "__main__":
    main()
