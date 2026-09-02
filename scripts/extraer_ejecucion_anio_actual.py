"""
Extrae la ejecucion (MONTO_DEVENGADO) del anio EN CURSO (por defecto, el
anio de hoy) de los items listados en MATRIZ.xlsx, cruzando contra el CSV
de "Gasto Diario" de datos abiertos del MEF.

Este script SI se debe re-ejecutar periodicamente (semanal/mensual, etc.),
a diferencia de extraer_ejecucion_historica_2021_2025.py que corre una
sola vez porque esos anios ya cerraron.

Misma logica de cruce que el script historico:
  - ACTIVIDAD en MATRIZ ("3999995: DESCRIPCION...") -> codigo antes de ":"
    cruzado contra ACTIVIDAD_ACCION_OBRA.
  - PROYECTO en MATRIZ (numero, ej. 2486294) -> cruzado contra
    PRODUCTO_PROYECTO.
"""

import re
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

COLUMNAS = [
    "ANO_EJE", "MES_EJE", "NIVEL_GOBIERNO", "NIVEL_GOBIERNO_NOMBRE",
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
    "MONTO_DEVENGADO",
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

    codigos_actividad, codigos_proyecto, filas_sin_codigo = set(), set(), []

    for _, fila in matriz.iterrows():
        act, proy = fila["ACTIVIDAD"], fila["PROYECTO"]
        if pd.notna(act):
            m = re.match(r"^\s*(\d+)\s*:", str(act))
            if m:
                codigos_actividad.add(m.group(1))
            else:
                filas_sin_codigo.append(str(act)[:80])
        elif pd.notna(proy):
            codigos_proyecto.add(str(int(proy)))
        else:
            filas_sin_codigo.append("(fila vacia)")

    print(f"MATRIZ: {len(matriz)} filas -> "
          f"{len(codigos_actividad)} codigos ACTIVIDAD, "
          f"{len(codigos_proyecto)} codigos PROYECTO, "
          f"{len(filas_sin_codigo)} sin codigo identificable")
    if filas_sin_codigo:
        print("  Filas excluidas por no tener codigo identificable:")
        for f in filas_sin_codigo:
            print("   -", f)

    return codigos_actividad, codigos_proyecto


def fecha_actualizacion_archivo(url):
    resp = requests.head(url)
    last_modified_raw = resp.headers.get("Last-Modified")
    if not last_modified_raw:
        return "No disponible"
    fecha_gmt = parsedate_to_datetime(last_modified_raw)
    fecha_peru = fecha_gmt.astimezone(timezone(timedelta(hours=-5)))
    return fecha_peru.strftime("%Y-%m-%d %H:%M:%S")


def main():
    codigos_actividad, codigos_proyecto = cargar_codigos_matriz(MATRIZ_PATH)

    con = duckdb.connect()
    con.register("tabla_actividad", pd.DataFrame({"code": list(codigos_actividad)}))
    con.register("tabla_proyecto", pd.DataFrame({"code": list(codigos_proyecto)}))

    tipos = {c: "VARCHAR" for c in TIPOS_VARCHAR}
    tipos.update({"ANO_EJE": "INTEGER", "MES_EJE": "INTEGER"})
    columnas_sql = ", ".join(COLUMNAS)

    query = f"""
        SELECT {columnas_sql}
        FROM read_csv('{URL}', header = true, types = {tipos})
        WHERE ACTIVIDAD_ACCION_OBRA IN (SELECT code FROM tabla_actividad)
           OR PRODUCTO_PROYECTO IN (SELECT code FROM tabla_proyecto)
    """
    print(f"-> Descargando y filtrando {ANIO} desde {URL} ...")
    resultado = con.execute(query).df()
    resultado["FECHA_ACTUALIZACION_ARCHIVO"] = fecha_actualizacion_archivo(URL)
    resultado.to_csv(SALIDA_CSV, index=False)
    print(f"Guardado: {SALIDA_CSV} ({len(resultado)} filas)")

    encontrados_act = set(resultado["ACTIVIDAD_ACCION_OBRA"].dropna().astype(str))
    encontrados_proy = set(resultado["PRODUCTO_PROYECTO"].dropna().astype(str))
    faltantes_act = codigos_actividad - encontrados_act
    faltantes_proy = codigos_proyecto - encontrados_proy

    if faltantes_act or faltantes_proy:
        print(f"\n⚠ Codigos de MATRIZ sin ninguna coincidencia en {ANIO} "
              f"(puede ser normal si el proyecto/actividad aun no ejecuta este anio):")
        for c in sorted(faltantes_act):
            print(f"   ACTIVIDAD {c}")
        for c in sorted(faltantes_proy):
            print(f"   PROYECTO {c}")


if __name__ == "__main__":
    main()
