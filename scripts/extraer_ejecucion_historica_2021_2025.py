"""
Extrae la ejecucion (MONTO_DEVENGADO) y el presupuesto (MONTO_PIM) historicos
2021-2025 de los PROYECTOS listados en MATRIZ.xlsx (columna B), cruzando
contra los CSV anuales de "Gasto Diario" de datos abiertos del MEF.

Logica de cruce:
  - Solo se usa la columna PROYECTO (numero, ej. 2486294) de MATRIZ.xlsx,
    cruzada contra PRODUCTO_PROYECTO. La columna ACTIVIDAD ya NO se usa
    para buscar.

Los datos vienen a nivel mensual en el CSV original; aqui se agrupan
quitando MES_EJE para no repetir filas por cada mes:
  - MONTO_DEVENGADO: se SUMA entre los meses del anio (es incremental).
  - MONTO_PIM: se toma el MAXIMO del anio (es el presupuesto asignado,
    se repite igual en cada fila mensual, sumarlo lo multiplicaria).

Como este periodo ya cerro (no cambia), este script se corre UNA sola vez.
Para el año en curso usar extraer_ejecucion_anio_actual.py.
"""

import re
import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
MATRIZ_PATH = "MATRIZ.xlsx"
ANIOS = range(2021, 2026)  # 2021, 2022, 2023, 2024, 2025
URL_TEMPLATE = "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto-Diario.csv"
SALIDA_CSV = "ejecucion_historica_2021_2025.csv"

# Columnas de dimension (todo menos los montos), sin MES_EJE
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

# Columnas del CSV que se fuerzan a VARCHAR para no perder ceros a la
# izquierda ni chocar por tipos mixtos entre columnas texto/numero
TIPOS_VARCHAR = [
    "NIVEL_GOBIERNO", "SECTOR", "PLIEGO", "SEC_EJEC", "EJECUTORA",
    "DEPARTAMENTO_EJECUTORA", "PROVINCIA_EJECUTORA", "DISTRITO_EJECUTORA",
    "PROGRAMA_PPTO", "TIPO_ACT_PROY", "PRODUCTO_PROYECTO",
    "ACTIVIDAD_ACCION_OBRA", "DEPARTAMENTO_META",
]


# ---------------------------------------------------------------------------
# 1. Leer MATRIZ.xlsx y extraer codigos de PROYECTO (columna B)
# ---------------------------------------------------------------------------
def cargar_codigos_matriz(path):
    matriz = pd.read_excel(path, sheet_name=0, usecols=[0, 1],
                            names=["ACTIVIDAD", "PROYECTO"], header=0)

    codigos_proyecto, filas_sin_codigo = set(), []

    for _, fila in matriz.iterrows():
        proy = fila["PROYECTO"]
        if pd.notna(proy):
            codigos_proyecto.add(str(int(proy)))
        else:
            # La columna ACTIVIDAD ya no se usa para buscar
            ref = fila["ACTIVIDAD"]
            filas_sin_codigo.append(str(ref)[:80] if pd.notna(ref) else "(fila vacia)")

    print(f"MATRIZ: {len(matriz)} filas -> {len(codigos_proyecto)} codigos PROYECTO, "
          f"{len(filas_sin_codigo)} sin codigo PROYECTO (se excluyen)")
    if filas_sin_codigo:
        print("  Filas excluidas por no tener codigo en la columna PROYECTO:")
        for f in filas_sin_codigo:
            print("   -", f)

    return codigos_proyecto


# ---------------------------------------------------------------------------
# 2. Consultar un anio del CSV de Gasto Diario del MEF, agrupado sin MES_EJE
# ---------------------------------------------------------------------------
def consultar_anio(con, anio, tabla_proy):
    url = URL_TEMPLATE.format(anio=anio)
    tipos = {c: "VARCHAR" for c in TIPOS_VARCHAR}
    tipos["ANO_EJE"] = "INTEGER"
    tipos["MES_EJE"] = "INTEGER"  # sigue en el CSV origen, solo no se selecciona

    columnas_sql = ", ".join(COLUMNAS_DIM)
    query = f"""
        SELECT {columnas_sql},
               MAX(MONTO_PIM) AS MONTO_PIM,
               SUM(MONTO_DEVENGADO) AS MONTO_DEVENGADO
        FROM read_csv('{url}', header = true, types = {tipos})
        WHERE PRODUCTO_PROYECTO IN (SELECT code FROM {tabla_proy})
        GROUP BY {columnas_sql}
    """
    print(f"-> Descargando y filtrando {anio} desde {url} ...")
    df = con.execute(query).df()
    print(f"   {anio}: {len(df)} filas (agrupadas, sin MES_EJE)")
    return df


# ---------------------------------------------------------------------------
# 3. Main
# ---------------------------------------------------------------------------
def main():
    codigos_proyecto = cargar_codigos_matriz(MATRIZ_PATH)

    con = duckdb.connect()
    con.register("tabla_proyecto", pd.DataFrame({"code": list(codigos_proyecto)}))

    resultados = []
    for anio in ANIOS:
        try:
            df_anio = consultar_anio(con, anio, "tabla_proyecto")
            resultados.append(df_anio)
        except Exception as e:
            print(f"   ⚠ Error al procesar {anio}: {e}")

    columnas_final = COLUMNAS_DIM + ["MONTO_PIM", "MONTO_DEVENGADO"]
    resultado_final = pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame(columns=columnas_final)
    resultado_final.to_csv(SALIDA_CSV, index=False)
    print(f"\nGuardado: {SALIDA_CSV} ({len(resultado_final)} filas totales)")

    # Validacion: codigos de MATRIZ que no aparecieron en ningun anio
    encontrados_proy = set(resultado_final["PRODUCTO_PROYECTO"].dropna().astype(str))
    faltantes_proy = codigos_proyecto - encontrados_proy

    if faltantes_proy:
        print("\n⚠ Codigos PROYECTO de MATRIZ sin ninguna coincidencia en 2021-2025:")
        for c in sorted(faltantes_proy):
            print(f"   PROYECTO {c}")
    else:
        print("\nTodos los codigos PROYECTO de MATRIZ tuvieron al menos una coincidencia.")


if __name__ == "__main__":
    main()
