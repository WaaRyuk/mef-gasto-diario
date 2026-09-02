"""
Extrae la ejecucion (MONTO_DEVENGADO) historica 2021-2025 de los items
listados en MATRIZ.xlsx, cruzando contra los CSV anuales de "Gasto Diario"
de datos abiertos del MEF.

Logica de cruce (una sola columna llena por fila en MATRIZ, nunca ambas):
  - Si la fila tiene ACTIVIDAD ("3999995: DESCRIPCION...") -> se extrae el
    codigo numerico antes de ":" y se cruza contra ACTIVIDAD_ACCION_OBRA.
  - Si la fila tiene PROYECTO (numero, ej. 2486294) -> se cruza contra
    PRODUCTO_PROYECTO.

Como este periodo ya cerro (no cambia), este script se corre UNA sola vez.
Para el año en curso usar extraer_ejecucion_2026.py, que se puede re-ejecutar
periodicamente.
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

# Columnas requeridas, en el orden solicitado
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

# Columnas del CSV que se fuerzan a VARCHAR para no perder ceros a la
# izquierda ni chocar por tipos mixtos entre columnas texto/numero
TIPOS_VARCHAR = [
    "NIVEL_GOBIERNO", "SECTOR", "PLIEGO", "SEC_EJEC", "EJECUTORA",
    "DEPARTAMENTO_EJECUTORA", "PROVINCIA_EJECUTORA", "DISTRITO_EJECUTORA",
    "PROGRAMA_PPTO", "TIPO_ACT_PROY", "PRODUCTO_PROYECTO",
    "ACTIVIDAD_ACCION_OBRA", "DEPARTAMENTO_META",
]


# ---------------------------------------------------------------------------
# 1. Leer MATRIZ.xlsx y extraer codigos de ACTIVIDAD / PROYECTO
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# 2. Consultar un anio del CSV de Gasto Diario del MEF
# ---------------------------------------------------------------------------
def consultar_anio(con, anio, tabla_act, tabla_proy):
    url = URL_TEMPLATE.format(anio=anio)
    tipos = {c: "VARCHAR" for c in TIPOS_VARCHAR}
    tipos.update({"ANO_EJE": "INTEGER", "MES_EJE": "INTEGER"})

    columnas_sql = ", ".join(COLUMNAS)
    query = f"""
        SELECT {columnas_sql}
        FROM read_csv('{url}', header = true, types = {tipos})
        WHERE ACTIVIDAD_ACCION_OBRA IN (SELECT code FROM {tabla_act})
           OR PRODUCTO_PROYECTO IN (SELECT code FROM {tabla_proy})
    """
    print(f"-> Descargando y filtrando {anio} desde {url} ...")
    df = con.execute(query).df()
    print(f"   {anio}: {len(df)} filas encontradas")
    return df


# ---------------------------------------------------------------------------
# 3. Main
# ---------------------------------------------------------------------------
def main():
    codigos_actividad, codigos_proyecto = cargar_codigos_matriz(MATRIZ_PATH)

    con = duckdb.connect()
    con.register("tabla_actividad", pd.DataFrame({"code": list(codigos_actividad)}))
    con.register("tabla_proyecto", pd.DataFrame({"code": list(codigos_proyecto)}))

    resultados = []
    for anio in ANIOS:
        try:
            df_anio = consultar_anio(con, anio, "tabla_actividad", "tabla_proyecto")
            resultados.append(df_anio)
        except Exception as e:
            print(f"   ⚠ Error al procesar {anio}: {e}")

    resultado_final = pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame(columns=COLUMNAS)
    resultado_final.to_csv(SALIDA_CSV, index=False)
    print(f"\nGuardado: {SALIDA_CSV} ({len(resultado_final)} filas totales)")

    # Validacion: codigos de MATRIZ que no aparecieron en ningun anio
    encontrados_act = set(resultado_final["ACTIVIDAD_ACCION_OBRA"].dropna().astype(str))
    encontrados_proy = set(resultado_final["PRODUCTO_PROYECTO"].dropna().astype(str))
    faltantes_act = codigos_actividad - encontrados_act
    faltantes_proy = codigos_proyecto - encontrados_proy

    if faltantes_act or faltantes_proy:
        print("\n⚠ Codigos de MATRIZ sin ninguna coincidencia en 2021-2025:")
        for c in sorted(faltantes_act):
            print(f"   ACTIVIDAD {c}")
        for c in sorted(faltantes_proy):
            print(f"   PROYECTO {c}")
    else:
        print("\nTodos los codigos de MATRIZ tuvieron al menos una coincidencia.")


if __name__ == "__main__":
    main()
