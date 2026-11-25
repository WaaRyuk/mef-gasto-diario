"""
Ejecucion de las transferencias de MINEM a los Gobiernos Regionales para el
fortalecimiento de las Direcciones Regionales de Energia y Minas (vista 5.2.3).

Cadena funcional programatica del convenio (lado GORE):
    Categoria Presupuestal  9001     Acciones Centrales
    Producto                3999999  Sin Producto
    Actividad               5000001  Planeamiento y Presupuesto
    Funcion                 03       Planeamiento, Gestion y Reserva de Contingencia
    Division Funcional      004      Planeamiento Gubernamental
    Grupo Funcional         0005     Planeamiento Institucional
    Finalidad               0044672  Fortalecimiento de los Gobiernos Regionales en
                                     materia de las funciones sectoriales transferidas

Filtro base: NIVEL_GOBIERNO = 'R' y FINALIDAD = 0044672 (todas las GORE).
El resto de la cadena NO se usa como filtro duro: se marca en CADENA_COMPLETA
(1 = coincide en todo) para detectar GORE que registraron la finalidad con otra
actividad o funcion. ES_DYT = 1 marca el rubro 13 (Donaciones y Transferencias), que es como la
GORE incorpora la transferencia financiera de MINEM (S/ 200 000 por GORE al
año, 25 GORE = S/ 5 M, que cuadra con el devengado de MINEM en 2.4.1.3.1.2,
actividad 5001254). El PIM puede ser mayor por saldos de balance de años
anteriores. Nota: META_NOMBRE trae el nombre de la finalidad, no un texto propio.

Salida: output/transferencias_gore.csv, mismo esquema que energia_minas_resumen
(fila MES_EJE = 0 "Sin mes" con PIA/PIM y filas mensuales con la ejecucion).

Uso:
    python scripts/extraer_transferencias_gore.py                  # año en curso
    python scripts/extraer_transferencias_gore.py --anios 2020-2026
    python scripts/extraer_transferencias_gore.py --anios 2024,2025
Cada corrida reemplaza en el CSV solo los años procesados y conserva el resto.
"""
import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import duckdb
import pandas as pd
import requests

PATRONES_URL = [
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto-Diario.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto-Mensual.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto.csv",
]
SALIDA = "output/transferencias_gore.csv"
PERU = timezone(timedelta(hours=-5))

# Codigos de la cadena. Se comparan sin ceros a la izquierda.
FINALIDAD = "44672"
CADENA = {
    "PROGRAMA_PPTO": "9001",
    "PRODUCTO_PROYECTO": "3999999",
    "ACTIVIDAD_ACCION_OBRA": "5000001",
    "FUNCION": "3",
    "DIVISION_FUNCIONAL": "4",
    "GRUPO_FUNCIONAL": "5",
}

COLUMNAS_TEXTO = [
    "NIVEL_GOBIERNO", "PLIEGO", "PLIEGO_NOMBRE", "SEC_EJEC", "EJECUTORA", "EJECUTORA_NOMBRE",
    "DEPARTAMENTO_EJECUTORA_NOMBRE",
    "PROGRAMA_PPTO", "PROGRAMA_PPTO_NOMBRE", "PRODUCTO_PROYECTO", "PRODUCTO_PROYECTO_NOMBRE",
    "ACTIVIDAD_ACCION_OBRA", "ACTIVIDAD_ACCION_OBRA_NOMBRE",
    "FUNCION", "FUNCION_NOMBRE", "DIVISION_FUNCIONAL", "DIVISION_FUNCIONAL_NOMBRE",
    "GRUPO_FUNCIONAL", "GRUPO_FUNCIONAL_NOMBRE",
    "SEC_FUNC", "META", "FINALIDAD", "META_NOMBRE",
    "FUENTE_FINANCIAMIENTO", "FUENTE_FINANCIAMIENTO_NOMBRE", "RUBRO", "RUBRO_NOMBRE",
    "TIPO_TRANSACCION", "GENERICA", "GENERICA_NOMBRE", "SUBGENERICA", "SUBGENERICA_DET",
    "ESPECIFICA", "ESPECIFICA_DET", "ESPECIFICA_DET_NOMBRE",
]
COLUMNAS_ENTERAS = ["ANO_EJE", "MES_EJE"]
COLUMNAS_MONTO = [
    "MONTO_PIA", "MONTO_PIM", "MONTO_CERTIFICADO", "MONTO_COMPROMETIDO_ANUAL",
    "MONTO_COMPROMETIDO", "MONTO_DEVENGADO", "MONTO_GIRADO",
]


def parsear_anios(texto):
    if not texto:
        return [datetime.now(PERU).year]
    anios = set()
    for parte in texto.split(","):
        parte = parte.strip()
        if "-" in parte:
            a, b = (int(x) for x in parte.split("-"))
            anios.update(range(a, b + 1))
        elif parte:
            anios.add(int(parte))
    return sorted(anios)


def resolver_url(anio):
    for patron in PATRONES_URL:
        url = patron.format(anio=anio)
        try:
            r = requests.head(url, allow_redirects=True, timeout=30)
            if r.status_code == 200:
                lm = r.headers.get("Last-Modified")
                fecha = (parsedate_to_datetime(lm).astimezone(PERU).strftime("%Y-%m-%d %H:%M:%S")
                         if lm else "No disponible")
                return url, fecha
        except requests.RequestException as e:
            print(f"   HEAD {url} -> {e}")
    raise RuntimeError(f"No hay archivo de gasto publicado para {anio}")


def sin_ceros(col):
    return f"NULLIF(LTRIM(TRIM({col}), '0'), '')"


def consultar_anio(con, anio):
    url, fecha = resolver_url(anio)
    print(f"-> {anio} desde {url} (Last-Modified {fecha})")

    existentes = {
        f[0].upper(): f[0]
        for f in con.execute(
            f"DESCRIBE SELECT * FROM read_csv('{url}', header = true, all_varchar = true)"
        ).fetchall()
    }
    tipos, select, faltantes = {}, [], []
    for grupo, tipo in ((COLUMNAS_TEXTO, "VARCHAR"), (COLUMNAS_ENTERAS, "INTEGER"),
                        (COLUMNAS_MONTO, "DOUBLE")):
        for c in grupo:
            nombre = existentes.get(c)
            if nombre:
                tipos[nombre] = tipo
                select.append(f'"{nombre}" AS {c}')
            else:
                faltantes.append(c)
                select.append(f"CAST(NULL AS {tipo}) AS {c}")
    if faltantes:
        print(f"   AVISO: el archivo {anio} no trae {faltantes}; quedan vacias.")
    if "FINALIDAD" not in existentes:
        raise RuntimeError(f"El archivo {anio} no trae la columna FINALIDAD")

    tipos_sql = ", ".join(f"'{k}': '{v}'" for k, v in tipos.items())
    # Marca de cadena completa: solo con las columnas que existen en el archivo.
    condiciones = [f"COALESCE({sin_ceros(c)}, '') = '{v}'"
                   for c, v in CADENA.items() if c in existentes]
    cadena_sql = " AND ".join(condiciones) if condiciones else "TRUE"

    df = con.execute(f"""
        WITH base AS (
            SELECT * FROM (
                SELECT {", ".join(select)}
                FROM read_csv('{url}', header = true, types = {{{tipos_sql}}})
            )
            WHERE TRIM(NIVEL_GOBIERNO) = 'R'
              AND {sin_ceros('FINALIDAD')} = '{FINALIDAD}'
        )
        SELECT
            ANO_EJE,
            TRIM(PLIEGO) || '. ' || PLIEGO_NOMBRE                    AS GORE,
            DEPARTAMENTO_EJECUTORA_NOMBRE                             AS DEPARTAMENTO,
            TRIM(SEC_EJEC)                                            AS SEC_EJEC,
            TRIM(EJECUTORA) || '. ' || EJECUTORA_NOMBRE              AS EJECUTORA,
            TRIM(PROGRAMA_PPTO) || '. ' || PROGRAMA_PPTO_NOMBRE      AS CATEGORIA_PPTAL,
            TRIM(PRODUCTO_PROYECTO) || '. ' || PRODUCTO_PROYECTO_NOMBRE AS PRODUCTO,
            TRIM(ACTIVIDAD_ACCION_OBRA) || '. ' || ACTIVIDAD_ACCION_OBRA_NOMBRE AS ACTIVIDAD,
            TRIM(FUNCION) || '. ' || FUNCION_NOMBRE                   AS FUNCION,
            TRIM(DIVISION_FUNCIONAL) || '. ' || DIVISION_FUNCIONAL_NOMBRE AS DIVISION_FUNCIONAL,
            TRIM(GRUPO_FUNCIONAL) || '. ' || GRUPO_FUNCIONAL_NOMBRE   AS GRUPO_FUNCIONAL,
            TRIM(SEC_FUNC)                                            AS SEC_FUNC,
            TRIM(META)                                                AS META,
            TRIM(FINALIDAD)                                           AS FINALIDAD,
            META_NOMBRE,
            TRIM(FUENTE_FINANCIAMIENTO) || '. ' || FUENTE_FINANCIAMIENTO_NOMBRE AS FUENTE_FINANCIAMIENTO,
            TRIM(RUBRO)                                               AS RUBRO,
            RUBRO_NOMBRE,
            TRIM(TIPO_TRANSACCION) || '.' || TRIM(GENERICA) || '.' || GENERICA_NOMBRE AS GENERICA,
            CONCAT_WS('.', TIPO_TRANSACCION, GENERICA, SUBGENERICA, SUBGENERICA_DET,
                      ESPECIFICA, ESPECIFICA_DET, ESPECIFICA_DET_NOMBRE) AS CLASIFICADOR,
            CASE WHEN {cadena_sql} THEN 1 ELSE 0 END                  AS CADENA_COMPLETA,
            CASE WHEN {sin_ceros('RUBRO')} = '13' THEN 1 ELSE 0 END   AS ES_DYT,
            COALESCE(MES_EJE, 0)                                      AS MES_EJE,
            CASE COALESCE(MES_EJE, 0)
                WHEN 1 THEN 'Ene' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Abr'
                WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago'
                WHEN 9 THEN 'Set' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dic'
                ELSE 'Sin mes' END                                     AS MES,
            SUM(MONTO_PIA)                AS pia,
            SUM(MONTO_PIM)                AS pim,
            SUM(MONTO_CERTIFICADO)        AS certificado,
            SUM(MONTO_COMPROMETIDO_ANUAL) AS comp_anual,
            SUM(MONTO_COMPROMETIDO)       AS comprometido,
            SUM(MONTO_DEVENGADO)          AS devengado,
            SUM(MONTO_GIRADO)             AS girado
        FROM base
        GROUP BY ALL
        ORDER BY GORE, EJECUTORA, META, MES_EJE
    """).df()
    df["FECHA_ACTUALIZACION_ARCHIVO"] = fecha

    resumen = (df.groupby(["GORE"], dropna=False)[["pim", "devengado"]].sum()
                 .sort_values("pim", ascending=False))
    print(f"   {len(df)} filas | {df['GORE'].nunique()} GORE | "
          f"PIM {df['pim'].sum():,.2f} | devengado {df['devengado'].sum():,.2f}")
    print(f"   cadena completa: {df.loc[df.CADENA_COMPLETA == 1, 'pim'].sum():,.2f} PIM | "
          f"rubro 13: {df.loc[df.ES_DYT == 1, 'pim'].sum():,.2f} PIM")
    print(resumen.to_string(float_format=lambda x: f"{x:,.2f}"))
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--anios", default=os.environ.get("ANIOS_GORE", ""),
                    help="Ej.: 2020-2026 o 2024,2025. Vacio = año en curso.")
    anios = parsear_anios(ap.parse_args().anios)
    print(f"Años a procesar: {anios}")

    con = duckdb.connect()
    resultados, errores = [], []
    for anio in anios:
        try:
            resultados.append(consultar_anio(con, anio))
        except Exception as e:  # sigue con los demas años
            print(f"   ERROR {anio}: {e}")
            errores.append(anio)

    if not resultados:
        sys.exit("No se proceso ningun año.")
    nuevo = pd.concat(resultados, ignore_index=True)
    procesados = sorted(int(a) for a in nuevo["ANO_EJE"].dropna().unique())

    os.makedirs("output", exist_ok=True)
    if os.path.exists(SALIDA):
        previo = pd.read_csv(SALIDA, dtype=str, keep_default_na=False)
        previo = previo[~previo["ANO_EJE"].astype(int).isin(procesados)]
        nuevo = nuevo.astype(object).where(nuevo.notna(), "").astype(str)
        nuevo = pd.concat([previo, nuevo], ignore_index=True)
    nuevo.to_csv(SALIDA, index=False)
    print(f"Guardado {SALIDA} ({len(nuevo)} filas). Años reemplazados: {procesados}")
    if errores:
        sys.exit(f"Años con error: {errores}")


if __name__ == "__main__":
    main()
