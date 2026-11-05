"""
Resumen diario del gasto MEF (MINEM y ECOFIN/ONP).

Años que lee, segun la fecha en Peru:
- 1 feb a 31 dic: solo el año en curso.
- 1 a 31 ene: el año anterior y, apenas el MEF lo publique, tambien el nuevo.
- Si el año ya cerrado todavia no entro al historico (el corte del 1 de febrero
  fallo o aun no corre), se sigue leyendo para que no desaparezca de ambos lados.

Solo reprocesa si algun archivo del MEF es mas nuevo que lo ya procesado o si
cambio la lista de años a leer.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import duckdb
import pandas as pd
import requests

URL_PLANTILLA = "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto-Diario.csv"

SECTOR_MINEM = "ENERGIA Y MINAS"
SECTOR_ECOFIN = "ECONOMIA Y FINANZAS"
FILTRO_ONP = "%NORMALIZACION PREVISIONAL%"  # ILIKE sobre PLIEGO_NOMBRE

# Fecha de la version vigente (la del año mas reciente). Se mantiene por
# compatibilidad: es la misma que sale en FECHA_ACTUALIZACION_ARCHIVO.
MARCADOR = "output/ultima_actualizacion.txt"
# Last-Modified procesado de cada año que se esta leyendo. Decide si hay algo nuevo.
MARCADOR_ANIOS = "output/ultima_actualizacion_por_anio.json"
# Años que ya estan en el historico. Lo escribe scripts/actualizar_historico.py.
CONTROL_HISTORICO = "output/historico_control.json"

# 2020-2025 entran con la carga inicial; desde 2026 entran por corte anual.
ANIO_PRIMER_CORTE = 2026

PERU = timezone(timedelta(hours=-5))
FMT = "%Y-%m-%d %H:%M:%S"

COLUMNAS_TEXTO = [
    "NIVEL_GOBIERNO", "SECTOR_NOMBRE", "PLIEGO", "PLIEGO_NOMBRE", "EJECUTORA", "EJECUTORA_NOMBRE",
    "TIPO_ACT_PROY_NOMBRE",
    "GENERICA", "GENERICA_NOMBRE",
    "FUENTE_FINANCIAMIENTO", "FUENTE_FINANCIAMIENTO_NOMBRE",
    "RUBRO", "RUBRO_NOMBRE",
    "PRODUCTO_PROYECTO", "PRODUCTO_PROYECTO_NOMBRE",
    "ACTIVIDAD_ACCION_OBRA", "ACTIVIDAD_ACCION_OBRA_NOMBRE",
    "PROGRAMA_PPTO", "PROGRAMA_PPTO_NOMBRE",
    "SEC_FUNC",
    "FINALIDAD",
    "TIPO_TRANSACCION", "SUBGENERICA", "SUBGENERICA_DET", "ESPECIFICA", "ESPECIFICA_DET", "ESPECIFICA_DET_NOMBRE",
]
COLUMNAS_ENTERAS = ["ANO_EJE", "MES_EJE"]
COLUMNAS_MONTO = [
    "MONTO_PIA", "MONTO_PIM", "MONTO_CERTIFICADO", "MONTO_COMPROMETIDO_ANUAL", "MONTO_COMPROMETIDO",
    "MONTO_DEVENGADO", "MONTO_GIRADO",
]


def avisar_workflow(procesado):
    """Informa al workflow de GitHub Actions si hubo o no procesamiento,
    para que los pasos siguientes (artefacto, commit) se salten cuando
    no hay nada nuevo. Si se corre local, no hace nada."""
    github_env = os.environ.get("GITHUB_ENV")
    if github_env:
        with open(github_env, "a") as f:
            f.write(f"PROCESADO={'true' if procesado else 'false'}\n")


def hoy_peru():
    return datetime.now(PERU).date()


def ultimo_anio_cerrado(hoy):
    """El año se da por cerrado el 1 de febrero del año siguiente."""
    return hoy.year - 1 if hoy.month >= 2 else hoy.year - 2


def leer_json(ruta):
    if not os.path.exists(ruta):
        return {}
    with open(ruta) as f:
        try:
            return json.load(f)
        except ValueError:
            return {}


def anios_a_leer(hoy, anios_en_historico):
    """Lista ordenada de años que debe leer el resumen diario."""
    anios = {hoy.year}
    if hoy.month == 1:
        anios.add(hoy.year - 1)
    for anio in range(ANIO_PRIMER_CORTE, ultimo_anio_cerrado(hoy) + 1):
        if anio not in anios_en_historico:
            anios.add(anio)
    return sorted(anios)


def leer_last_modified(url, ultima_procesada, intentos=3, espera=15):
    """HEAD con cache-busting y reintentos. Corta apenas ve algo mas nuevo que
    lo ya procesado. Devuelve (no_publicado, Last-Modified mas reciente visto)."""
    mejor = None
    no_publicado = False
    for i in range(1, intentos + 1):
        try:
            r = requests.head(
                url,
                params={"nc": int(time.time())},
                headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
                allow_redirects=True,
                timeout=30,
            )
            lm = r.headers.get("Last-Modified")
            print(
                f"Intento {i}: status={r.status_code} Last-Modified={lm} "
                f"Content-Length={r.headers.get('Content-Length')} "
                f"Age={r.headers.get('Age')} Date={r.headers.get('Date')} "
                f"Via={r.headers.get('Via')} X-Cache={r.headers.get('X-Cache')}"
            )
            if r.status_code in (403, 404):
                no_publicado = True
                break
            if lm and r.status_code == 200:
                dt = parsedate_to_datetime(lm)
                if mejor is None or dt > mejor:
                    mejor = dt
                if ultima_procesada is None or dt.astimezone(PERU) > ultima_procesada:
                    break  # ya hay algo nuevo, no hace falta insistir
        except requests.RequestException as e:
            print(f"Intento {i}: error de red -> {e}")
        if i < intentos:
            time.sleep(espera)
    return no_publicado, mejor


def cargar_base(con, url):
    """UNA sola lectura del CSV (superset de columnas que necesitan ambos analisis).
    Si un año antiguo no trae alguna columna, entra vacia y se avisa en el log."""
    existentes = {
        fila[0].upper(): fila[0]
        for fila in con.execute(
            f"DESCRIBE SELECT * FROM read_csv('{url}', header = true, all_varchar = true)"
        ).fetchall()
    }

    tipos, columnas_sql, faltantes = {}, [], []
    for grupo, tipo in (
        (COLUMNAS_TEXTO, "VARCHAR"),
        (COLUMNAS_ENTERAS, "INTEGER"),
        (COLUMNAS_MONTO, "DOUBLE"),
    ):
        for columna in grupo:
            nombre = existentes.get(columna)
            if nombre:
                tipos[nombre] = tipo
                columnas_sql.append(f'"{nombre}" AS {columna}')
            else:
                faltantes.append(columna)
                columnas_sql.append(f"CAST(NULL AS {tipo}) AS {columna}")
    if faltantes:
        print(f"AVISO: {url} no trae las columnas {faltantes}; quedan vacias.")

    tipos_sql = ", ".join(f"'{k}': '{v}'" for k, v in tipos.items())
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base AS
        SELECT * FROM (
            SELECT {", ".join(columnas_sql)}
            FROM read_csv('{url}', header = true, types = {{{tipos_sql}}})
        )
        WHERE NIVEL_GOBIERNO = 'E'
    """)


def consultar_minem(con):
    return con.execute(f"""
    WITH minem AS (
        SELECT SECTOR_NOMBRE,
               TRIM(PLIEGO) || '. ' || PLIEGO_NOMBRE AS PLIEGO,
               TRIM(EJECUTORA) || '. ' || EJECUTORA_NOMBRE AS EJECUTORA,
               TIPO_ACT_PROY_NOMBRE,
               TRIM(TIPO_TRANSACCION) || '.' || TRIM(GENERICA) || '.' || GENERICA_NOMBRE AS GENERICA,
               TRIM(FUENTE_FINANCIAMIENTO) || '. ' || FUENTE_FINANCIAMIENTO_NOMBRE AS FUENTE_FINANCIAMIENTO,
               RUBRO,
               RUBRO_NOMBRE,
               CASE WHEN TRIM(TIPO_ACT_PROY_NOMBRE) = 'PROYECTO'
                    THEN TRIM(PRODUCTO_PROYECTO) || '. ' || PRODUCTO_PROYECTO_NOMBRE
                    ELSE NULL END AS PRODUCTO_PROYECTO,
               ACTIVIDAD_ACCION_OBRA,
               ACTIVIDAD_ACCION_OBRA_NOMBRE,
               TRIM(PROGRAMA_PPTO) || '. ' || PROGRAMA_PPTO_NOMBRE AS PROGRAMA_PPTO,
               SEC_FUNC,
               CONCAT_WS('.', TIPO_TRANSACCION, GENERICA, SUBGENERICA, SUBGENERICA_DET,
                         ESPECIFICA, ESPECIFICA_DET, ESPECIFICA_DET_NOMBRE) AS CLASIFICADOR,
               ANO_EJE,
               MES_EJE,
               CASE MES_EJE
                   WHEN 1 THEN 'Ene' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Abr'
                   WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago'
                   WHEN 9 THEN 'Set' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dic'
                   ELSE 'Sin mes' END AS MES,
               MONTO_PIA, MONTO_PIM, MONTO_CERTIFICADO, MONTO_COMPROMETIDO_ANUAL, MONTO_COMPROMETIDO, MONTO_DEVENGADO
        FROM base
        WHERE SECTOR_NOMBRE = '{SECTOR_MINEM}'
    )
    SELECT 'Ranking Sectores' AS NIVEL_DETALLE, SECTOR_NOMBRE,
           CAST(NULL AS VARCHAR) AS PLIEGO, CAST(NULL AS VARCHAR) AS EJECUTORA,
           TIPO_ACT_PROY_NOMBRE, CAST(NULL AS VARCHAR) AS GENERICA,
           CAST(NULL AS VARCHAR) AS FUENTE_FINANCIAMIENTO,
           CAST(NULL AS VARCHAR) AS RUBRO,
           CAST(NULL AS VARCHAR) AS RUBRO_NOMBRE,
           CAST(NULL AS VARCHAR) AS PRODUCTO_PROYECTO,
           CAST(NULL AS VARCHAR) AS ACTIVIDAD_ACCION_OBRA,
           CAST(NULL AS VARCHAR) AS ACTIVIDAD_ACCION_OBRA_NOMBRE,
           CAST(NULL AS VARCHAR) AS PROGRAMA_PPTO,
           CAST(NULL AS VARCHAR) AS SEC_FUNC, CAST(NULL AS VARCHAR) AS CLASIFICADOR,
           ANO_EJE, CAST(NULL AS INTEGER) AS MES_EJE, CAST(NULL AS VARCHAR) AS MES,
           SUM(MONTO_PIA) AS pia, SUM(MONTO_PIM) AS pim, SUM(MONTO_CERTIFICADO) AS certificado,
           SUM(MONTO_COMPROMETIDO_ANUAL) AS comp_anual, SUM(MONTO_COMPROMETIDO) AS comprometido,
           SUM(MONTO_DEVENGADO) AS devengado
    FROM base
    WHERE SECTOR_NOMBRE <> '{SECTOR_MINEM}'
    GROUP BY SECTOR_NOMBRE, TIPO_ACT_PROY_NOMBRE, ANO_EJE
    UNION ALL
    SELECT 'Detalle MINEM' AS NIVEL_DETALLE, SECTOR_NOMBRE, PLIEGO, EJECUTORA,
           TIPO_ACT_PROY_NOMBRE, GENERICA, FUENTE_FINANCIAMIENTO, RUBRO, RUBRO_NOMBRE,
           PRODUCTO_PROYECTO,
           ACTIVIDAD_ACCION_OBRA, ACTIVIDAD_ACCION_OBRA_NOMBRE,
           PROGRAMA_PPTO,
           SEC_FUNC, CLASIFICADOR,
           ANO_EJE, MES_EJE, MES,
           SUM(MONTO_PIA) AS pia, SUM(MONTO_PIM) AS pim, SUM(MONTO_CERTIFICADO) AS certificado,
           SUM(MONTO_COMPROMETIDO_ANUAL) AS comp_anual, SUM(MONTO_COMPROMETIDO) AS comprometido,
           SUM(MONTO_DEVENGADO) AS devengado
    FROM minem
    GROUP BY SECTOR_NOMBRE, PLIEGO, EJECUTORA, TIPO_ACT_PROY_NOMBRE, GENERICA,
             FUENTE_FINANCIAMIENTO, RUBRO, RUBRO_NOMBRE, PRODUCTO_PROYECTO,
             ACTIVIDAD_ACCION_OBRA, ACTIVIDAD_ACCION_OBRA_NOMBRE, PROGRAMA_PPTO,
             SEC_FUNC, CLASIFICADOR, ANO_EJE, MES_EJE, MES
    ORDER BY NIVEL_DETALLE, SECTOR_NOMBRE, pim DESC
""").df()


def consultar_ecofin(con):
    return con.execute(f"""
    WITH onp AS (
        SELECT
            TRIM(PLIEGO) || '. ' || PLIEGO_NOMBRE AS PLIEGO_D,
            TRIM(EJECUTORA) || '. ' || EJECUTORA_NOMBRE AS EJECUTORA_D,
            TIPO_ACT_PROY_NOMBRE,
            TRIM(TIPO_TRANSACCION) || '.' || TRIM(GENERICA) || '.' || GENERICA_NOMBRE AS GENERICA_D,
            TRIM(FUENTE_FINANCIAMIENTO) || '. ' || FUENTE_FINANCIAMIENTO_NOMBRE AS FUENTE_FINANCIAMIENTO_D,
            RUBRO AS RUBRO_D,
            RUBRO_NOMBRE AS RUBRO_NOMBRE_D,
            FINALIDAD AS FINALIDAD_D,
            CASE WHEN TRIM(TIPO_ACT_PROY_NOMBRE) = 'PROYECTO'
                 THEN TRIM(PRODUCTO_PROYECTO) || '. ' || PRODUCTO_PROYECTO_NOMBRE
                 ELSE NULL END AS PRODUCTO_PROYECTO_D,
            ACTIVIDAD_ACCION_OBRA AS ACTIVIDAD_ACCION_OBRA_D,
            ACTIVIDAD_ACCION_OBRA_NOMBRE AS ACTIVIDAD_ACCION_OBRA_NOMBRE_D,
            TRIM(PROGRAMA_PPTO) || '. ' || PROGRAMA_PPTO_NOMBRE AS PROGRAMA_PPTO_D,
            SEC_FUNC,
            CONCAT_WS('.', TIPO_TRANSACCION, GENERICA, SUBGENERICA, SUBGENERICA_DET,
                      ESPECIFICA, ESPECIFICA_DET, ESPECIFICA_DET_NOMBRE) AS CLASIFICADOR,
            ANO_EJE,
            MES_EJE,
            CASE MES_EJE
                WHEN 1 THEN 'Ene' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Abr'
                WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago'
                WHEN 9 THEN 'Set' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dic'
                ELSE 'Sin mes' END AS MES,
            CASE WHEN MES_EJE BETWEEN 1 AND 12
                 THEN MAKE_DATE(ANO_EJE, MES_EJE, 1)
                 ELSE NULL END AS FECHA,
            MONTO_PIA, MONTO_PIM, MONTO_CERTIFICADO, MONTO_COMPROMETIDO_ANUAL, MONTO_COMPROMETIDO,
            MONTO_DEVENGADO, MONTO_GIRADO
        FROM base
        WHERE SECTOR_NOMBRE = '{SECTOR_ECOFIN}' AND PLIEGO_NOMBRE ILIKE '{FILTRO_ONP}'
    )
    SELECT 'Ranking Sectores' AS NIVEL_DETALLE,
           SECTOR_NOMBRE,
           CAST(NULL AS VARCHAR) AS PLIEGO, CAST(NULL AS VARCHAR) AS EJECUTORA,
           CAST(NULL AS VARCHAR) AS TIPO_ACT_PROY_NOMBRE,
           CAST(NULL AS VARCHAR) AS GENERICA, CAST(NULL AS VARCHAR) AS FUENTE_FINANCIAMIENTO,
           CAST(NULL AS VARCHAR) AS RUBRO,
           CAST(NULL AS VARCHAR) AS RUBRO_NOMBRE,
           CAST(NULL AS VARCHAR) AS FINALIDAD,
           CAST(NULL AS VARCHAR) AS PRODUCTO_PROYECTO,
           CAST(NULL AS VARCHAR) AS ACTIVIDAD_ACCION_OBRA,
           CAST(NULL AS VARCHAR) AS ACTIVIDAD_ACCION_OBRA_NOMBRE,
           CAST(NULL AS VARCHAR) AS PROGRAMA_PPTO,
           CAST(NULL AS VARCHAR) AS SEC_FUNC, CAST(NULL AS VARCHAR) AS CLASIFICADOR,
           ANO_EJE, CAST(NULL AS INTEGER) AS MES_EJE,
           CAST(NULL AS VARCHAR) AS MES, CAST(NULL AS DATE) AS FECHA,
           SUM(MONTO_PIA) AS pia, SUM(MONTO_PIM) AS pim, SUM(MONTO_CERTIFICADO) AS certificado,
           SUM(MONTO_COMPROMETIDO_ANUAL) AS comp_anual, SUM(MONTO_COMPROMETIDO) AS comprometido,
           SUM(MONTO_DEVENGADO) AS devengado, SUM(MONTO_GIRADO) AS girado
    FROM base
    WHERE SECTOR_NOMBRE <> '{SECTOR_ECOFIN}'
    GROUP BY SECTOR_NOMBRE, ANO_EJE

    UNION ALL

    SELECT 'Detalle Pliegos ECOFIN' AS NIVEL_DETALLE,
           SECTOR_NOMBRE,
           TRIM(PLIEGO) || '. ' || PLIEGO_NOMBRE,
           TRIM(EJECUTORA) || '. ' || EJECUTORA_NOMBRE,
           CAST(NULL AS VARCHAR),
           CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),
           CAST(NULL AS VARCHAR),
           CAST(NULL AS VARCHAR),
           CAST(NULL AS VARCHAR),
           CAST(NULL AS VARCHAR),
           CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),
           CAST(NULL AS VARCHAR),
           CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR),
           ANO_EJE, CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR), CAST(NULL AS DATE),
           SUM(MONTO_PIA), SUM(MONTO_PIM), SUM(MONTO_CERTIFICADO),
           SUM(MONTO_COMPROMETIDO_ANUAL), SUM(MONTO_COMPROMETIDO),
           SUM(MONTO_DEVENGADO), SUM(MONTO_GIRADO)
    FROM base
    WHERE SECTOR_NOMBRE = '{SECTOR_ECOFIN}' AND PLIEGO_NOMBRE NOT ILIKE '{FILTRO_ONP}'
    GROUP BY SECTOR_NOMBRE, ANO_EJE, PLIEGO, PLIEGO_NOMBRE, EJECUTORA, EJECUTORA_NOMBRE

    UNION ALL

    SELECT 'Detalle ONP' AS NIVEL_DETALLE,
           '{SECTOR_ECOFIN}',
           PLIEGO_D, EJECUTORA_D,
           TIPO_ACT_PROY_NOMBRE,
           GENERICA_D, FUENTE_FINANCIAMIENTO_D,
           RUBRO_D,
           RUBRO_NOMBRE_D,
           FINALIDAD_D,
           PRODUCTO_PROYECTO_D,
           ACTIVIDAD_ACCION_OBRA_D, ACTIVIDAD_ACCION_OBRA_NOMBRE_D,
           PROGRAMA_PPTO_D,
           SEC_FUNC, CLASIFICADOR,
           ANO_EJE, MES_EJE, MES, FECHA,
           SUM(MONTO_PIA), SUM(MONTO_PIM), SUM(MONTO_CERTIFICADO),
           SUM(MONTO_COMPROMETIDO_ANUAL), SUM(MONTO_COMPROMETIDO),
           SUM(MONTO_DEVENGADO), SUM(MONTO_GIRADO)
    FROM onp
    GROUP BY PLIEGO_D, EJECUTORA_D, TIPO_ACT_PROY_NOMBRE, GENERICA_D, FUENTE_FINANCIAMIENTO_D, RUBRO_D,
             RUBRO_NOMBRE_D, FINALIDAD_D,
             PRODUCTO_PROYECTO_D, ACTIVIDAD_ACCION_OBRA_D, ACTIVIDAD_ACCION_OBRA_NOMBRE_D,
             PROGRAMA_PPTO_D, SEC_FUNC, CLASIFICADOR, ANO_EJE, MES_EJE, MES, FECHA
""").df()


def procesar_archivo(url):
    """Devuelve (minem, ecofin) de un archivo anual. Cada archivo se procesa
    con su propia conexion para liberar memoria antes de leer el siguiente."""
    con = duckdb.connect()
    try:
        cargar_base(con, url)
        return consultar_minem(con), consultar_ecofin(con)
    finally:
        con.close()


def main():
    hoy = hoy_peru()
    anios_en_historico = {int(a) for a in leer_json(CONTROL_HISTORICO)}
    anios = anios_a_leer(hoy, anios_en_historico)
    procesados = leer_json(MARCADOR_ANIOS)
    print(f"Fecha Peru: {hoy} | Años a leer: {anios} | En historico: {sorted(anios_en_historico)}")

    # 1. Version disponible de cada año
    versiones = {}
    hay_nuevo = False
    for anio in anios:
        ultima = procesados.get(str(anio))
        ultima_dt = datetime.strptime(ultima, FMT).replace(tzinfo=PERU) if ultima else None
        no_publicado, fecha_gmt = leer_last_modified(URL_PLANTILLA.format(anio=anio), ultima_dt)

        if no_publicado and hoy.month == 1 and anio == hoy.year:
            print(f"{anio}: el MEF aun no lo publica. Se continua sin ese año.")
            continue
        if fecha_gmt is None:
            print(f"{anio}: no se pudo leer Last-Modified. No se reprocesa.")
            avisar_workflow(False)
            sys.exit(0)

        fecha_peru = fecha_gmt.astimezone(PERU)
        print(f"{anio}: Last-Modified {fecha_peru.strftime(FMT)} | procesado {ultima or '(ninguno)'}")
        # Si el servidor devuelve una version anterior (cache desfasada), no se
        # considera nueva ni se retrocede el marcador.
        if ultima_dt is None or fecha_peru > ultima_dt:
            hay_nuevo = True
            versiones[anio] = fecha_peru.strftime(FMT)
        else:
            versiones[anio] = ultima

    cambio_anios = {str(a) for a in versiones} != set(procesados)
    if not hay_nuevo and not cambio_anios:
        print("Sin datos nuevos ni cambio de años. No se reprocesa.")
        avisar_workflow(False)
        sys.exit(0)

    print(f"Procesando años {sorted(versiones)}...")

    # 2. Un archivo a la vez
    minem, ecofin = [], []
    for anio in sorted(versiones):
        print(f"-> {anio}")
        res_minem, res_ecofin = procesar_archivo(URL_PLANTILLA.format(anio=anio))
        minem.append(res_minem)
        ecofin.append(res_ecofin)

    resultado_minem = pd.concat(minem, ignore_index=True)
    resultado_ecofin = pd.concat(ecofin, ignore_index=True)

    # La fecha vigente es la del año mas reciente que exista.
    fecha_actualizacion = versiones[max(versiones)]

    resultado_minem["FECHA_ACTUALIZACION_ARCHIVO"] = fecha_actualizacion
    resultado_minem.to_csv("energia_minas_resumen.csv", index=False)
    resultado_ecofin["FECHA_ACTUALIZACION_ARCHIVO"] = fecha_actualizacion
    resultado_ecofin.to_csv("economia_finanzas_resumen.csv", index=False)

    # 3. Dejar constancia de lo procesado para la proxima corrida.
    os.makedirs("output", exist_ok=True)
    with open(MARCADOR, "w") as f:
        f.write(fecha_actualizacion)
    with open(MARCADOR_ANIOS, "w") as f:
        json.dump({str(a): versiones[a] for a in sorted(versiones)}, f, indent=2)

    avisar_workflow(True)

    # Validaciones rapidas
    print("=== MINEM ===")
    print(resultado_minem.groupby(["NIVEL_DETALLE", "ANO_EJE"]).size())
    print("=== ECOFIN ===")
    print(resultado_ecofin.groupby(["NIVEL_DETALLE", "ANO_EJE"]).size())
    print("Fecha de actualizacion del archivo origen (hora Peru):", fecha_actualizacion)


if __name__ == "__main__":
    main()
