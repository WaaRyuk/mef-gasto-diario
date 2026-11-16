"""
Datos de inversiones (Banco de Inversiones / Invierte.pe) desde Datos Abiertos
MEF, filtrados a los CUIs activos del MINEM en el año en curso.

Lista de CUIs: output/energia_minas_resumen.csv (lo genera
scripts/resumen_combinado.py). Filas PROYECTO del pliego 016 con PIM o
devengado > 0 en el año.

Archivos que se leen (https://fs.datosabiertos.mef.gob.pe/datastorefiles/):
  DETALLE_INVERSIONES.csv  -> output/inv_detalle.csv
      estado, situacion, viable, costo actualizado, control concurrente,
      laudos/fianzas, OPMI/UF/UEI, PMI, ET/F8/F9/cierre, avance, ubicacion
  FORMATO_12B.csv          -> output/inv_formato12b.csv
      avance fisico y de ejecucion, ultimo estado situacional, problematica,
      proximos pasos, programacion inicial y actualizada mensual del año
  ESTADO_SITUACIONAL.csv   -> output/inv_estado_situacional.csv  (historial)
  INVERSIONES_ET.csv       -> output/inv_expediente_tecnico.csv
  PROCESO_SELECCION.csv    -> output/inv_componentes.csv
      componentes: producto, meta, costo, valorizacion acumulada, avance
  INVERSIONES_BRECHAS.csv  -> output/inv_brechas.csv
  CIERRE_INVERSIONES.csv   -> output/inv_cierre.csv

Se guardan las columnas tal como vienen del MEF (texto); los tipos se asignan
en Power Query. Solo se reprocesa un archivo si su Last-Modified cambio o si
cambio la lista de CUIs (control en output/inv_control.json).

Variables de entorno opcionales:
  INV_PLIEGOS   pliegos a incluir, separados por coma (default "016").
                "TODOS" toma todo el sector (incluye IPEN e INGEMMET).
  INV_FORZAR    "1" para reprocesar todo aunque no haya cambios.
"""
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import duckdb
import pandas as pd
import requests

BASE_URL = "https://fs.datosabiertos.mef.gob.pe/datastorefiles/"
ARCHIVOS = {
    "detalle": "DETALLE_INVERSIONES.csv",
    "formato12b": "FORMATO_12B.csv",
    "estado_situacional": "ESTADO_SITUACIONAL.csv",
    "expediente_tecnico": "INVERSIONES_ET.csv",
    "componentes": "PROCESO_SELECCION.csv",
    "brechas": "INVERSIONES_BRECHAS.csv",
    "cierre": "CIERRE_INVERSIONES.csv",
}

ENTRADA = "output/energia_minas_resumen.csv"
SALIDA = "output/inv_{}.csv"
CONTROL = "output/inv_control.json"

PLIEGOS = [p.strip() for p in os.getenv("INV_PLIEGOS", "016").split(",") if p.strip()]
FORZAR = os.getenv("INV_FORZAR") == "1"

PERU = timezone(timedelta(hours=-5))
FMT = "%Y-%m-%d %H:%M:%S"


def avisar_workflow(procesado):
    """Deja PROCESADO=true/false para que el workflow decida si hace commit."""
    destino = os.getenv("GITHUB_ENV")
    if destino:
        with open(destino, "a") as f:
            f.write(f"PROCESADO={'true' if procesado else 'false'}\n")


def cuis_activos(anio):
    df = pd.read_csv(ENTRADA, dtype=str)
    df = df[(df["NIVEL_DETALLE"] == "Detalle MINEM") & (df["TIPO_ACT_PROY_NOMBRE"] == "PROYECTO")]
    df = df[df["ANO_EJE"] == str(anio)]
    if PLIEGOS != ["TODOS"]:
        df = df[df["PLIEGO"].str[:3].isin(PLIEGOS)]
    df = df.assign(
        CUI=df["PRODUCTO_PROYECTO"].str.extract(r"^(\d{7})", expand=False),
        pim=pd.to_numeric(df["pim"], errors="coerce").fillna(0),
        devengado=pd.to_numeric(df["devengado"], errors="coerce").fillna(0),
    ).dropna(subset=["CUI"])
    agg = df.groupby("CUI")[["pim", "devengado"]].sum()
    return sorted(agg[(agg["pim"] > 0) | (agg["devengado"] > 0)].index)


def last_modified(url, intentos=3, espera=15):
    """Last-Modified del archivo en hora Peru, o None si no se pudo leer."""
    for i in range(1, intentos + 1):
        try:
            r = requests.head(url, params={"nc": int(time.time())},
                              headers={"Cache-Control": "no-cache"}, allow_redirects=True, timeout=30)
            lm = r.headers.get("Last-Modified")
            print(f"  HEAD intento {i}: status={r.status_code} Last-Modified={lm} "
                  f"Content-Length={r.headers.get('Content-Length')}")
            if r.status_code == 200 and lm:
                return parsedate_to_datetime(lm).astimezone(PERU)
        except requests.RequestException as e:
            print(f"  HEAD intento {i}: error de red -> {e}")
        if i < intentos:
            time.sleep(espera)
    return None


def descargar(url, destino, intentos=3):
    """Descarga el CSV completo a disco una sola vez (los archivos pesan cientos de MB)."""
    for i in range(1, intentos + 1):
        try:
            with requests.get(url, params={"nc": int(time.time())}, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                with open(destino, "wb") as f:
                    for bloque in r.iter_content(chunk_size=1 << 20):
                        f.write(bloque)
            print(f"  descargado {os.path.getsize(destino) / 1e6:.1f} MB")
            return
        except requests.RequestException as e:
            print(f"  descarga intento {i}: {e}")
            if i == intentos:
                raise
            time.sleep(30)


def leer_filtrado(origen, cuis):
    """Lee el CSV (URL o ruta) como texto y devuelve solo las filas de los CUIs.
    Prueba UTF-8 y luego Latin-1; si hay filas mal formadas, las tolera."""
    lista_sql = ", ".join(f"'{c}'" for c in cuis)
    opciones = [
        "header = true, all_varchar = true",
        "header = true, all_varchar = true, encoding = 'latin-1'",
        "header = true, all_varchar = true, strict_mode = false, null_padding = true, quote = '"', escape = '"'",
        "header = true, all_varchar = true, encoding = 'latin-1', strict_mode = false, null_padding = true, quote = '"', escape = '"'",
    ]
    ultimo_error = None
    for opc in opciones:
        con = duckdb.connect()
        try:
            cols = [f[0] for f in con.execute(
                f"DESCRIBE SELECT * FROM read_csv('{origen}', {opc})").fetchall()]
            col_cui = next((c for c in cols if c.strip().upper() == "CODIGO_UNICO"), None)
            if col_cui is None:
                raise ValueError(f"no tiene columna CODIGO_UNICO (columnas: {cols[:10]}...)")
            df = con.execute(f"""
                SELECT * FROM read_csv('{origen}', {opc})
                WHERE regexp_replace(trim("{col_cui}"), '\\.0+$', '') IN ({lista_sql})
            """).df()
            if opc != opciones[0]:
                print(f"  leido con opciones: {opc}")
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception as e:  # noqa: BLE001 - se prueba la siguiente opcion
            ultimo_error = e
        finally:
            con.close()
    raise RuntimeError(f"No se pudo leer {origen}: {ultimo_error}")


def leer_json(ruta):
    try:
        with open(ruta) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def main():
    ahora = datetime.now(PERU)
    cuis = cuis_activos(ahora.year)
    if not cuis:
        sys.exit(f"Sin CUIs para filtrar; revisa {ENTRADA}")
    huella_cuis = hashlib.md5(",".join(cuis).encode()).hexdigest()
    print(f"{ahora.strftime(FMT)} | {len(cuis)} CUIs activos {ahora.year} (pliegos: {','.join(PLIEGOS)})")

    control = leer_json(CONTROL)
    cambio_cuis = control.get("huella_cuis") != huella_cuis
    archivos_ctrl = control.get("archivos", {})
    procesados, errores = [], []

    for nombre, archivo in ARCHIVOS.items():
        url = BASE_URL + archivo
        salida = SALIDA.format(nombre)
        print(f"-> {archivo}")
        lm = last_modified(url)
        lm_txt = lm.strftime(FMT) if lm else None
        previo = archivos_ctrl.get(nombre)
        sin_cambios = (lm_txt is not None and lm_txt == previo and not cambio_cuis
                       and not FORZAR and os.path.exists(salida))
        if sin_cambios:
            print(f"  sin cambios desde {previo}; se omite")
            continue
        try:
            with tempfile.TemporaryDirectory() as tmp:
                local = os.path.join(tmp, archivo)
                descargar(url, local)
                df = leer_filtrado(local, cuis)
        except Exception as e:  # noqa: BLE001 - un archivo caido no frena a los demas
            errores.append(archivo)
            print(f"  ERROR {e}")
            continue
        df["FECHA_ACTUALIZACION_ARCHIVO"] = lm_txt or ""
        os.makedirs("output", exist_ok=True)
        df.to_csv(salida, index=False, encoding="utf-8-sig")
        cui_col = next(c for c in df.columns if c.upper() == "CODIGO_UNICO")
        print(f"  {len(df)} filas, {df[cui_col].nunique()} CUIs -> {salida}")
        archivos_ctrl[nombre] = lm_txt or ahora.strftime(FMT)
        procesados.append(nombre)

    if procesados:
        with open(CONTROL, "w") as f:
            json.dump({"huella_cuis": huella_cuis, "n_cuis": len(cuis),
                       "archivos": archivos_ctrl}, f, indent=2)
    avisar_workflow(bool(procesados))
    print(f"Procesados: {procesados or 'ninguno'} | Con error: {errores or 'ninguno'}")
    if len(errores) == len(ARCHIVOS):
        sys.exit(1)


if __name__ == "__main__":
    main()
