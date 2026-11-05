"""
Historico del gasto MEF: años cerrados desde 2020, con los mismos niveles y
columnas que el resumen diario, para poder anexarlos en Power BI.

- Primera corrida: carga 2020-2025, un año a la vez.
- Cada 1 de febrero: agrega el año que acaba de cerrar (2026 el 1 feb 2027).

Procesa solo lo que falta segun output/historico_control.json, asi que se puede
relanzar sin duplicar filas.

Uso:
    python scripts/actualizar_historico.py --pendientes   # lista lo que falta
    python scripts/actualizar_historico.py --anio 2020    # procesa un año
"""
import argparse
import json
import os
import sys
from email.utils import parsedate_to_datetime

import requests

from resumen_combinado import (
    CONTROL_HISTORICO,
    FMT,
    PERU,
    hoy_peru,
    leer_json,
    procesar_archivo,
    ultimo_anio_cerrado,
)

ANIO_INICIAL = 2020

SALIDA_MINEM = "output/historico_energia_minas.csv"
SALIDA_ECOFIN = "output/historico_economia_finanzas.csv"

# El nombre del archivo del MEF no es igual en todos los años.
PATRONES_URL = [
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto-Diario.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto-Mensual.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/{anio}-Gasto.csv",
]


def anios_pendientes():
    control = leer_json(CONTROL_HISTORICO)
    return [
        anio
        for anio in range(ANIO_INICIAL, ultimo_anio_cerrado(hoy_peru()) + 1)
        if str(anio) not in control
    ]


def resolver_archivo(anio):
    """Devuelve (url, Last-Modified en hora Peru) del archivo de ese año."""
    for patron in PATRONES_URL:
        url = patron.format(anio=anio)
        try:
            r = requests.head(url, allow_redirects=True, timeout=30)
        except requests.RequestException as e:
            print(f"{url}: error de red -> {e}")
            continue
        print(f"{url}: status={r.status_code} Last-Modified={r.headers.get('Last-Modified')}")
        if r.status_code == 200:
            lm = r.headers.get("Last-Modified")
            fecha = parsedate_to_datetime(lm).astimezone(PERU).strftime(FMT) if lm else ""
            return url, fecha
    raise FileNotFoundError(f"No se encontro archivo del MEF para {anio}.")


def validar_columnas(df, ruta):
    """El CSV existente debe tener las mismas columnas que las filas a anexar."""
    if os.path.exists(ruta):
        with open(ruta, encoding="utf-8") as f:
            encabezado = f.readline().rstrip("\r\n")
        if encabezado != ",".join(df.columns):
            raise ValueError(f"Las columnas no coinciden con {ruta}.")


def anexar(df, ruta):
    existe = os.path.exists(ruta)
    df.to_csv(ruta, mode="a" if existe else "w", header=not existe, index=False)


def procesar_anio(anio):
    control = leer_json(CONTROL_HISTORICO)
    if str(anio) in control:
        print(f"{anio} ya esta en el historico. No se hace nada.")
        return
    if anio > ultimo_anio_cerrado(hoy_peru()):
        raise ValueError(f"{anio} aun no cierra; entra el 1 de febrero del año siguiente.")

    url, fecha = resolver_archivo(anio)
    minem, ecofin = procesar_archivo(url)

    # Se valida antes de escribir para no dejar un año a medias.
    for nombre, df, nivel in (
        ("MINEM", minem, "Detalle MINEM"),
        ("ECOFIN", ecofin, "Detalle ONP"),
    ):
        filas = (df["NIVEL_DETALLE"] == nivel).sum()
        if filas == 0:
            raise ValueError(f"{anio}: {nombre} no trae filas de '{nivel}'.")
        print(f"{anio} {nombre}:\n{df['NIVEL_DETALLE'].value_counts().to_string()}")

    minem["FECHA_ACTUALIZACION_ARCHIVO"] = fecha
    ecofin["FECHA_ACTUALIZACION_ARCHIVO"] = fecha

    validar_columnas(minem, SALIDA_MINEM)
    validar_columnas(ecofin, SALIDA_ECOFIN)

    os.makedirs("output", exist_ok=True)
    anexar(minem, SALIDA_MINEM)
    anexar(ecofin, SALIDA_ECOFIN)

    control[str(anio)] = fecha
    with open(CONTROL_HISTORICO, "w") as f:
        json.dump(dict(sorted(control.items())), f, indent=2)
    print(f"{anio} agregado al historico (version del archivo: {fecha}).")


def main():
    parser = argparse.ArgumentParser(description="Actualiza el historico del gasto MEF.")
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--pendientes", action="store_true", help="lista los años que faltan")
    grupo.add_argument("--anio", type=int, help="procesa un año cerrado")
    args = parser.parse_args()

    if args.pendientes:
        print(" ".join(str(a) for a in anios_pendientes()))
        return

    try:
        procesar_anio(args.anio)
    except Exception as e:
        print(f"ERROR en {args.anio}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
