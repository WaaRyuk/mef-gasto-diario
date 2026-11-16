"""
Datos del SSI (Sistema de Seguimiento de Inversiones, MEF) que NO estan en
Datos Abiertos MEF, para las inversiones activas del año en curso del MINEM.

Lo financiero (PIA, PIM, certificado, devengado) NO se trae: ya sale del Gasto
Diario. Aqui solo va lo que viene de Invierte.pe, SEACE, InfObras y Contraloria.

Lista de CUIs: output/energia_minas_resumen.csv (lo genera
scripts/resumen_combinado.py). Filas PROYECTO del pliego 016 con PIM o
devengado > 0 en el año.

Servicios consultados (los mismos que usa https://ofi5.mef.gob.pe/ssi/).
No son una API oficial: pueden cambiar sin aviso.

Salidas (CUI es la llave en todas):
  output/ssi_ficha.csv             1 fila por CUI: estado, situacion, viabilidad,
                                   costo actualizado, avance fisico, ET/F8/F9/F12B,
                                   liquidacion, cierre, UEI/UF, modalidad, cartera
  output/ssi_brechas.csv           indicador de brecha y valor de cierre (F9)
  output/ssi_contratos_seace.csv   contratos SEACE vinculados al CUI
  output/ssi_infobras.csv          obras registradas en InfObras
  output/ssi_informes_control.csv  informes de Contraloria (OCI) con URL
  output/ssi_alertas.csv           alertas de riesgo, perdida de viabilidad y
                                   paralizacion
  output/ssi_oxi.csv               convenios de Obras por Impuestos

Variables de entorno opcionales:
  SSI_PLIEGOS   pliegos a incluir, separados por coma (default "016").
                "TODOS" toma todo el sector (incluye IPEN e INGEMMET).
  SSI_PAUSA     segundos entre consultas (default 0.3).
"""
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

BASE = "https://ofi5.mef.gob.pe"
ENTRADA = "output/energia_minas_resumen.csv"
SALIDA = "output/ssi_{}.csv"

PLIEGOS = [p.strip() for p in os.getenv("SSI_PLIEGOS", "016").split(",") if p.strip()]
PAUSA = float(os.getenv("SSI_PAUSA", "0.3"))
MAX_PROP_FALLOS = 0.3  # si fallan mas CUIs que esto, no se sobrescriben las salidas

PERU = timezone(timedelta(hours=-5))

# Campos de la ficha (traeDetInvSSI) que se guardan. Lista fija para que el
# esquema no cambie entre corridas. Los montos SIAF se excluyen a proposito.
CAMPOS_FICHA = [
    # Identificacion
    "CODIGO_UNICO", "COD_SNIP", "NOMBRE_INVERSION", "TIPO_FORMATO", "COD_TIPO_INVERSION",
    "MARCO", "NIVEL", "SECTOR", "ENTIDAD", "FUNCION", "DES_PROGRAMA", "DES_SUB_PROGRAMA",
    # Responsables
    "NOMBRE_OPMI", "COD_UNIDAD_UEI", "DES_UNIDAD_UEI", "COD_UNIDAD_UF", "DES_UNIDAD_UF",
    # Estado
    "ESTADO", "SITUACION", "ULT_ESTADO_SITUACIONAL", "MODAL_EJEC", "BENEFICIARIO",
    "IND_REG_PMI", "ES_CARTERA_PRIORIZADA", "IND_CARTERA_PRIOR", "ACUERDOS", "DES_ACUERDOS",
    "DES_CONVOCATORIA", "IND_REG_FONIPREL",
    # Montos Invierte.pe (no SIAF)
    "MTO_VIABLE", "MTO_TOT_VIAB", "MTO_F8_C", "COSTO_ACTUALIZADO",
    "MTO_CCONCURR_FYE", "MTO_CCONCURR_EJE",
    # Avance fisico y F12B
    "AVAN_FISICO", "PORC_AVANCE_FIS", "PORC_AVANCE_EJEC",
    "TIENE_F12B", "ULT_PERIODO_REG_F12B", "MONTO_PROG_ANO_MES_F12", "MONTO_ACTU_ANO_MES_F12",
    "TOTAL_MONTO_ACT_F12",
    # Hitos del ciclo de inversion
    "ET_REGISTRADO", "TIENE_F8", "CIERRE_REGISTRADO",
    "FEC_REGISTRO", "FEC_VIABLE", "FEC_REG_ET", "FEC_APROB_ET", "FEC_REG_F8",
    "FEC_INI_EJ", "FEC_FIN_EJ", "FEC_INI_EJEC", "FEC_FIN_EJEC", "FEC_ULT_MOD_F8",
    "FEC_REG_F12B", "FEC_UL_MOD_F12B", "FECHA_ULT_ACT_F12B", "FEC_UL_SIT",
    "FEC_REG_F9", "FEC_PRE_LIQ", "FEC_LIQUID", "FEC_DECLARAC", "FEC_CIERRE",
]

# Listas por CUI: nombre de salida -> [(origen, url, datos)]. "{cu}" se reemplaza por el CUI.
LISTAS = {
    "brechas": [("BRECHA", "/invierteWS/Ssi/traeCierreBrecha", {"id": "{cu}", "tipo": "BRE"})],
    "contratos_seace": [("SEACE", "/invierteWS/Ssi/traeContratoSeaceDWH",
                         {"id": "{cu}", "codsnip": "{snip}", "vers": "v2"})],
    "infobras": [("INFOBRAS", "/ssi/Ssi/traeDatosObraDWH", {"id": "{cu}"})],
    "informes_control": [("CONTRALORIA", "/ssi/Ssi/traeInfControlDWH", {"id": "{cu}"})],
    "alertas": [
        ("RIESGO_3", "/invierteWS/Ssi/traeAlertaRiesgoSSI", {"id": "{cu}", "tip": "3"}),
        ("RIESGO_4", "/invierteWS/Ssi/traeAlertaRiesgoSSI", {"id": "{cu}", "tip": "4"}),
        ("RIESGO_DWH_1", "/invierteWS/Ssi/traeAlertaRiesgoSSIDWH", {"id": "{cu}", "tip": "1"}),
        ("PERDIDA_VIABILIDAD", "/invierteWS/Ssi/traeAlertaPerdViab", {"id": "{cu}", "tipo": "{tipinv}"}),
        ("PARALIZACION", "/invierte/paraliza/traeListaParalizaPublico", {"id": "{cu}"}),
    ],
    "oxi": [("OXI", "/invierteWS/Ssi/traeListaConvOXI", {"id": "{cu}"})],
}

RE_NET = re.compile(r"^/Date\((-?\d+)\)/$")
RE_DMY = re.compile(r"^(\d{2})/(\d{2})/(\d{2}|\d{4})(?: \d{2}:\d{2}:\d{2})?$")


def cuis_activos(anio):
    """CUIs de proyectos con PIM o devengado > 0 en el año, segun el resumen diario."""
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
    agg = (df.groupby("CUI")
             .agg(EJECUTORA=("EJECUTORA", lambda s: " | ".join(sorted(s.unique()))),
                  pim=("pim", "sum"), devengado=("devengado", "sum"))
             .reset_index())
    return agg[(agg["pim"] > 0) | (agg["devengado"] > 0)][["CUI", "EJECUTORA"]]


def nueva_sesion():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; mef-gasto-diario/1.0)",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": BASE + "/ssi/",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    })
    return s


def consultar(sesion, ruta, datos, intentos=3):
    for i in range(intentos):
        try:
            r = sesion.post(BASE + ruta, data=datos, timeout=60)
            r.raise_for_status()
            res = r.json()
            time.sleep(PAUSA)
            return res if isinstance(res, list) else []
        except (requests.RequestException, ValueError) as e:
            if i == intentos - 1:
                raise
            print(f"  reintento {i + 1} {ruta} {datos.get('id')}: {e}")
            time.sleep(5 * (i + 1))
    return []


def normalizar_fecha(valor):
    """'/Date(ms)/', 'dd/mm/aa' o 'dd/mm/aaaa [hh:mm:ss]' -> 'YYYY-MM-DD'. Lo demas, igual."""
    if not isinstance(valor, str):
        return valor
    v = valor.strip()
    m = RE_NET.match(v)
    if m:
        return datetime.fromtimestamp(int(m.group(1)) / 1000, PERU).strftime("%Y-%m-%d")
    m = RE_DMY.match(v)
    if m:
        d, mes, a = m.groups()
        a = ("20" + a) if len(a) == 2 else a
        return f"{a}-{mes}-{d}"
    return valor


def limpiar(fila):
    return {k: normalizar_fecha(v) for k, v in fila.items() if not isinstance(v, (list, dict))}


def procesar_cui(sesion, cui):
    """Devuelve (ficha, {lista: [filas]}). ficha es None si el CUI no esta en el SSI."""
    res = consultar(sesion, "/invierteWS/Ssi/traeDetInvSSI", {"id": cui, "tipo": "SIAF"})
    f = res[0] if res else {}
    if not f.get("NOMBRE_INVERSION"):
        return None, {}

    ficha = {c: normalizar_fecha(f.get(c)) for c in CAMPOS_FICHA}
    ficha["AVAN_FISICO"] = pd.to_numeric(ficha["AVAN_FISICO"], errors="coerce")

    claves = {"cu": cui, "snip": str(f.get("COD_SNIP") or cui),
              "tipinv": str(f.get("COD_TIPO_INVERSION") or 1)}
    listas = {}
    for nombre, consultas in LISTAS.items():
        filas = []
        for origen, ruta, plantilla in consultas:
            datos = {k: v.format(**claves) for k, v in plantilla.items()}
            for fila in consultar(sesion, ruta, datos):
                filas.append({"CUI": cui, "ORIGEN": origen, **limpiar(fila)})
        listas[nombre] = filas
    return ficha, listas


def main():
    ahora = datetime.now(PERU)
    lista = cuis_activos(ahora.year)
    print(f"{len(lista)} CUIs activos {ahora.year} (pliegos: {','.join(PLIEGOS)})")
    if lista.empty:
        sys.exit("Sin CUIs para consultar; revisa " + ENTRADA)

    sesion = nueva_sesion()
    fichas, acumulado, fallos = [], {k: [] for k in LISTAS}, []
    for n, fila in enumerate(lista.to_dict("records"), 1):
        cui = fila["CUI"]
        try:
            ficha, listas = procesar_cui(sesion, cui)
        except Exception as e:  # noqa: BLE001 - se registra y se sigue
            fallos.append(cui)
            print(f"[{n}/{len(lista)}] {cui} ERROR {e}")
            continue
        base = {"CUI": cui, "EJECUTORA": fila["EJECUTORA"], "ENCONTRADO_SSI": "SI" if ficha else "NO"}
        fichas.append({**base, **(ficha or {})})
        for k, filas in listas.items():
            acumulado[k].extend(filas)
        resumen = " ".join(f"{k}={len(v)}" for k, v in listas.items() if v)
        print(f"[{n}/{len(lista)}] {cui} {base['ENCONTRADO_SSI']} {(ficha or {}).get('ESTADO') or ''} {resumen}")

    if len(fallos) > MAX_PROP_FALLOS * len(lista):
        sys.exit(f"Fallaron {len(fallos)} de {len(lista)} CUIs; no se sobrescriben las salidas.")

    corte = ahora.strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs("output", exist_ok=True)

    df = pd.DataFrame(fichas).reindex(columns=["CUI", "EJECUTORA", "ENCONTRADO_SSI"] + CAMPOS_FICHA)
    df["FECHA_CONSULTA_SSI"] = corte
    df.to_csv(SALIDA.format("ficha"), index=False, encoding="utf-8-sig")
    print(f"ficha: {len(df)} filas ({(df['ENCONTRADO_SSI'] == 'NO').sum()} sin ficha en SSI)")

    for k, filas in acumulado.items():
        dl = pd.DataFrame(filas)
        if dl.empty:
            dl = pd.DataFrame(columns=["CUI", "ORIGEN"])
        dl["FECHA_CONSULTA_SSI"] = corte
        dl.to_csv(SALIDA.format(k), index=False, encoding="utf-8-sig")
        print(f"{k}: {len(dl)} filas")

    if fallos:
        print(f"CUIs con error ({len(fallos)}): {', '.join(fallos)}")


if __name__ == "__main__":
    main()
