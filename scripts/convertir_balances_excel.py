#!/usr/bin/env python3
"""
Convierte un calendario de balances en Excel (mismo formato que
Calendario_Master_Balances_2026.xlsx) a data/balances_manual.json.

Uso:
    python3 scripts/convertir_balances_excel.py ruta/al/archivo.xlsx

Columnas esperadas (fila de encabezado, la app busca automáticamente
la fila correcta buscando "Ticker"):
    Ticker | Nombre de la Empresa | Mercado / Categoría | Trimestre
    | Fecha de Presentación ("30 de Julio de 2026") | Horario (EST)
    | Estado Actual
"""
import sys, json, re
from datetime import datetime, timezone, timedelta
import pandas as pd

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def parsear_fecha(texto):
    if not isinstance(texto, str):
        return None
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", texto, re.IGNORECASE)
    if not m:
        return None
    dia, mes_txt, anio = m.groups()
    mes = MESES.get(mes_txt.lower())
    if not mes:
        return None
    return f"{anio}-{mes:02d}-{int(dia):02d}"


def encontrar_fila_encabezado(ruta):
    crudo = pd.read_excel(ruta, header=None)
    for i, row in crudo.iterrows():
        if row.astype(str).str.contains("Ticker", case=False, na=False).any():
            return i
    raise ValueError("No se encontró una fila con la columna 'Ticker'. Revisar el formato del archivo.")


def main():
    if len(sys.argv) < 2:
        print("Uso: python3 scripts/convertir_balances_excel.py archivo.xlsx")
        sys.exit(1)
    ruta = sys.argv[1]
    fila_header = encontrar_fila_encabezado(ruta)
    df = pd.read_excel(ruta, header=fila_header)
    df = df.dropna(subset=["Ticker"])

    hoy_art = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")
    por_tk = {}
    sin_fecha = []
    for _, row in df.iterrows():
        tk = str(row["Ticker"]).strip().upper()
        fecha = parsear_fecha(row.get("Fecha de Presentación"))
        if fecha is None:
            sin_fecha.append(tk)
            continue
        por_tk.setdefault(tk, []).append(fecha)

    resultado = {}
    for tk, fechas in por_tk.items():
        fechas = sorted(set(fechas))
        futuras = [f for f in fechas if f >= hoy_art]
        pasadas = [f for f in fechas if f < hoy_art]
        resultado[tk] = {
            "prox": futuras[0] if futuras else None,
            "ultimo": pasadas[-1] if pasadas else None,
            "todas": fechas[-8:],
        }

    salida = "data/balances_manual.json"
    json.dump(resultado, open(salida, "w"), ensure_ascii=False)
    print(f"OK: {len(resultado)} tickers -> {salida}")
    if sin_fecha:
        print(f"Sin fecha parseable (revisar si son ETFs, esperable, o un formato de fecha distinto): {sin_fecha}")


if __name__ == "__main__":
    main()
