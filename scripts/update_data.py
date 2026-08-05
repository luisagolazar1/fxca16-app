#!/usr/bin/env python3
"""
FXCA16 — Descarga datos + genera data.js + push a GitHub
Corre en GitHub Actions (o local)
"""
import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import time, os, sys
from datetime import datetime

BARRAS = 1600

# ── Tickers ──
USA_TICKERS = [
    "AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "ORCL", "NFLX",
    "AMD", "INTC", "QCOM", "TXN", "MU", "CRM", "NOW", "ADBE", "IBM", "UBER",
    "COIN", "MELI", "SPOT", "BABA", "PYPL", "GLOB", "JPM", "BAC", "WFC", "GS",
    "MS", "C", "AXP", "V", "MA", "BRK-B", "NDAQ", "UNH", "JNJ", "PFE",
    "ABBV", "MRK", "LLY", "CAH", "WMT", "KO", "PEP", "PG", "MCD", "SBUX",
    "NKE", "DIS", "XOM", "CVX", "VIST", "PBR", "AAL", "DAL", "UAL", "F",
    "GM", "VZ", "T", "TMUS", "ABT", "TMO", "DHR", "CVS", "BA", "CAT",
    "HON", "RTX", "AMT", "PLTR", "SNOW", "NET", "SHOP", "SQ", "RBLX", "SCHW",
    "BLK", "COF", "ISRG", "REGN", "GILD", "COST", "TGT", "HD", "LOW", "SLB",
    "OXY", "GE", "UPS", "FDX", "SPY", "QQQ", "IWM", "DIA", "GLD", "SLV",
    "XLE", "XLF", "XLK", "TLT",
]

MERVAL_TICKERS_YF = [
    "AGRO.BA","ALUA.BA","AUSO.BA","BHIP.BA","BMA.BA","BOLT.BA","BPAT.BA",
    "BYMA.BA","CADO.BA","CAPX.BA","CARC.BA","CECO2.BA","CELU.BA","CEPU.BA",
    "CGPA2.BA","COME.BA","CVH.BA","CTIO.BA","DGCU2.BA","EDN.BA","FERR.BA",
    "FIPL.BA","GAMI.BA","GARO.BA","GBAN.BA","GCLA.BA","GGAL.BA","GRIM.BA",
    "HARG.BA","INTR.BA","INVJ.BA","IRSA.BA","LEDE.BA","LOMA.BA","LONG.BA",
    "METR.BA","MIRG.BA","MOLI.BA","MORI.BA","OEST.BA","PAMP.BA","PATA.BA",
    "POLL.BA","RICH.BA","RIGO.BA","ROSE.BA","SAMI.BA","SEMI.BA","SUPV.BA",
    "TECO2.BA","TGNO4.BA","TGSU2.BA","TXAR.BA","VALO.BA","YPFD.BA",
]

def clean(t): return t.replace(".BA", "")

# ── Cargar tickers custom agregados desde la app ──
def load_custom_tickers():
    custom_path = os.path.join(os.path.dirname(__file__), "..", "custom_tickers.json")
    try:
        with open(custom_path) as f:
            data = json.load(f)
        tickers = data.get("tickers", [])
        if tickers:
            print(f"📌 Custom tickers: {tickers}")
        return tickers
    except:
        return []

def descargar_grupo(tickers_yf, moneda, periodo="2y", intervalo="1h"):
    """
    NOTA IMPORTANTE SOBRE HORIZONTE DE DATOS
    Yahoo limita el intervalo 1h a ~1 año real, sin importar que se pida "2y".
    Con 11 meses de historia toda validación cubre un solo régimen de mercado,
    que fue exactamente el problema detectado: el edge medido provenía de un
    único mes de rally.
    Por eso ahora se descarga TAMBIÉN una serie diaria de 10 años (descargar_diario),
    que es lo que alimenta la validación estadística y el motor de alfa.
    """
    print(f"\n{'━'*55}")
    print(f"  Descargando {len(tickers_yf)} tickers ({moneda}) — {intervalo} / {periodo}")
    print(f"{'━'*55}")
    all_rows = []
    errores = []
    for yf_ticker in tickers_yf:
        try:
            print(f"  {yf_ticker:<12}", end=" ", flush=True)
            df = yf.download(yf_ticker, period=periodo, interval=intervalo,
                           auto_adjust=True, progress=False)
            if df.empty:
                print("⚠️  sin datos"); errores.append(yf_ticker); continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.reset_index()
            dt_col = "Datetime" if "Datetime" in df.columns else "Date"
            df = df.rename(columns={dt_col: "datetime"})
            if hasattr(df["datetime"].dtype, "tz") and df["datetime"].dtype.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert(None)
            df["hour"]   = pd.to_datetime(df["datetime"]).dt.hour
            df["ticker"] = clean(yf_ticker)
            df["moneda"] = moneda
            col_map = {c: c.lower() for c in df.columns}
            df = df.rename(columns=col_map)
            df = df[["ticker","datetime","hour","open","high","low","close","volume","moneda"]]
            df = df.dropna(subset=["close"])
            df = df[df["close"] > 0]
            print(f"✅  {len(df):>5} barras | {df['close'].iloc[-1]:>12,.2f}")
            all_rows.append(df)
        except Exception as e:
            print(f"❌  {e}"); errores.append(yf_ticker)
    if not all_rows: return pd.DataFrame(), errores
    result = pd.concat(all_rows, ignore_index=True)
    return result.sort_values(["ticker","datetime"]).reset_index(drop=True), errores

def backtest_w(bars, w):
    closes = [b['c'] for b in bars]
    highs  = [b['hi'] for b in bars]
    lows   = [b['lo'] for b in bars]
    n = len(closes)
    if n < 60: return 0, 0
    s20 = s50 = 0
    sma20, sma50 = [None]*n, [None]*n
    for i in range(n):
        s20 += closes[i]; s50 += closes[i]
        if i >= 20: s20 -= closes[i-20]
        if i >= 50: s50 -= closes[i-50]
        if i >= 19: sma20[i] = s20 / min(i+1,20)
        if i >= 49: sma50[i] = s50 / min(i+1,50)
    atrs = [highs[i]-lows[i] if i==0 else
            max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1]))
            for i in range(n)]
    atr_arr = [sum(atrs[max(0,i-13):i+1])/14 if i>=13 else None for i in range(n)]
    trades = []
    for d in range(55, n-w-1):
        if not sma20[d] or not sma50[d] or not atr_arr[d]: continue
        buy  = sma20[d]>sma50[d] and closes[d]>sma20[d]
        sell = sma20[d]<sma50[d] and closes[d]<sma20[d]
        if not buy and not sell: continue
        entry=closes[d]; atr=atr_arr[d]
        sl=entry-atr*1.5 if buy else entry+atr*1.5
        tp=entry+atr*2.5 if buy else entry-atr*2.5
        ex=closes[min(d+w,n-1)]
        for f in range(1,w+1):
            if d+f>=n: break
            if buy:
                if lows[d+f]<=sl:  ex=sl; break
                if highs[d+f]>=tp: ex=tp; break
            else:
                if highs[d+f]>=sl: ex=sl; break
                if lows[d+f]<=tp:  ex=tp; break
        trades.append((ex-entry)/entry*(1 if buy else -1)>0)
    if not trades: return 0, 0
    return sum(trades)/len(trades), len(trades)


# ══════════════════════════════════════════════════════════════
# HISTÓRICO DIARIO LARGO — 10 años
# El intervalo 1d no tiene la limitación del 1h. Permite validar
# a través de varios regímenes (2018, 2020, 2022) en vez de uno solo.
# Además pesa ~10x menos que la serie horaria.
# ══════════════════════════════════════════════════════════════
# (descargar_diario completo quedó obsoleto: reemplazado por
# descargar_diario_incremental, que reutiliza el histórico ya bajado)
def descargar_noticias(df_total, tickers_yf, max_tickers=25, previo=None):
    """
    Noticias del día, embebidas en data.js.

    La app es 100% estática (sin backend), así que no puede llamar a una
    API de noticias desde el navegador sin exponer credenciales. Se bajan
    acá y viajan en data.js, igual que los precios.

    Criterio de relevancia: se piden noticias de los activos que MÁS SE
    MOVIERON hoy. Ahí es donde una noticia efectivamente explica algo —
    pedirlas de un papel que no se movió devuelve ruido de agenda.

    NOTA: no pudo probarse contra Yahoo real en el entorno donde se
    escribió (sandbox sin salida a finance.yahoo.com). El parseo tolera
    varias formas de respuesta porque la API de noticias de Yahoo cambia
    de shape seguido. Revisar el log de la primera corrida, sección
    "📰 Noticias", antes de confiar en el resultado.
    """
    previo = previo or {}
    print(f"\n📰 Noticias: buscando en los {max_tickers} activos que más se movieron")
    if df_total is None or df_total.empty:
        print("    sin datos de precios — se omite")
        return previo

    # Variación del último día por ticker
    movs = []
    try:
        for tk, g in df_total.groupby("ticker"):
            g = g.sort_values("datetime")
            if len(g) < 2:
                continue
            cierres = g["close"].dropna()
            if len(cierres) < 2:
                continue
            var = abs(cierres.iloc[-1] / cierres.iloc[-2] - 1) * 100
            movs.append((tk, var))
    except Exception as e:
        print(f"    no se pudo calcular variación: {e}")
        return previo
    movs.sort(key=lambda x: -x[1])
    candidatos = [tk for tk, _ in movs[:max_tickers]]
    var_por_tk = dict(movs)

    def extraer(art):
        """Tolera varias formas: el shape de Yahoo cambia seguido."""
        c = art.get("content") if isinstance(art.get("content"), dict) else art
        titulo = c.get("title") or c.get("headline") or ""
        if not titulo:
            return None
        url = ""
        for k in ("canonicalUrl", "clickThroughUrl", "link"):
            v = c.get(k)
            if isinstance(v, dict):
                url = v.get("url") or ""
            elif isinstance(v, str):
                url = v
            if url:
                break
        prov = c.get("provider")
        fuente = prov.get("displayName", "") if isinstance(prov, dict) else (prov or "")
        fecha = c.get("pubDate") or c.get("displayTime") or c.get("providerPublishTime") or ""
        if isinstance(fecha, (int, float)):
            try:
                fecha = pd.to_datetime(fecha, unit="s").strftime("%Y-%m-%dT%H:%M:%S")
            except Exception:
                fecha = ""
        return {"titulo": str(titulo)[:220], "url": str(url)[:400],
                "fuente": str(fuente)[:60], "fecha": str(fecha)[:25]}

    hoy = pd.Timestamp.now().normalize()
    por_ticker, todas = {}, []
    fallidos = 0
    for i, yf_tk in enumerate(candidatos, 1):
        if not hay_tiempo(45):
            print(f"    ⏭  corte por presupuesto de tiempo en {i}/{len(candidatos)}")
            break
        try:
            arts = yf.Ticker(yf_tk).get_news(count=6, tab="news") or []
        except Exception:
            fallidos += 1
            continue
        limpias = []
        for a in arts:
            e = extraer(a)
            if e and e["titulo"]:
                limpias.append(e)
        if not limpias:
            continue
        tk = clean(yf_tk)
        por_ticker[tk] = limpias[:4]
        for e in limpias[:3]:
            reciente = True
            try:
                if e["fecha"]:
                    d = pd.Timestamp(e["fecha"]).tz_localize(None).normalize()
                    reciente = (hoy - d).days <= 2
            except Exception:
                pass
            if reciente:
                todas.append({**e, "ticker": tk, "var": round(var_por_tk.get(yf_tk, 0), 2)})

    # Destacadas: las de los activos con mayor movimiento, sin repetir ticker
    todas.sort(key=lambda x: -x["var"])
    destacadas, vistos = [], set()
    for n in todas:
        if n["ticker"] in vistos:
            continue
        vistos.add(n["ticker"])
        destacadas.append(n)
        if len(destacadas) >= 6:
            break

    print(f"    OK: {len(por_ticker)} tickers con noticias · {len(destacadas)} destacadas")
    if fallidos:
        print(f"    sin noticias o error: {fallidos}")
    log_tiempo("noticias")
    return {"destacadas": destacadas, "porTicker": por_ticker,
            "actualizado": pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S")}


def descargar_earnings(tickers_yf, previo=None):
    """
    yf.Ticker.get_earnings_dates() quedó roto (Yahoo cambió el endpoint viejo,
    ver https://github.com/ranaroussi/yfinance/issues/2591 y similares — devuelve
    None incluso para tickers grandes como AAPL). Se reemplaza por la API nueva
    de calendarios (yf.Calendars, agregada en yfinance #2615), que consulta por
    rango de fechas y permite filtrar por ticker vía CalendarQuery de bajo nivel
    (el wrapper público get_earnings_calendar() no expone ese filtro).

    NOTA: esta función no pudo probarse contra datos reales de Yahoo en el
    entorno donde se escribió (sandbox sin salida a finance.yahoo.com). Revisar
    el log de la primera corrida del workflow tras este cambio antes de confiar
    en los resultados.
    """
    from yfinance.calendars import CalendarQuery

    previo = previo or {}
    pendientes = [t for t in tickers_yf if clean(t) not in previo]
    orden = pendientes + [t for t in tickers_yf if clean(t) in previo]
    print(f"\n📅 Earnings: {len(pendientes)} sin datos de {len(tickers_yf)}")
    cal = dict(previo)
    fallidos, sin_procesar = [], 0
    hoy = pd.Timestamp.now().normalize()
    ini_rango = (hoy - pd.Timedelta(days=200)).strftime("%Y-%m-%d")
    fin_rango = (hoy + pd.Timedelta(days=200)).strftime("%Y-%m-%d")

    cal_api = yf.Calendars()
    BATCH = 15  # lotes chicos: la API cap a 100 resultados por consulta y
                # queremos margen si varios tickers del lote reportan el mismo día

    def consultar_lote(lote):
        """Devuelve {ticker_yf: [fechas 'YYYY-MM-DD', ...]} para earnings del lote
        dentro de [ini_rango, fin_rango]."""
        resultados = {t: [] for t in lote}
        query = CalendarQuery("and", [
            CalendarQuery("or", [
                CalendarQuery("eq", ["eventtype", "EAD"]),
                CalendarQuery("eq", ["eventtype", "ERA"]),
            ]),
            CalendarQuery("gte", ["startdatetime", ini_rango]),
            CalendarQuery("lte", ["startdatetime", fin_rango]),
            CalendarQuery("or", [CalendarQuery("eq", ["ticker", t]) for t in lote]),
        ])
        try:
            df = cal_api._get_data("EARNINGS", query, limit=100, force=True)
        except Exception:
            return None  # distinto de {} — señala fallo de consulta, no "sin earnings"
        if df is None or df.empty:
            return resultados
        col_ticker = next((c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower()), None)
        col_fecha  = next((c for c in df.columns if "date" in c.lower() or "time" in c.lower()), None)
        if col_ticker is None or col_fecha is None:
            return None
        for _, row in df.iterrows():
            tkr = str(row[col_ticker]).upper()
            if tkr not in resultados:
                continue
            try:
                f = pd.Timestamp(row[col_fecha]).tz_localize(None).normalize()
                resultados[tkr].append(f.strftime("%Y-%m-%d"))
            except Exception:
                continue
        return resultados

    for i in range(0, len(orden), BATCH):
        if not hay_tiempo(45):
            sin_procesar += len(orden) - i
            break
        lote = orden[i:i + BATCH]
        res = consultar_lote(lote)
        if res is None:
            fallidos.extend(lote)
            continue
        for yf_ticker, fechas in res.items():
            if not fechas:
                fallidos.append(yf_ticker); continue
            tk = clean(yf_ticker)
            fechas = sorted(set(fechas))
            futuras = [f for f in fechas if f >= hoy.strftime("%Y-%m-%d")]
            pasadas = [f for f in fechas if f <  hoy.strftime("%Y-%m-%d")]
            cal[tk] = {
                "prox":   futuras[0] if futuras else None,
                "ultimo": pasadas[-1] if pasadas else None,
                "todas":  fechas[-8:],
            }
        print(f"    {min(i + BATCH, len(orden))}/{len(orden)}...")

    print(f"    OK: {len(cal)} tickers con calendario")
    if fallidos:
        print(f"    sin datos: {len(fallidos)} ({fallidos[:8]})")
    if sin_procesar:
        print(f"    ⏭  {sin_procesar} para la próxima corrida")
    log_tiempo("earnings")
    return cal


# ══════════════════════════════════════════════════════════════
# FUNDAMENTALES — CALIDAD COMO FILTRO DE RIESGO
#
# Uso deliberadamente acotado: NO se usa para predecir retornos.
# Se usa para EXCLUIR empresas frágiles del universo operable.
#
# Por qué esta distinción importa: Yahoo entrega la foto ACTUAL,
# no datos point-in-time. Usar el P/E de hoy para backtestear 2024
# es sesgo de anticipación. Pero preguntar "¿esta empresa pierde
# plata hoy?" para decidir si la incluyo hoy es legítimo: no hay
# ninguna afirmación histórica involucrada.
#
# Basado en los factores de calidad de Novy-Marx y Asness (QMJ):
# rentabilidad, solidez financiera y generación de caja.
# ══════════════════════════════════════════════════════════════
def descargar_fundamentales(tickers_yf, previo=None):
    previo = previo or {}
    pendientes = [t for t in tickers_yf if clean(t) not in previo]
    orden = pendientes + [t for t in tickers_yf if clean(t) in previo]
    print(f"\n🏛️  Fundamentales: {len(pendientes)} sin datos de {len(tickers_yf)}")
    out = dict(previo)
    sin_datos, sin_procesar = [], 0
    for i, yf_ticker in enumerate(orden, 1):
        if not hay_tiempo(45):
            sin_procesar += 1
            continue
        try:
            info = yf.Ticker(yf_ticker).get_info() or {}
            tk = clean(yf_ticker)
            tipo = info.get("quoteType", "")

            # Los ETF no tienen fundamentales de empresa
            if tipo in ("ETF", "MUTUALFUND", "INDEX"):
                out[tk] = {"tipo": "ETF", "calidad": None}
                continue

            g = lambda k: info.get(k) if isinstance(info.get(k), (int, float)) else None
            f = {
                "tipo":        "ACCION",
                "margenNeto":  g("profitMargins"),
                "margenOper":  g("operatingMargins"),
                "roe":         g("returnOnEquity"),
                "deudaPatr":   g("debtToEquity"),
                "liquidez":    g("currentRatio"),
                "fcf":         g("freeCashflow"),
                "crecIng":     g("revenueGrowth"),
                "crecGan":     g("earningsGrowth"),
                "capBursatil": g("marketCap"),
                "beta":        g("beta"),
                "sector":      info.get("sector"),
            }

            # ── Score de calidad 0-100 y banderas rojas ──
            puntos, maximo, banderas = 0, 0, []

            if f["margenNeto"] is not None:
                maximo += 25
                if   f["margenNeto"] > 0.15: puntos += 25
                elif f["margenNeto"] > 0.05: puntos += 18
                elif f["margenNeto"] > 0:    puntos += 10
                else: banderas.append("margen neto negativo")

            if f["roe"] is not None:
                maximo += 25
                if   f["roe"] > 0.20: puntos += 25
                elif f["roe"] > 0.10: puntos += 18
                elif f["roe"] > 0:    puntos += 10
                else: banderas.append("ROE negativo")

            if f["deudaPatr"] is not None:
                maximo += 25
                d = f["deudaPatr"]
                if   d < 50:  puntos += 25
                elif d < 100: puntos += 18
                elif d < 200: puntos += 10
                else: banderas.append(f"deuda/patrimonio {d:.0f}%")

            if f["fcf"] is not None:
                maximo += 15
                if f["fcf"] > 0: puntos += 15
                else: banderas.append("flujo de caja libre negativo")

            if f["liquidez"] is not None:
                maximo += 10
                if   f["liquidez"] > 1.5: puntos += 10
                elif f["liquidez"] > 1.0: puntos += 6
                else: banderas.append(f"liquidez corriente {f['liquidez']:.2f}")

            f["calidad"]  = round(puntos / maximo * 100) if maximo >= 50 else None
            f["banderas"] = banderas
            f["fragil"]   = len(banderas) >= 2
            out[tk] = f

            if i % 25 == 0:
                print(f"    {i}/{len(tickers_yf)}...")
        except Exception:
            sin_datos.append(yf_ticker)

    conCalidad = sum(1 for v in out.values() if v.get("calidad") is not None)
    fragiles   = sum(1 for v in out.values() if v.get("fragil"))
    print(f"    OK: {len(out)} tickers | {conCalidad} con score | {fragiles} marcados frágiles")
    if sin_datos:
        print(f"    sin datos: {len(sin_datos)}")
    if sin_procesar:
        print(f"    ⏭  {sin_procesar} para la próxima corrida")
    log_tiempo("fundamentales")
    return out



# ══════════════════════════════════════════════════════════════
# PRESUPUESTO DE TIEMPO — trabajo repartido entre corridas
#
# El workflow tiene 15 minutos. La carga inicial (10 años × 158
# tickers + earnings + fundamentales) no entra en una sola pasada.
#
# En vez de pedir más tiempo, el script trabaja hasta agotar su
# presupuesto, guarda lo conseguido y termina limpio. Como el modo
# incremental reutiliza lo ya bajado, la corrida siguiente saltea
# eso en segundos y avanza con lo que falta. En 3-4 corridas queda
# todo completo, y a partir de ahí cada una tarda pocos minutos.
# ══════════════════════════════════════════════════════════════
INICIO_EJECUCION = time.time()
PRESUPUESTO_SEG  = int(os.environ.get("FXCA16_BUDGET", "660"))   # 11 min de los 15

def tiempo_restante():
    return PRESUPUESTO_SEG - (time.time() - INICIO_EJECUCION)

def hay_tiempo(minimo=45):
    return tiempo_restante() > minimo

def log_tiempo(etapa):
    usado = time.time() - INICIO_EJECUCION
    print(f"    ⏱  {etapa}: {usado/60:.1f} min usados · {tiempo_restante()/60:.1f} min restantes")

# ══════════════════════════════════════════════════════════════
# ACTUALIZACIÓN INCREMENTAL DEL HISTÓRICO DIARIO
#
# El histórico de años anteriores no cambia: re-descargarlo cada día
# son ~160 llamadas inútiles a Yahoo y 30 minutos de workflow.
# Esta función lee lo que ya está en data.js y pide solo lo faltante.
#
# EL DETALLE QUE IMPORTA — splits y dividendos:
# con auto_adjust=True Yahoo reajusta TODO el histórico cuando hay un
# split. Si uno solo agrega barras al final, el tramo viejo queda con
# precios en otra escala y todos los indicadores se rompen en silencio.
# Por eso se descarga una ventana de solapamiento y se comparan los
# precios de las fechas comunes: si no coinciden, hubo ajuste y ese
# ticker se re-descarga completo.
# ══════════════════════════════════════════════════════════════
def leer_diario_existente(ruta="src/data.js"):
    """Extrae CSV_DATA_DAILY_RAW del data.js actual."""
    if not os.path.exists(ruta):
        return {}
    try:
        with open(ruta, encoding="utf-8") as f:
            txt = f.read()
        marca = "export const CSV_DATA_DAILY_RAW = "
        i = txt.find(marca)
        if i < 0:
            return {}
        i += len(marca)
        prof, fin = 0, i
        for k in range(i, len(txt)):
            if txt[k] == "{": prof += 1
            elif txt[k] == "}":
                prof -= 1
                if prof == 0:
                    fin = k + 1
                    break
        prev = json.loads(txt[i:fin])
        print(f"  📂 Histórico existente: {len(prev)} tickers")
        return prev
    except Exception as e:
        print(f"  ⚠️  No se pudo leer el histórico previo: {e}")
        return {}


def descargar_diario_incremental(tickers_yf, moneda, previo, periodo_full="10y", solape=7):
    print(f"  Actualizando {len(tickers_yf)} tickers ({moneda}) — modo incremental")
    hoy = pd.Timestamp.now().normalize()
    resultado, completos, incrementales, reajustados = {}, 0, 0, []

    # Prioridad: primero los que no tienen nada (son los que más aportan),
    # después los que solo necesitan ponerse al día.
    faltantes  = [t for t in tickers_yf if len(previo.get(clean(t), [])) < 100]
    al_dia     = [t for t in tickers_yf if len(previo.get(clean(t), [])) >= 100]
    orden      = faltantes + al_dia
    if faltantes:
        print(f"    {len(faltantes)} sin histórico · {len(al_dia)} a actualizar")

    sin_procesar = 0
    for i, yf_ticker in enumerate(orden, 1):
        tk = clean(yf_ticker)
        viejo = previo.get(tk, [])

        # Se agotó el presupuesto: conservar lo que había y seguir en la próxima corrida
        if not hay_tiempo(60):
            if viejo:
                resultado[tk] = viejo
            sin_procesar += 1
            continue

        # Sin datos previos o muy pocos → descarga completa
        if len(viejo) < 100:
            try:
                df = yf.download(yf_ticker, period=periodo_full, interval="1d",
                                 progress=False, auto_adjust=True, threads=False)
                if df is None or df.empty:
                    continue
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                resultado[tk] = _df_a_barras(df.reset_index(), moneda)
                completos += 1
            except Exception:
                pass
            continue

        ult = viejo[-1]["d"]
        dias_faltantes = (hoy - pd.Timestamp(ult)).days
        if dias_faltantes <= 0:
            resultado[tk] = viejo          # ya está al día
            continue

        # Pedir lo faltante + ventana de solapamiento para verificar ajustes
        rango = f"{min(360, dias_faltantes + solape + 5)}d"
        try:
            df = yf.download(yf_ticker, period=rango, interval="1d",
                             progress=False, auto_adjust=True, threads=False)
            if df is None or df.empty:
                resultado[tk] = viejo
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            nuevas = _df_a_barras(df.reset_index(), moneda)
            if not nuevas:
                resultado[tk] = viejo
                continue

            # ¿Hubo split o dividendo? Comparar el solapamiento
            viejo_por_fecha = {b["d"]: b["c"] for b in viejo}
            desvios = []
            for b in nuevas:
                if b["d"] in viejo_por_fecha:
                    ref = viejo_por_fecha[b["d"]]
                    if ref:
                        desvios.append(abs(b["c"] - ref) / ref)
            hubo_ajuste = bool(desvios) and (sum(desvios) / len(desvios)) > 0.005

            if hubo_ajuste:
                # Precios reajustados: el histórico viejo ya no es comparable
                reajustados.append(tk)
                df = yf.download(yf_ticker, period=periodo_full, interval="1d",
                                 progress=False, auto_adjust=True, threads=False)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                resultado[tk] = _df_a_barras(df.reset_index(), moneda)
                completos += 1
            else:
                fechas_nuevas = {b["d"] for b in nuevas}
                fusion = [b for b in viejo if b["d"] not in fechas_nuevas] + nuevas
                fusion.sort(key=lambda b: b["d"])
                resultado[tk] = fusion[-2600:]
                incrementales += 1
        except Exception:
            resultado[tk] = viejo

        if i % 40 == 0:
            print(f"    {i}/{len(tickers_yf)}...")

    print(f"    ✅ {len(resultado)} tickers | {incrementales} incrementales | {completos} completos")
    if reajustados:
        print(f"    🔄 Re-descargados por split/dividendo: {reajustados[:10]}")
    if sin_procesar:
        print(f"    ⏭  {sin_procesar} quedaron para la próxima corrida (presupuesto agotado)")
    log_tiempo(f"diario {moneda}")
    return resultado


def _df_a_barras(df, moneda):
    barras = []
    for _, r in df.iterrows():
        fecha = r.get("Date") or r.get("Datetime")
        if pd.isna(fecha) or pd.isna(r.get("Close")):
            continue
        barras.append({
            "d":  pd.Timestamp(fecha).strftime("%Y-%m-%d"),
            "o":  round(float(r["Open"]), 4),
            "hi": round(float(r["High"]), 4),
            "lo": round(float(r["Low"]), 4),
            "c":  round(float(r["Close"]), 4),
            "v":  int(r["Volume"]) if not pd.isna(r.get("Volume")) else 0,
            "m":  moneda,
        })
    return barras


def leer_bloque_existente(nombre, ruta="src/data.js"):
    """Lee cualquier export const NOMBRE = {...} del data.js actual."""
    if not os.path.exists(ruta):
        return {}
    try:
        with open(ruta, encoding="utf-8") as f:
            txt = f.read()
        marca = f"export const {nombre} = "
        i = txt.find(marca)
        if i < 0:
            return {}
        i += len(marca)
        prof, fin = 0, i
        for k in range(i, len(txt)):
            if txt[k] == "{": prof += 1
            elif txt[k] == "}":
                prof -= 1
                if prof == 0:
                    fin = k + 1
                    break
        return json.loads(txt[i:fin])
    except Exception:
        return {}


def necesita_refresco_semanal(previo, tickers_esperados):
    """
    Earnings y fundamentales cambian por trimestre, no por día.
    Se refrescan los lunes, o si faltan tickers, o si nunca se bajaron.
    """
    if not previo:
        return True, "sin datos previos"
    faltantes = [clean(t) for t in tickers_esperados if clean(t) not in previo]
    if len(faltantes) > 5:
        return True, f"{len(faltantes)} tickers nuevos"
    if pd.Timestamp.now().dayofweek == 0:      # lunes
        return True, "refresco semanal (lunes)"
    return False, "se reutiliza lo existente"

def main():
    print("="*55)
    print("FXCA16 — Actualización de datos")
    print("="*55)

    # ── PASO 1: Descargar datos ──
    df_usa, err_usa = descargar_grupo(USA_TICKERS, moneda="USD")
    df_merval, err_merval = descargar_grupo(MERVAL_TICKERS_YF, moneda="ARS")

    # ── Tickers custom desde la app ──
    custom_tks = load_custom_tickers()
    df_custom = pd.DataFrame()
    if custom_tks:
        # Detectar si son argentinos (.BA) o USA
        custom_usa = [t for t in custom_tks if not t.endswith(".BA") and len(t) <= 6]
        custom_arg = [t if t.endswith(".BA") else t+".BA" for t in custom_tks if t.endswith(".BA")]
        dfs = []
        if custom_usa:
            df_c, _ = descargar_grupo(custom_usa, moneda="USD")
            if not df_c.empty: dfs.append(df_c)
        if custom_arg:
            df_c, _ = descargar_grupo(custom_arg, moneda="ARS")
            if not df_c.empty: dfs.append(df_c)
        if dfs:
            df_custom = pd.concat(dfs, ignore_index=True)
            print(f"📌 Custom descargados: {df_custom['ticker'].nunique()} tickers")

    frames = [df_usa, df_merval]
    if not df_custom.empty: frames.append(df_custom)
    df_total = pd.concat(frames, ignore_index=True)
    df_total = df_total.sort_values(["ticker","datetime"]).reset_index(drop=True)

    # ── Deduplicar (ticker, datetime) ──
    # Si un ticker está a la vez en la lista estándar y en custom_tickers.json
    # se descarga dos veces y el concat de arriba produce barras duplicadas
    # exactas. Eso rompe los indicadores: cada barra repetida es una barra de
    # variación cero intercalada, lo que aplana el RSI hacia 50 y duplica de
    # hecho el período de suavizado de EMA/MACD.
    #
    # Caso real (2026-08-03): AMD, MELI, ORCL, PBR y SPOT tenían el 100% de sus
    # barras horarias duplicadas por estar en ambas listas. Corregirlo cambiaba
    # la señal en 5 de 10 casos medidos — AMD 30D pasaba de COMPRA FUERTE a
    # VENTA y SPOT 7D de VENTA a COMPRA.
    antes = len(df_total)
    df_total = df_total.drop_duplicates(subset=["ticker","datetime"], keep="last").reset_index(drop=True)
    dups = antes - len(df_total)
    if dups:
        print(f"🧹 Deduplicadas {dups:,} barras repetidas (tickers en lista estándar + custom)")

    last_date = str(df_total["datetime"].max())[:10]

    print(f"\n✅ Total: {df_total['ticker'].nunique()} tickers | {len(df_total):,} filas | hasta {last_date}")

    # ── PASO 2: Comprimir para data.js ──
    result = {}
    for tk, grp in df_total.groupby("ticker"):
        grp = grp.sort_values("datetime").tail(BARRAS)
        moneda = str(grp["moneda"].iloc[0])
        result[tk] = [
            {"d":str(r["datetime"])[:10],"h":int(r["hour"]),
             "o":round(float(r["open"]),2),"hi":round(float(r["high"]),2),
             "lo":round(float(r["low"]),2),"c":round(float(r["close"]),2),
             "v":int(r["volume"]),"m":moneda}
            for _, r in grp.iterrows()
        ]

    # ── PASO 2b: HISTÓRICO DIARIO LARGO (10 años) ──
    # Yahoo solo entrega ~1 año en intervalo 1h. Con esa ventana toda
    # validación cubre un único régimen de mercado. La serie diaria
    # permite medir a través de 2018, 2020 y 2022.
    # Se reutiliza lo ya descargado: el histórico viejo no cambia.
    # Solo se piden las ruedas faltantes, con verificación de splits.
    print("\n📅 Actualizando histórico diario (incremental)...")
    previo = leer_diario_existente()
    d_usa    = descargar_diario_incremental(USA_TICKERS,        "USD", previo)
    d_merval = descargar_diario_incremental(MERVAL_TICKERS_YF,  "ARS", previo)
    daily_result = {**d_usa, **d_merval}
    if daily_result:
        n_dias = int(np.median([len(v) for v in daily_result.values()]))
        total  = sum(len(v) for v in daily_result.values())
        print(f"✅ Diario: {len(daily_result)} tickers | mediana {n_dias} ruedas | {total:,} barras")

    # ── PASO 2c: Calendario de earnings real ──
    todos_tk = USA_TICKERS + MERVAL_TICKERS_YF
    earnings_prev = leer_bloque_existente("FXCA16_EARNINGS")
    refrescar, motivo = necesita_refresco_semanal(earnings_prev, todos_tk)
    if len(earnings_prev) < len(todos_tk) * 0.9:
        refrescar, motivo = True, f"faltan {len(todos_tk)-len(earnings_prev)} tickers"
    if refrescar:
        print(f"\n📅 Earnings: descargando ({motivo})")
        try:
            earnings_cal = descargar_earnings(todos_tk, earnings_prev)
        except Exception as e:
            print(f"  earnings falló: {e}")
            earnings_cal = earnings_prev
    else:
        print(f"\n📅 Earnings: {motivo} ({len(earnings_prev)} tickers)")
        earnings_cal = earnings_prev

    # ── PASO 2c-bis: Noticias del día ──
    noticias_prev = leer_bloque_existente("FXCA16_NOTICIAS")
    try:
        noticias = descargar_noticias(df_total, todos_tk, previo=noticias_prev)
    except Exception as e:
        print(f"  noticias falló: {e}")
        noticias = noticias_prev or {}

    # ── PASO 2d: Fundamentales (calidad como filtro) ──
    fund_prev = leer_bloque_existente("FXCA16_FUNDAMENTALES")
    refrescar_f, motivo_f = necesita_refresco_semanal(fund_prev, todos_tk)
    if len(fund_prev) < len(todos_tk) * 0.9:
        refrescar_f, motivo_f = True, f"faltan {len(todos_tk)-len(fund_prev)} tickers"
    if refrescar_f:
        print(f"\n🏛️  Fundamentales: descargando ({motivo_f})")
        try:
            fundamentales = descargar_fundamentales(todos_tk, fund_prev)
        except Exception as e:
            print(f"  fundamentales falló: {e}")
            fundamentales = fund_prev
    else:
        print(f"\n🏛️  Fundamentales: {motivo_f} ({len(fund_prev)} tickers)")
        fundamentales = fund_prev

    # ── PASO 3: Calcular dynParams ──
    print("\n⚙️  Calculando dynParams...")
    dyn_params = {}
    for tk, bars in result.items():
        best_w, best_wr, best_sims = 7, 0, 0
        for w in [5,7,10,14]:
            wr, sims = backtest_w(bars, w)
            if sims >= 10 and wr > best_wr:
                best_w, best_wr, best_sims = w, wr, sims
        if best_sims == 0:
            _, best_sims = backtest_w(bars, 7)
        dyn_params[tk] = {
            "w": best_w, "wr": round(best_wr, 3), "sims": best_sims,
            "conf": round((best_wr-0.5)*0.4, 3) if best_sims>=10 else 0,
            "p80adj": -3 if best_wr>=0.65 else (3 if best_wr<=0.35 else 0),
        }
    print(f"✅ {len(dyn_params)} tickers calibrados")

    # ── PASO 4: Generar data.js ──
    raw = json.dumps(result, separators=(',',':'))
    dyn_raw = json.dumps(dyn_params, separators=(',',':'))
    daily_raw = json.dumps(daily_result, separators=(',',':'))
    earn_raw  = json.dumps(earnings_cal, separators=(',',':'))
    fund_raw  = json.dumps(fundamentales, separators=(',',':'))
    news_raw  = json.dumps(noticias, separators=(',',':'), ensure_ascii=False)

    # ── Resumen de completitud ──
    n_esp = len(USA_TICKERS) + len(MERVAL_TICKERS_YF)
    completo = (len(daily_result) >= n_esp*0.95 and
                len(earnings_cal) >= n_esp*0.9 and
                len(fundamentales) >= n_esp*0.9)
    print("\n" + "="*58)
    print("ESTADO DE LA CARGA")
    print("="*58)
    for nom, got in [("Histórico diario", len(daily_result)),
                     ("Calendario earnings", len(earnings_cal)),
                     ("Fundamentales", len(fundamentales))]:
        pct = got/n_esp*100 if n_esp else 0
        barra = "█"*int(pct/5) + "░"*(20-int(pct/5))
        print(f"  {nom:22s} {barra} {got:3d}/{n_esp} ({pct:.0f}%)")
    if completo:
        print("\n  ✅ Carga completa. Las próximas corridas serán rápidas.")
    else:
        print("\n  ⏳ Carga parcial — volvé a correr el workflow para continuar.")
        print("     Lo ya descargado se conserva; la próxima retoma donde quedó.")
    print("="*58)

    data_js = f"""// FXCA16 — datos actualizados al {last_date}
// Generado automáticamente — no editar manualmente

const CSV_DATA_EMBEDDED_RAW = {raw};

// Histórico DIARIO de ~10 años. Yahoo limita el intervalo 1h a ~1 año,
// insuficiente para validar a través de distintos regímenes de mercado.
// Esta serie es la que alimenta el tab Validación y el motor de alfa.
export const CSV_DATA_DAILY_RAW = {daily_raw};

export function expandDaily(raw) {{
  const out = {{}};
  for (const [tk, bars] of Object.entries(raw)) {{
    out[tk] = bars.map(b => ({{
      date:b.d, open:b.o, high:b.hi, low:b.lo,
      close:b.c, volume:b.v, moneda:b.m, _ticker:tk
    }}));
  }}
  return out;
}}

// Calendario de earnings real por ticker (fechas efectivas de Yahoo).
// Reemplaza la lista escrita a mano, que tenía huecos.
export const FXCA16_EARNINGS = {earn_raw};

// Fundamentales — USO ACOTADO A FILTRO DE RIESGO.
// Son la foto actual, no datos point-in-time: sirven para decidir qué
// incluir en el universo HOY, nunca para validar históricamente.
export const FXCA16_FUNDAMENTALES = {fund_raw};

// Noticias del día. Se bajan en el workflow porque la app es estática y
// no puede llamar a una API de noticias sin exponer credenciales.
// "destacadas" = las de los activos que más se movieron hoy.
export const FXCA16_NOTICIAS = {news_raw};

export const FXCA16_DYN_PARAMS = {dyn_raw};

export function expandEmbedded(raw) {{
  const out = {{}};
  for (const [tk, bars] of Object.entries(raw)) {{
    out[tk] = bars.map(b => ({{
      date:b.d, hour:b.h, open:b.o, high:b.hi,
      low:b.lo, close:b.c, volume:b.v, moneda:b.m, _ticker:tk
    }}));
  }}
  return out;
}}

export default CSV_DATA_EMBEDDED_RAW;
"""

    # ── PASO 5: Escribir data.js ──
    data_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'data.js')
    with open(data_path, 'w') as f:
        f.write(data_js)

    print(f"\n📦 data.js: {len(result)} tickers · {len(data_js)/1024:.0f} KB")
    print(f"✅ Archivo escrito en {data_path}")
    print(f"📅 Datos hasta: {last_date}")

if __name__ == "__main__":
    main()
