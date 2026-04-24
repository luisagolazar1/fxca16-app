#!/usr/bin/env python3
"""
FXCA16 — Descarga datos + genera data.js + push a GitHub
Corre en GitHub Actions (o local)
"""
import yfinance as yf
import pandas as pd
import json, os, sys
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
    "HON", "RTX", "AMT", "SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "XLE",
    "XLF", "XLK", "TLT",
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

    data_js = f"""// FXCA16 — datos actualizados al {last_date}
// Generado automáticamente — no editar manualmente

const CSV_DATA_EMBEDDED_RAW = {raw};

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
