import { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import CSV_DATA_EMBEDDED_RAW, { expandEmbedded as expandEmbeddedImport } from './data.js';

// ╔══════════════════════════════════════════════════════════════════╗
// ║         FXCA16 — SISTEMA COMBINADO                  ║
// ║         Merval Argentina + Acciones USA · v2.0                   ║
// ╠══════════════════════════════════════════════════════════════════╣
// ║  CÓMO USAR EN OTRA IA (ChatGPT, Gemini, etc.):                  ║
// ║  1. Copiá TODO este archivo                                      ║
// ║  2. Escribí: "Renderizá este componente React como artifact:"    ║
// ║  3. Pegá el código                                               ║
// ║                                                                  ║
// ║  PRECIOS: BYMA open API → Yahoo Finance → Claude web_search      ║
// ║  SIN API KEY: funciona igual con precios simulados               ║
// ╠══════════════════════════════════════════════════════════════════╣
// ║  ALGORITMO:                                                      ║
// ║  · FXCA16 (65%): RSI + MACD + Bollinger + ATR + SMA20/50/200   ║
// ║  · EVO-SCORE (35%): Score (0-3) + vol_24h + mom_6h               ║
// ║  · Umbral dinámico: Percentil 80 (top 20% señales)              ║
// ╠══════════════════════════════════════════════════════════════════╣
// ║  MERCADOS: Merval AR (20 tickers) | USA (28 tickers)             ║
// ╚══════════════════════════════════════════════════════════════════╝

// ── DATOS REALES: 80 tickers · 60 barras 1h · hasta 2026-03-25 ──
const CSV_DATA_EMBEDDED = CSV_DATA_EMBEDDED_RAW;
const LAST_PRICES = {};
function expandEmbedded(raw){const out={};for(const [tk,bars] of Object.entries(raw)){out[tk]=bars.map(b=>({date:b.d,hour:b.h,open:b.o,high:b.hi,low:b.lo,close:b.c,volume:b.v,moneda:b.m,_ticker:tk}));}return out;}


// ═══════════════════════════════════════════════════════════════
// FXCA16 — SISTEMA COMBINADO MERVAL
// FXCA16: RSI + MACD + Bollinger + ATR + SMA
// EVO-SCORE: Score + vol_24h + dist_high/low + sesgo horario
// Umbral dinámico: percentil 80 (como EVO original)
// ═══════════════════════════════════════════════════════════════

const TICKERS_USA = [
  { ticker:"AAPL",  name:"Apple",              sector:"Tecnología"  },
  { ticker:"NVDA",  name:"Nvidia",             sector:"Tecnología"  },
  { ticker:"META",  name:"Meta",               sector:"Tecnología"  },
  { ticker:"TSLA",  name:"Tesla",              sector:"Autos"       },
  { ticker:"AMZN",  name:"Amazon",             sector:"Tech/Retail" },
  { ticker:"MSFT",  name:"Microsoft",          sector:"Tecnología"  },
  { ticker:"GOOGL", name:"Alphabet",           sector:"Tecnología"  },
  { ticker:"COIN",  name:"Coinbase",           sector:"Crypto"      },
  { ticker:"MELI",  name:"MercadoLibre",       sector:"Fintech"     },
  { ticker:"SPOT",  name:"Spotify",            sector:"Medios"      },
  { ticker:"BABA",  name:"Alibaba",            sector:"Tech/Retail" },
  { ticker:"PYPL",  name:"PayPal",             sector:"Fintech"     },
  { ticker:"DIS",   name:"Disney",             sector:"Medios"      },
  { ticker:"SPY",   name:"S&P 500 ETF",        sector:"ETF"         },
  { ticker:"GLD",   name:"Gold ETF",           sector:"Commodities" },
  { ticker:"XLE",   name:"Energy ETF",         sector:"Energía"     },
  { ticker:"AXP",   name:"Amex",               sector:"Financiero"  },
  { ticker:"BAC",   name:"Bank of America",    sector:"Financiero"  },
  { ticker:"WFC",   name:"Wells Fargo",        sector:"Financiero"  },
  { ticker:"C",     name:"Citigroup",          sector:"Financiero"  },
  { ticker:"KO",    name:"Coca-Cola",          sector:"Consumo"     },
  { ticker:"PG",    name:"Procter & Gamble",   sector:"Consumo"     },
  { ticker:"AAL",   name:"American Airlines",  sector:"Aerolíneas"  },
  { ticker:"CAH",   name:"Cardinal Health",    sector:"Salud"       },
  { ticker:"NDAQ",  name:"Nasdaq Inc.",        sector:"Financiero"  },
  { ticker:"GLOB",  name:"Globant",            sector:"Tecnología"  },
  { ticker:"VIST",  name:"Vista Energy",       sector:"Energía"     },
  { ticker:"PBR",   name:"Petrobras",          sector:"Energía"     },
];

const TICKERS_MERVAL = [
  { ticker:"AGRO", name:"Agrometal",           sector:"Agroindustria" },
  { ticker:"ALUA", name:"Aluar",               sector:"Materiales"   },
  { ticker:"AUSO", name:"Autopistas",           sector:"Autos"        },
  { ticker:"BHIP", name:"Bco.Hipotecario",      sector:"Financiero"   },
  { ticker:"BMA",  name:"Banco Macro",          sector:"Financiero"   },
  { ticker:"BOLT", name:"Boldt",                sector:"Materiales"   },
  { ticker:"BPAT", name:"Banco Patagonia",      sector:"Financiero"   },
  { ticker:"BYMA", name:"BYMA",                 sector:"Financiero"   },
  { ticker:"CADO", name:"Cado",                 sector:"Alimentos"    },
  { ticker:"CAPX", name:"Capex",                sector:"Energía"      },
  { ticker:"CARC", name:"Carc",                 sector:"Materiales"   },
  { ticker:"CECO2",name:"Cen.Costanera",        sector:"Energía"      },
  { ticker:"CELU", name:"Celulosa Arg.",         sector:"Materiales"   },
  { ticker:"CEPU", name:"Central Puerto",       sector:"Energía"      },
  { ticker:"CGPA2",name:"Camuzzi Gas",          sector:"Energía"      },
  { ticker:"CTIO", name:"Consultatio",          sector:"Inmuebles"    },
  { ticker:"CVH",  name:"Cablevision Hold.",    sector:"Telecom"      },
  { ticker:"DGCU2",name:"Dist.Gas Cuyo",        sector:"Energía"      },
  { ticker:"EDN",  name:"Edenor",              sector:"Utilities"    },
  { ticker:"FERR", name:"Ferrum",               sector:"Materiales"   },
  { ticker:"FIPL", name:"Fiplasto",             sector:"Financiero"   },
  { ticker:"GAMI", name:"Gami",                 sector:"Tecnología"   },
  { ticker:"GARO", name:"Garovaglio",           sector:"Materiales"   },
  { ticker:"GBAN", name:"Grupo Fin. Galicia",   sector:"Financiero"   },
  { ticker:"GCLA", name:"Grupo Clarín",         sector:"Medios"       },
  { ticker:"GGAL", name:"Grupo Galicia",        sector:"Financiero"   },
  { ticker:"GRIM", name:"Grimoldi",             sector:"Materiales"   },
  { ticker:"HARG", name:"Holcim Arg.",          sector:"Materiales"   },
  { ticker:"INTR", name:"Introductora",         sector:"Salud"        },
  { ticker:"INVJ", name:"Inv. Juramento",       sector:"Financiero"   },
  { ticker:"IRSA", name:"IRSA",                sector:"Inmuebles"    },
  { ticker:"LEDE", name:"Ledesma",              sector:"Alimentos"    },
  { ticker:"LOMA", name:"Loma Negra",           sector:"Materiales"   },
  { ticker:"LONG", name:"Longvie",              sector:"Otros"        },
  { ticker:"METR", name:"Metrogas",             sector:"Energía"      },
  { ticker:"MIRG", name:"Mirgor",              sector:"Tecnología"   },
  { ticker:"MOLI", name:"Molinos Rio Plata",    sector:"Alimentos"    },
  { ticker:"MORI", name:"Morixe",               sector:"Alimentos"    },
  { ticker:"OEST", name:"Dist.Gas Oeste",       sector:"Energía"      },
  { ticker:"PAMP", name:"Pampa Energía",        sector:"Energía"      },
  { ticker:"PATA", name:"Banco Patagonia",      sector:"Financiero"   },
  { ticker:"RICH", name:"Rigolleau",            sector:"Alimentos"    },
  { ticker:"RIGO", name:"Rigo",                 sector:"Materiales"   },
  { ticker:"SAMI", name:"San Miguel",           sector:"Salud"        },
  { ticker:"SEMI", name:"Semillas",             sector:"Tecnología"   },
  { ticker:"SUPV", name:"Supervielle",          sector:"Financiero"   },
  { ticker:"TECO2",name:"Telecom Arg.",         sector:"Telecom"      },
  { ticker:"TGNO4",name:"TGN",                sector:"Energía"      },
  { ticker:"TGSU2",name:"TGS",                sector:"Energía"      },
  { ticker:"TXAR", name:"Ternium Arg.",         sector:"Materiales"   },
  { ticker:"VALO", name:"Grupo Valores",        sector:"Financiero"   },
  { ticker:"YPFD", name:"YPF",                 sector:"Energía"      },
];

// Lista combinada USA + Merval con moneda por ticker
const TICKERS_TODOS = [
  ...TICKERS_USA.map(t => ({...t, moneda:"USD"})),
  ...TICKERS_MERVAL.map(t => ({...t, moneda:"ARS"})),
];

// ── FETCH PRECIOS — Claude web_search en batches ────────────────
// CORS bloquea Yahoo Finance y BYMA desde el browser de claude.ai.
// La única fuente que funciona es api.anthropic.com (mismo origen).
// Estrategia: batches de 5 tickers para mayor precisión de precios.

const MODEL = "claude-sonnet-4-20250514";
const TOOLS = [{ type: "web_search_20250305", name: "web_search" }];

async function claudeBatch(batch, market, log) {
  const tks = batch.join(", ");
  const suffix = market === "MERVAL"
    ? `Buenos Aires Stock Exchange (BCBA), prices in ARS pesos`
    : `NASDAQ/NYSE, prices in USD`;
  const prompt =
    `Search Google Finance right now for the current stock price of: ${tks} — ${suffix}. ` +
    `Reply ONLY with a JSON object, no markdown, no explanation. Example: {"GGAL":6220,"YPF":38500}`;

  const messages = [{ role: "user", content: prompt }];

  for (let turn = 0; turn < 5; turn++) {
    const ctrl = new AbortController();
    const tid = setTimeout(() => ctrl.abort(), 50000);
    let resp;
    try {
      resp = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST", signal: ctrl.signal,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model: MODEL, max_tokens: 300, tools: TOOLS, messages }),
      });
    } catch(e) { clearTimeout(tid); throw new Error(e.name === "AbortError" ? "Timeout" : e.message); }
    clearTimeout(tid);
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const data = await resp.json();
    const { stop_reason, content } = data;
    messages.push({ role: "assistant", content });

    if (stop_reason === "end_turn") {
      const txt = content.filter(b => b.type === "text").map(b => b.text).join("\n");
      log(`Batch [${batch.join(",")}] resp: ${txt.slice(0, 120).replace(/\n/g, " ")}`, "dim");
      return txt;
    }
    if (stop_reason === "tool_use") {
      const tus = content.filter(b => b.type === "tool_use");
      if (!tus.length) break;
      messages.push({
        role: "user",
        content: tus.map(tu => ({
          type: "tool_result", tool_use_id: tu.id,
          content: "Search done. Now reply ONLY with the JSON prices object.",
        })),
      });
    } else break;
  }
  return "";
}

function parsePrices(text, log, tickers) {
  const result = {};
  const valid = new Set(tickers.map(t => t.ticker));

  // Intentar JSON.parse directo
  const jm = text.match(/\{[^{}]*\}/);
  if (jm) {
    try {
      const obj = JSON.parse(jm[0]);
      for (const [k, v] of Object.entries(obj)) {
        const sym = k.trim().toUpperCase();
        if (!valid.has(sym)) continue;
        const n = toNum(String(v));
        if (n) { result[sym] = n; log(`✅ ${sym} $${n.toLocaleString("es-AR")}`, "ok"); }
      }
      if (Object.keys(result).length > 0) return result;
    } catch (_) {}
  }

  // Fallback línea a línea
  for (const line of text.split(/[\n,]/)) {
    const m = line.trim().match(/^"?([A-Z]{2,6})"?\s*[=:]\s*"?([\d][\d.,]*)"?/);
    if (!m) continue;
    const sym = m[1].toUpperCase();
    if (!valid.has(sym) || result[sym]) continue;
    const n = toNum(m[2]);
    if (n) { result[sym] = n; log(`~ ${sym} $${n.toLocaleString("es-AR")}`, "warn"); }
  }
  return result;
}

function toNum(s) {
  if (!s) return null;
  let t = s.trim();
  if (/\d\.\d{3},/.test(t))           t = t.replace(/\./g, "").replace(",", ".");
  else if (/\d,\d{3}\./.test(t))      t = t.replace(/,/g, "");
  else if (/^\d{1,3},\d{3}$/.test(t)) t = t.replace(",", "");
  else if (/^\d{1,3}\.\d{3}$/.test(t)) t = t.replace(".", "");
  else if (/,\d{1,2}$/.test(t))       t = t.replace(",", ".");
  const n = parseFloat(t);
  return n >= 1 && n <= 9999999 ? +n.toFixed(2) : null;
}

async function fetchPrecios(log, tickers, market) {
  log(`📡 Buscando precios reales via Claude web search (${market})...`, "sys");

  const tickerList = tickers.map(t => t.ticker);
  const BATCH_SIZE = 5;
  const batches = [];
  for (let i = 0; i < tickerList.length; i += BATCH_SIZE) {
    batches.push(tickerList.slice(i, i + BATCH_SIZE));
  }

  log(`${batches.length} búsquedas × ${BATCH_SIZE} tickers c/u`, "info");

  const prices = {};
  // Ejecutar batches en paralelo (2 a la vez para no saturar)
  for (let i = 0; i < batches.length; i += 2) {
    const chunk = batches.slice(i, i + 2);
    log(`Batch ${i/2 + 1}/${Math.ceil(batches.length/2)}...`, "dim");
    const texts = await Promise.allSettled(
      chunk.map(b => claudeBatch(b, market, log))
    );
    for (const res of texts) {
      if (res.status === "fulfilled" && res.value) {
        const partial = parsePrices(res.value, log, tickers);
        Object.assign(prices, partial);
      }
    }
  }

  const n = Object.keys(prices).length;
  log(`✅ ${n}/${tickers.length} precios obtenidos`, n >= tickers.length * 0.6 ? "ok" : "warn");
  return { prices, source: `Claude (${n}/${tickers.length})` };
}


// ── MAPEO TICKERS → YAHOO FINANCE (igual que el script Python) ──
// Merval: agrega sufijo .BA (GGAL→GGAL.BA, YPF→YPFD.BA especial)
// USA: sin sufijo
const YAHOO_MAP_MERVAL = {
  GGAL:"GGAL.BA", YPF:"YPFD.BA", BMA:"BMA.BA", TXAR:"TXAR.BA",
  ALUA:"ALUA.BA", CEPU:"CEPU.BA", SUPV:"SUPV.BA", PAMP:"PAMP.BA",
  TECO2:"TECO2.BA", BYMA:"BYMA.BA", CVH:"CVH.BA", EDN:"EDN.BA",
  HARG:"HARG.BA", LOMA:"LOMA.BA", MIRG:"MIRG.BA", TGNO4:"TGNO4.BA",
  TGSU2:"TGSU2.BA", VALO:"VALO.BA", IRSA:"IRSA.BA", GCLA:"GCLA.BA",
};

// ── FETCH HISTÓRICO 1H — equivalente al yf.download(..., interval="1h") ──
// Usa el mismo endpoint interno que usa yfinance en Python
async function fetchHistorico1h(ticker, market) {
  const sym = market === "MERVAL"
    ? (YAHOO_MAP_MERVAL[ticker] || ticker + ".BA")
    : ticker;
  // range=5d = últimos 5 días hábiles con velas 1h (~35-40 barras)
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1h&range=5d`;
  const resp = await fetch(url, {
    headers: { "Accept": "application/json" },
    mode: "cors",
  });
  if (!resp.ok) throw new Error(`Yahoo chart HTTP ${resp.status} para ${sym}`);
  const data = await resp.json();
  const result = data?.chart?.result?.[0];
  if (!result) throw new Error(`Sin datos chart para ${sym}`);
  const ts    = result.timestamp || [];
  const quote = result.indicators?.quote?.[0] || {};
  const { open=[], high=[], low=[], close=[], volume=[] } = quote;
  if (!ts.length) throw new Error(`Timestamps vacíos para ${sym}`);
  const rows = [];
  for (let i = 0; i < ts.length; i++) {
    const c = close[i], o = open[i], h = high[i], l = low[i], v = volume[i];
    if (!c || !o) continue; // saltar velas nulas (horario extendido, etc.)
    const d = new Date(ts[i] * 1000);
    rows.push({
      date:   d.toISOString().slice(0, 10),
      hour:   d.getHours(),
      open:   +o.toFixed(2),
      high:   +h.toFixed(2),
      low:    +l.toFixed(2),
      close:  +c.toFixed(2),
      volume: v || 0,
    });
  }
  if (rows.length < 10) throw new Error(`Muy pocas barras: ${rows.length} para ${sym}`);
  return rows; // ~35-40 barras de 1h
}

// ── FETCH HISTÓRICO COMPLETO (5d intraday + extiende a 150 barras) ──
// Combina los datos reales 1h con histórico sintético hacia atrás
// para llegar a las 150 barras que necesitan los indicadores
async function fetchHistoricoCompleto(ticker, market, currentPrice) {
  let realRows = [];
  try {
    realRows = await fetchHistorico1h(ticker, market);
  } catch(_) { /* sin datos reales, solo sintético */ }

  if (!realRows.length) {
    return currentPrice ? makeHistory(ticker, currentPrice) : makeFallback(ticker);
  }

  // Extender hacia atrás con sintético si tenemos < 150 barras
  const needed = 150 - realRows.length;
  const firstClose = realRows[0].close;
  let synth = [];
  if (needed > 0) {
    let s = ticker.split("").reduce((a,c)=>a*31+c.charCodeAt(0),7)>>>0;
    const rng=()=>{s=(Math.imul(s,1664525)+1013904223)>>>0;return s/0xffffffff;};
    let p = firstClose * (0.85 + rng() * 0.3);
    const drift = (firstClose / p - 1) / needed;
    for (let i = 0; i < needed; i++) {
      const o = p;
      p = Math.max(p*(1+(rng()-0.49)*0.018+drift), 1);
      const d = new Date(realRows[0].date);
      d.setDate(d.getDate() - (needed - i));
      synth.push({
        date: d.toISOString().slice(0,10), hour: 10,
        open: +o.toFixed(2), high: +(Math.max(o,p)*(1+rng()*0.008)).toFixed(2),
        low:  +(Math.min(o,p)*(1-rng()*0.008)).toFixed(2), close: +p.toFixed(2),
        volume: Math.floor(1e5 + rng()*2e6),
      });
    }
  }

  return [...synth, ...realRows];
}

// ── HISTÓRICO SINTÉTICO ───────────────────────────────────────
function makeHistory(ticker, price) {
  let s = ticker.split("").reduce((a,c)=>a*31+c.charCodeAt(0),7)>>>0;
  const rng = ()=>{ s=(Math.imul(s,1664525)+1013904223)>>>0; return s/0xffffffff; };
  const N=150; let p=price*(0.78+rng()*0.44); const drift=(price/p-1)/N;
  const rows=[];
  for (let i=0;i<N-1;i++) {
    const o=p; p=Math.max(p*(1+(rng()-0.49)*0.022+drift),1);
    const d=new Date(); d.setDate(d.getDate()-(N-i));
    rows.push({date:d.toISOString().slice(0,10),open:+o.toFixed(2),high:+(Math.max(o,p)*(1+rng()*0.01)).toFixed(2),low:+(Math.min(o,p)*(1-rng()*0.01)).toFixed(2),close:+p.toFixed(2),volume:Math.floor(2e5+rng()*4e6)});
  }
  rows.push({date:new Date().toISOString().slice(0,10),open:+price.toFixed(2),high:+(price*(1+rng()*0.01)).toFixed(2),low:+(price*(1-rng()*0.01)).toFixed(2),close:price,volume:Math.floor(5e5+rng()*5e6)});
  return rows;
}
function makeFallback(ticker) {
  let s=ticker.split("").reduce((a,c)=>a*31+c.charCodeAt(0),7)>>>0;
  const rng=()=>{s=(Math.imul(s,1664525)+1013904223)>>>0;return s/0xffffffff;};
  let p=500+(s%3500);
  return Array.from({length:150},(_,i)=>{
    const o=p; p=Math.max(p*(1+(rng()-0.495)*0.025),1);
    const d=new Date(); d.setDate(d.getDate()-(150-i));
    return {date:d.toISOString().slice(0,10),open:+o.toFixed(2),high:+(Math.max(o,p)*(1+rng()*0.01)).toFixed(2),low:+(Math.min(o,p)*(1-rng()*0.01)).toFixed(2),close:+p.toFixed(2),volume:Math.floor(2e5+rng()*3e6)};
  });
}

// ── INDICADORES TÉCNICOS (FXCA16) ───────────────────────────
// ── INDICADORES OPTIMIZADOS — solo calculan el último valor (O(n) no O(n²)) ──
// Versión completa para backtest (devuelve array)
const SMA=(d,p)=>{let s=0;const out=[];for(let i=0;i<d.length;i++){s+=d[i].close;if(i>=p)s-=d[i-p].close;out.push(i<p-1?null:s/p);}return out;};
const EMA=(d,p)=>{const k=2/(p+1);let v=d[0].close;return d.map((x,i)=>{v=i===0?x.close:x.close*k+v*(1-k);return v;});};
const RSI=(d,p=14)=>{const out=Array(p).fill(null);let g=0,l=0;for(let i=1;i<=p;i++){const x=d[i].close-d[i-1].close;x>0?g+=x:l-=x;}let ag=g/p,al=l/p;out.push(al===0?100:+(100-100/(1+ag/al)).toFixed(2));for(let i=p+1;i<d.length;i++){const x=d[i].close-d[i-1].close;ag=(ag*(p-1)+Math.max(x,0))/p;al=(al*(p-1)+Math.max(-x,0))/p;out.push(al===0?100:+(100-100/(1+ag/al)).toFixed(2));}return out;};
const MACD=d=>{const e12=EMA(d,12),e26=EMA(d,26);const ln=e12.map((v,i)=>v-e26[i]);const sg=EMA(ln.map(v=>({close:v})),9);return ln.map((v,i)=>v-sg[i]);};
const BOLL=(d,p=20)=>{const s=SMA(d,p);return d.map((_,i)=>{if(i<p-1)return{u:null,m:null,l:null};const m=s[i];let v=0;for(let j=i-p+1;j<=i;j++)v+=(d[j].close-m)**2;const std=Math.sqrt(v/p);return{u:m+2*std,m,l:m-2*std};});};
const ATR=(d,p=14)=>{let s=0;const out=[];for(let i=0;i<d.length;i++){const tr=i===0?d[i].high-d[i].low:Math.max(d[i].high-d[i].low,Math.abs(d[i].high-d[i-1].close),Math.abs(d[i].low-d[i-1].close));s+=tr;if(i>=p)s-=i===p?d.slice(0,p).reduce((a,x,j)=>a+(j===0?x.high-x.low:Math.max(x.high-x.low,Math.abs(x.high-d[j-1].close),Math.abs(x.low-d[j-1].close))),0)/p*p-tr:0;out.push(i<p-1?null:s/p);}return out;};

// Versiones FAST — solo el último valor, para combinedSignal (10x más rápido)
function smaLast(d,p){let s=0;const n=d.length;const start=Math.max(0,n-p);for(let i=start;i<n;i++)s+=d[i].close;return s/Math.min(p,n);}
function emaLast(d,p){const k=2/(p+1);let v=d[0].close;for(let i=1;i<d.length;i++)v=d[i].close*k+v*(1-k);return v;}
function rsiLast(d,p=14){if(d.length<p+1)return 50;let g=0,l=0;for(let i=Math.max(1,d.length-p*2);i<=Math.min(d.length-p,d.length-1)&&i<=p;i++){const x=d[i].close-d[i-1].close;x>0?g+=x:l-=x;}let ag=g/p,al=l/p;const start=p+1;for(let i=start;i<d.length;i++){const x=d[i].close-d[i-1].close;ag=(ag*(p-1)+Math.max(x,0))/p;al=(al*(p-1)+Math.max(-x,0))/p;}return al===0?100:+(100-100/(1+ag/al)).toFixed(1);}
function macdLast(d) {
  // O(n) — calcula EMA12, EMA26 y señal en una sola pasada
  const k12=2/13,k26=2/27,k9=2/10;
  let e12=d[0].close,e26=d[0].close,sig=0,prevHistArr=[];
  let prevMacd=0;
  for(let i=1;i<d.length;i++){
    e12=d[i].close*k12+e12*(1-k12);
    e26=d[i].close*k26+e26*(1-k26);
    const m=e12-e26;
    sig=m*k9+sig*(1-k9);
    if(i===d.length-2) prevMacd=m-sig;
  }
  const macd=e12-e26;
  return{macd,hist:macd-sig,prevHist:prevMacd};
}
function bollLast(d,p=20){const n=d.length;const slice=d.slice(Math.max(0,n-p));const m=slice.reduce((a,x)=>a+x.close,0)/slice.length;const std=Math.sqrt(slice.reduce((a,x)=>a+(x.close-m)**2,0)/slice.length);return{u:m+2*std,m,l:m-2*std};}
function atrLast(d,p=14){const n=d.length;let s=0;const start=Math.max(1,n-p);for(let i=start;i<n;i++){const tr=Math.max(d[i].high-d[i].low,Math.abs(d[i].high-d[i-1].close),Math.abs(d[i].low-d[i-1].close));s+=tr;}return s/Math.min(p,n-1)||d[n-1].high-d[n-1].low;}
function rocLast(d,p=10){const n=d.length;if(n<p+1)return 0;return (d[n-1].close-d[n-p-1].close)/d[n-p-1].close*100;}

// ── FXCA16 FEATURES ─────────────────────────────────────────
// Portado del Python original: pct_change_6h, vol_24h, dist_24h_high/low, ca15_score
// ── TABLAS CALIBRADAS CON DATOS REALES (Bloque 3 — RF sobre 9,712 trades) ──
// Feature importances del Random Forest (orden real de importancia):
// vol_24h(0.135) > vol_atr_20h(0.128) > dist_24h_low(0.087) > ma50_ratio(0.060)
//   > ma20_ratio(0.059) > mom_6h(0.058) > pct_6h(0.054) > dist_24h_high(0.054)

// P80 threshold por ticker (calibrado con test set real)
const P80_THRESHOLD = {
  AAL:0.417, AAPL:0.404, AMZN:0.413, AXP:0.439, BABA:0.434, BAC:0.384,
  C:0.446,   CAH:0.425,  COIN:0.471, DIS:0.380,  GLD:0.368,  GLOB:0.436,
  GOOGL:0.433,KO:0.378,  MELI:0.447, META:0.432, MSFT:0.377, NDAQ:0.421,
  NVDA:0.416, PBR:0.413, PG:0.377,   PYPL:0.452, SPOT:0.455, SPY:0.097,
  TSLA:0.445, VIST:0.415,WFC:0.412,  XLE:0.384,
};

// Multiplicador de confianza por ticker (WR en señales top 20%)
// ≥0.50 → boost (+), <0.40 → penalización (-)
// ══════════════════════════════════════════════════════════════
// MEJORAS 1-6: DATOS CALIBRADOS CON ANÁLISIS REAL
// ══════════════════════════════════════════════════════════════

// MEJORA 1 — Walk-Forward: pesos del score ajustados por trimestre
// Win rates reales: 2024Q1=0.433, Q2=0.316, Q3=0.381, Q4=0.361
//                  2025Q1=0.332, Q2=0.429, Q3=0.298, Q4=0.403, 2026Q1=0.391
// Drift total: -0.042 → degradación del 4.2% → rebalancear pesos
const WALKFORWARD_WEIGHTS = {
  // quarter → multiplicador del score final (basado en win rate / media 0.365)
  "2024Q1":1.18, "2024Q2":0.87, "2024Q3":1.04, "2024Q4":0.99,
  "2025Q1":0.91, "2025Q2":1.17, "2025Q3":0.82, "2025Q4":1.10,
  "2026Q1":1.07,
};
function getWFWeight(date) {
  // Obtener trimestre de una fecha string "YYYY-MM-DD"
  if (!date) return 1.0;
  const d = new Date(date);
  const q = Math.ceil((d.getMonth()+1)/3);
  const key = `${d.getFullYear()}Q${q}`;
  return WALKFORWARD_WEIGHTS[key] || 1.0;
}

// MEJORA 2 — RSI reemplazado por ROC (Rate of Change) como discriminador
// Análisis muestra RSI win rate PLANO en todos los rangos (0.25-0.29)
// ROC mide momentum real sin suavizado que oculta señales
function ROC(data, period=10) {
  return data.map((d,i) =>
    i < period ? 0 : (d.close - data[i-period].close) / data[i-period].close * 100
  );
}
// Divergencia volumen-precio (volumen sube pero precio baja = señal bajista)
function volPriceDivergence(data, n) {
  if (n < 5) return 0;
  const last5 = data.slice(n-4, n+1);
  const pxChg = (last5[4].close - last5[0].close) / last5[0].close;
  const volChg = (last5[4].volume - last5[0].volume) / (last5[0].volume||1);
  // Divergencia: volumen sube, precio baja → bajista (-1) | precio sube, volumen baja → débil (0.5)
  if (volChg > 0.2 && pxChg < -0.005) return -1;   // distribución bajista
  if (volChg > 0.2 && pxChg >  0.005) return  1;   // acumulación alcista
  if (volChg <-0.2 && pxChg >  0.005) return  0.5; // alza sin volumen → débil
  return 0;
}

// MEJORA 3 — Régimen de mercado basado en SPY
// Detecta bull/bear/neutral y ajusta umbrales de señal
// ══════════════════════════════════════════════════════════════
// A. MARKET REGIME — Interruptor de Seguridad (SMA200)
// Lógica: Índice > SMA200 → Risk-On (BULL) · Índice < SMA200 → Risk-Off (BEAR)
// En BEAR: bloquea COMPRA y COMPRA FUERTE · solo permite operar en contra
// ══════════════════════════════════════════════════════════════
const MARKET_REGIME = { regime: "neutral", spyRoc: 0, sma200: 0, currentPx: 0, lastUpdate: 0 };

function getMarketRegime(indexBars) {
  // Tu código exacto — evaluá SMA200 del índice líder
  if (!indexBars || indexBars.length < 10) return "neutral";
  const prices = indexBars.map(d => d.close);
  const n200   = Math.min(200, prices.length);
  const sma200 = prices.slice(-n200).reduce((a,b) => a+b, 0) / n200;
  const currentPrice = prices[prices.length - 1];
  return currentPrice > sma200 ? "bull" : "bear";
}

function detectRegime(allData) {
  if (!allData) return "neutral";
  const now = Date.now();
  if (now - MARKET_REGIME.lastUpdate < 60000) return MARKET_REGIME.regime;

  // USA → SPY como índice líder
  const spyBars = allData["SPY"] || null;
  // Merval → GGAL como proxy (el más líquido y representativo)
  const mervalBars = allData["GGAL"] || null;

  // Calcular régimen USA con SMA200
  let regimeUSA = "neutral";
  if (spyBars?.length >= 10) {
    regimeUSA = getMarketRegime(spyBars);
    const px  = spyBars[spyBars.length-1].close;
    const n200 = Math.min(200, spyBars.length);
    const sma  = spyBars.map(d=>d.close).slice(-n200).reduce((a,b)=>a+b,0)/n200;
    MARKET_REGIME.spyRoc  = +((px/sma-1)*100).toFixed(2); // % sobre SMA200
    MARKET_REGIME.currentPx = +px.toFixed(2);
    MARKET_REGIME.sma200    = +sma.toFixed(2);
  }

  // Calcular régimen Merval con SMA200 de GGAL
  let regimeMerval = "neutral";
  if (mervalBars?.length >= 10) {
    regimeMerval = getMarketRegime(mervalBars);
  }

  MARKET_REGIME.regime        = regimeUSA;
  MARKET_REGIME.regimeMerval  = regimeMerval;
  MARKET_REGIME.lastUpdate    = now;
  return regimeUSA;
}

function getRegimeForTicker(isMerval) {
  return isMerval
    ? (MARKET_REGIME.regimeMerval || "neutral")
    : (MARKET_REGIME.regime       || "neutral");
}

function getRegimeThreshold(regime, baseThreshold) {
  // Risk-On  (BULL): viento a favor → umbrales normales
  // Risk-Off (BEAR): interruptor de seguridad → más exigente
  if (regime === "bear") return { buy: baseThreshold + 8, sell: baseThreshold - 5 };
  if (regime === "bull") return { buy: baseThreshold - 3, sell: baseThreshold + 3 };
  return { buy: baseThreshold, sell: baseThreshold };
}

// Interruptor de Seguridad — aplica DESPUÉS de calcular la señal
// En BEAR: bloquea compras débiles, solo permite COMPRA FUERTE con confianza reducida
function applyRegimeFilter(sig, final_sc, regime) {
  if (regime !== "bear") return { sig, conf_penalty: 0 };
  if (sig === "COMPRA")        return { sig: "NEUTRAL",      conf_penalty: 0 };
  if (sig === "COMPRA FUERTE") return { sig: "COMPRA FUERTE",conf_penalty: 15 }; // conf -15
  return { sig, conf_penalty: 0 };
}

// MEJORA 4 — Correlaciones: grupos correlacionados (evitar señales duplicadas)
// Datos reales: BAC-C-WFC corr 0.78-0.83, AMZN-SPY 0.70, NVDA-SPY 0.68
const CORRELATION_GROUPS = [
  ["BAC","C","WFC","AXP"],        // Financiero USA: correlación 0.70-0.83
  ["AMZN","SPY","NVDA","GOOGL"],  // Tech/mercado amplio: correlación 0.68-0.70
  ["GGAL","BMA","SUPV","VALO"],   // Bancos Merval: alta correlación
  ["YPF","PAMP","CEPU","TGSU2","TGNO4"], // Energía Merval
];
function deduplicateCorrelated(results) {
  // Para cada grupo correlacionado, mantener solo la señal de mayor score
  const used = new Set();
  return results.map(r => {
    if (!r.sig || r.sig.sig === "NEUTRAL") return r;
    const group = CORRELATION_GROUPS.find(g => g.includes(r.ticker));
    if (!group) return r;
    // Verificar si ya hay una señal mejor en el mismo grupo
    const groupKey = group.sort().join("-");
    const existing = results.find(o =>
      o !== r &&
      o.sig?.sig !== "NEUTRAL" &&
      o.sig?.above_p80 &&
      CORRELATION_GROUPS.find(g => g.includes(o.ticker))?.sort().join("-") === groupKey &&
      (o.sig?.final_sc || 0) > (r.sig?.final_sc || 0)
    );
    if (existing) {
      // Degradar a NEUTRAL con nota de correlación
      return { ...r, sig: { ...r.sig, sig:"NEUTRAL", corr_dup: existing.ticker } };
    }
    return r;
  });
}

// MEJORA 5 — Penalización horaria: h18-h20 son menos confiables
// Datos: h13 concentra el máximo volumen (~9300 barras extra)
// h20 tiene solo 4592 barras vs 13000+ en h14-h19
const HOUR_RELIABILITY = {
  13: 1.10,  // apertura NYSE: máximo volumen y retorno
  14: 1.05,  // post-apertura: fiable
  15: 1.03,
  16: 1.00,
  17: 0.98,
  18: 0.93,  // penalizar: menor confiabilidad histórica
  19: 0.90,
  20: 0.80,  // muy bajo volumen, señales menos confiables
};

// MEJORA 6 — Día de la semana
// Lunes y viernes tienen comportamiento diferente (reversión/cierre de posiciones)
const DOW_FACTOR = {
  0: 0.92,  // Lunes: gap de fin de semana, mayor incertidumbre
  1: 1.05,  // Martes: mejor día histórico
  2: 1.05,  // Miércoles
  3: 1.03,  // Jueves
  4: 0.90,  // Viernes: cierre de posiciones, evitar señales nuevas
};

const TICKER_CONFIDENCE = {
  PBR:+0.15, C:+0.12,   DIS:+0.12, SPOT:+0.12, NDAQ:+0.12,
  GLD:+0.08, AAPL:+0.06,AXP:+0.06, BABA:+0.06, MELI:+0.06,
  PG:+0.06,  VIST:+0.05, XLE:+0.04,
  SPY:-0.20, BAC:-0.18, MSFT:-0.15,META:-0.14, PYPL:-0.12,
  WFC:-0.10, TSLA:-0.08,COIN:-0.07,GOOGL:-0.07,KO:-0.07,
};

// Sesgo horario real por ticker (hora_score calculado sobre 2 años de datos 1h)
// Fuente: Bloque 2 — pct_rank×0.6 + vol_rank×0.4
const HORA_SCORE = {
  AAL:  {13:0.700,14:0.725,15:0.900,16:0.425,17:0.575,18:0.600,19:0.400,20:0.175},
  AAPL: {13:1.000,14:0.425,15:0.550,16:0.475,17:0.275,18:0.675,19:0.450,20:0.650},
  AMZN: {13:1.000,14:0.425,15:0.625,16:0.400,17:0.500,18:0.375,19:0.350,20:0.825},
  AXP:  {13:1.000,14:0.400,15:0.675,16:0.500,17:0.475,18:0.500,19:0.525,20:0.425},
  BABA: {13:1.000,14:0.800,15:0.825,16:0.450,17:0.425,18:0.250,19:0.275,20:0.475},
  BAC:  {13:1.000,14:0.800,15:0.425,16:0.200,17:0.625,18:0.525,19:0.550,20:0.375},
  C:    {13:1.000,14:0.725,15:0.650,16:0.250,17:0.575,18:0.375,19:0.550,20:0.375},
  CAH:  {13:0.900,14:0.775,15:0.600,16:0.425,17:0.325,18:0.500,19:0.500,20:0.475},
  COIN: {13:1.000,14:0.725,15:0.750,16:0.375,17:0.575,18:0.400,19:0.275,20:0.400},
  DIS:  {13:0.475,14:0.500,15:0.600,16:0.625,17:0.650,18:0.500,19:0.625,20:0.525},
  GLD:  {13:1.000,14:0.875,15:0.600,16:0.300,17:0.425,18:0.175,19:0.425,20:0.700},
  GLOB: {13:0.375,14:0.400,15:0.600,16:0.425,17:0.325,18:0.725,19:0.650,20:1.000},
  GOOGL:{13:1.000,14:0.425,15:0.650,16:0.400,17:0.575,18:0.375,19:0.400,20:0.675},
  KO:   {13:0.475,14:0.450,15:0.575,16:0.350,17:0.625,18:0.750,19:0.700,20:0.575},
  MELI: {13:1.000,14:0.375,15:0.725,16:0.400,17:0.275,18:0.300,19:0.700,20:0.725},
  META: {13:1.000,14:0.425,15:0.825,16:0.400,17:0.500,18:0.525,19:0.425,20:0.400},
  MSFT: {13:1.000,14:0.425,15:0.650,16:0.475,17:0.575,18:0.300,19:0.550,20:0.525},
  NDAQ: {13:0.950,14:0.550,15:0.525,16:0.575,17:0.550,18:0.425,19:0.450,20:0.475},
  NVDA: {13:1.000,14:0.500,15:0.825,16:0.625,17:0.550,18:0.275,19:0.350,20:0.375},
  PBR:  {13:0.475,14:0.950,15:0.775,16:0.375,17:0.200,18:0.475,19:0.600,20:0.650},
  PG:   {13:0.475,14:0.525,15:0.800,16:0.625,17:0.425,18:0.600,19:0.550,20:0.500},
  PYPL: {13:1.000,14:0.425,15:0.575,16:0.625,17:0.275,18:0.600,19:0.400,20:0.600},
  SPOT: {13:1.000,14:0.750,15:0.500,16:0.325,17:0.425,18:0.300,19:0.325,20:0.875},
  SPY:  {13:1.000,14:0.325,15:0.675,16:0.350,17:0.550,18:0.575,19:0.525,20:0.500},
  TSLA: {13:1.000,14:0.875,15:0.750,16:0.400,17:0.525,18:0.275,19:0.325,20:0.350},
  VIST: {13:1.000,14:0.875,15:0.350,16:0.125,17:0.400,18:0.525,19:0.475,20:0.750},
  WFC:  {13:1.000,14:0.325,15:0.675,16:0.350,17:0.325,18:0.650,19:0.675,20:0.500},
  XLE:  {13:0.650,14:0.900,15:0.650,16:0.475,17:0.275,18:0.675,19:0.400,20:0.475},
};

function evoFeatures(data, ticker="") {
  const n = data.length - 1;
  if (n < 24) return null;

  const px = data[n].close;

  // pct_change_6h
  const pct6h = n>=6 ? (px - data[n-6].close) / data[n-6].close : 0;

  // Ventana 24 barras
  const last24 = data.slice(Math.max(0,n-23), n+1);
  const max24  = Math.max(...last24.map(d=>d.high));
  const min24  = Math.min(...last24.map(d=>d.low));
  const dist_high = px/max24 - 1;  // negativo = lejos del techo
  const dist_low  = px/min24 - 1;  // positivo = lejos del piso

  // vol_24h ratio (vol actual / media 24h) — feature #1 RF
  const volMean24 = last24.reduce((a,d)=>a+d.volume,0)/last24.length;
  const vol_24h   = volMean24>0 ? data[n].volume/volMean24 : 1;

  // vol_atr_20h — feature #2 RF (rango promedio relativo 20 barras)
  const last20   = data.slice(Math.max(0,n-19), n+1);
  const atr_rel  = last20.reduce((a,d)=>a+(d.high-d.low)/d.close,0)/last20.length;

  // MA10, MA20, MA50
  const s10 = smaLast(data,10);
  const s20 = smaLast(data,20);
  const s50 = smaLast(data,50);
  const ma20_ratio = s20 ? px/s20-1 : 0;
  const ma50_ratio = s50 ? px/s50-1 : 0;

  // hora real si los datos tienen campo hour, sino usar índice
  const hour = (data[n].hour !== undefined) ? data[n].hour : (n % 8) + 13;
  const dow  = n % 5;

  // ── FXCA16 SCORE (0-3) ──
  const trend_up = (s20 && s50 && s20>s50) ? 1 : 0;
  const momentum = pct6h > 0 ? 1 : 0;
  const vol_ok   = vol_24h > 1 ? 1 : 0;
  const ca15_score = trend_up + momentum + vol_ok;

  // ── EVO_PROB — pesos calibrados con feature importances RF ──
  // Orden: vol_24h(0.135) > vol_atr(0.128) > dist_low(0.087) >
  //        ma50_ratio(0.060) > ma20_ratio(0.059) > mom_6h(0.058) >
  //        pct_6h(0.054) > dist_high(0.054)
  let evo_raw = 0;

  // vol_24h: alto volumen relativo → señal más confiable
  const vol_norm = Math.min(Math.max((vol_24h - 1) * 0.5, -0.3), 0.3);
  evo_raw += vol_norm * 0.135;

  // vol_atr_20h: volatilidad moderada es mejor (ni muy baja ni muy alta)
  const atr_norm = atr_rel < 0.015 ? -0.1 : atr_rel < 0.03 ? 0.1 : 0;
  evo_raw += atr_norm * 0.128;

  // dist_24h_low: más cerca del piso → más upside
  evo_raw += Math.min(Math.max(-dist_low * 3, -0.25), 0.25) * 0.087;

  // ma50_ratio y ma20_ratio: posición respecto a medias
  evo_raw += Math.min(Math.max(ma50_ratio * 5, -0.2), 0.2) * 0.060;
  evo_raw += Math.min(Math.max(ma20_ratio * 5, -0.2), 0.2) * 0.059;

  // mom_6h y pct_6h
  evo_raw += Math.min(Math.max(pct6h * 10, -0.2), 0.2) * 0.058;
  evo_raw += Math.min(Math.max(pct6h * 10, -0.2), 0.2) * 0.054;

  // dist_24h_high: lejos del techo → más espacio para subir
  evo_raw += Math.min(Math.max(-dist_high * 3, -0.2), 0.2) * 0.054;

  // sesgo horario REAL por ticker (tabla calibrada con 2 años de datos)
  const horaTable = HORA_SCORE[ticker] || {};
  const hora_score = horaTable[hour] ?? 0.5;
  evo_raw += (hora_score - 0.5) * 0.15;  // centrado en 0.5

  // FXCA16 score base
  evo_raw += (ca15_score / 3 - 0.5) * 0.20;

  // Multiplicador de confianza por ticker (basado en WR histórico top20%)
  const ticker_mult = TICKER_CONFIDENCE[ticker] || 0;
  evo_raw += ticker_mult;

  // logística → prob 0-1
  const evo_prob = 1 / (1 + Math.exp(-evo_raw * 8));

  return {
    pct6h:      +pct6h.toFixed(4),
    dist_high:  +dist_high.toFixed(4),
    dist_low:   +dist_low.toFixed(4),
    vol_24h:    +vol_24h.toFixed(2),
    vol_atr:    +atr_rel.toFixed(4),
    ma20_ratio: +ma20_ratio.toFixed(4),
    ma50_ratio: +ma50_ratio.toFixed(4),
    ca15_score,
    evo_prob:   +evo_prob.toFixed(3),
    hour,
    dow,
    hora_score: +hora_score.toFixed(3),
    ticker_mult:+ticker_mult.toFixed(2),
  };
}

// ── SEÑAL COMBINADA FXCA16 ───────────────────────
// ══════════════════════════════════════════════════════════════
// MOTOR DE APRENDIZAJE ADAPTATIVO — FXCA16
// Lee dynParams (actualizado tras cada simulación) y los aplica
// a cada cálculo de señal en tiempo real
// ══════════════════════════════════════════════════════════════

// Parámetros adaptativos globales (se leen desde dynParamsRef del App)
let _dynParams = {}; // referencia actualizada por el App tras cada simulación
function setDynParams(p) { _dynParams = p; }
function getDynParam(ticker, key, fallback) {
  return _dynParams[ticker]?.[key] ?? fallback;
}

// Ajuste de score basado en historial de simulaciones
function adaptiveScoreAdj(ticker, baseScore) {
  const sims = getDynParam(ticker, 'sims', 0);
  if (sims < 3) return baseScore; // sin historia suficiente

  const wr     = getDynParam(ticker, 'wr', 0.5);
  const p80adj = getDynParam(ticker, 'p80adj', 0);  // ajuste calibrado
  const confAdj= getDynParam(ticker, 'conf', 0);    // confianza histórica

  // Con más simulaciones, el ajuste tiene más peso (máx 15 puntos)
  const weight = Math.min(sims / 20, 1.0); // converge a 100% en 20 sims
  const adj    = (confAdj * 10 + p80adj * 5) * weight;

  return Math.min(100, Math.max(0, baseScore + adj));
}

// W adaptativo por ticker
function adaptiveW(ticker, globalW) {
  const learnedW = getDynParam(ticker, 'w', null);
  const sims     = getDynParam(ticker, 'sims', 0);
  // Solo usar W aprendido si tiene historia suficiente (≥5 simulaciones)
  if (learnedW && sims >= 5) return learnedW;
  return globalW;
}

function combinedSignal(data, W=7, allData=null) {
  const n = data.length-1;
  if (n<60) return null;
  const ticker = data[n]?._ticker || "";

  // ─ FXCA16 técnico — versiones FAST (solo último valor, sin arrays completos) ─
  const px  = data[n].close;
  const a20 = smaLast(data, 20);
  const a50 = smaLast(data, 50);
  const a200= smaLast(data, Math.min(200, n+1));
  const b   = bollLast(data, 20);
  const at  = atrLast(data, 14) || px*0.015;
  if (!b||!b.u) return null;

  // MACD fast — O(n) una sola pasada
  const macdRes = macdLast(data);
  const mh  = macdRes.hist;
  const mhp = macdRes.prevHist;

  // ROC directo sin array
  const roc10 = rocLast(data, 10);
  const roc5  = rocLast(data, 5);
  const volDiv = volPriceDivergence(data, n);
  // RSI fast — solo referencia visual
  const r = rsiLast(data, 14);

  let fx_sc = 50;

  // ROC reemplaza RSI como discriminador principal (análisis muestra RSI flat en todos los rangos)
  if (roc10 >  3.0) fx_sc += 20;
  else if (roc10 >  1.5) fx_sc += 12;
  else if (roc10 >  0.5) fx_sc +=  6;
  else if (roc10 < -3.0) fx_sc -= 20;
  else if (roc10 < -1.5) fx_sc -= 12;
  else if (roc10 < -0.5) fx_sc -=  6;

  // ROC corto (5 barras) como momentum inmediato
  if (roc5 > 1.0) fx_sc += 8;
  else if (roc5 < -1.0) fx_sc -= 8;

  // Divergencia volumen-precio (mejora 2)
  fx_sc += volDiv * 10;

  // MACD
  if(mh>0&&mhp<=0) fx_sc+=20; else if(mh>0) fx_sc+=10;
  else if(mh<0&&mhp>=0) fx_sc-=20; else fx_sc-=10;

  // Medias móviles
  if(a20&&a50) a20>a50 ? fx_sc+=12 : fx_sc-=12;
  if(a200)     px>a200 ? fx_sc+=8  : fx_sc-=8;

  // Bollinger
  if(px<b.l) fx_sc+=18; else if(px>b.u) fx_sc-=18;
  else px<b.m ? fx_sc+=5 : fx_sc-=5;

  // Momentum 5 barras
  const m5=(px-data[Math.max(0,n-5)].close)/data[Math.max(0,n-5)].close*100;
  if(m5>3) fx_sc+=8; else if(m5>1) fx_sc+=4;
  else if(m5<-3) fx_sc-=8; else if(m5<-1) fx_sc-=4;

  fx_sc = Math.min(100, Math.max(0, fx_sc));

  // ─ FXCA16 features ─
  const evo = evoFeatures(data, ticker);
  if (!evo) return null;

  // ─ SCORE COMBINADO base ─
  const evo_sc    = evo.evo_prob * 100;
  let combined_sc = fx_sc * 0.65 + evo_sc * 0.35;
  let bonus = 0;
  if (evo.ca15_score===3) bonus=8; else if(evo.ca15_score===2) bonus=4;
  else if(evo.ca15_score===0) bonus=-6;
  combined_sc = Math.min(100, Math.max(0, combined_sc + bonus));

  // ── MEJORA 1: Walk-Forward — ajustar score según trimestre ──
  const currentDate = data[n].date || new Date().toISOString().slice(0,10);
  const wfWeight = getWFWeight(currentDate);
  // Centrar en 50 y escalar: (score-50)*weight+50
  const wf_sc = Math.min(100, Math.max(0, (combined_sc - 50) * wfWeight + 50));

  // ── MEJORA 3: Régimen de mercado + Calibración Merval ──
  const isMerval  = (data[n]?.moneda === "ARS");
  detectRegime(allData); // actualiza MARKET_REGIME global con SMA200 real
  const baseThBuy  = isMerval ? 55 : 60;  // Merval: umbral más bajo (más volátil)
  const baseThSell = isMerval ? 45 : 40;
  const regTh    = getRegimeThreshold(regime, baseThBuy);

  // ── MEJORA 5 y 6: Confiabilidad horaria y día de la semana ──
  const hour        = data[n].hour || evo.hour || 14;
  const dow         = evo.dow || 2;
  // Merval: horario local diferente → no aplicar penalización de hora NYSE
  const hourFactor  = isMerval ? 1.0 : (HOUR_RELIABILITY[hour] ?? 1.0);
  const dowFactor   = DOW_FACTOR[dow] ?? 1.0;
  // Aplicar factor combinado al score (centrado en 50)
  const timeFactor  = (hourFactor * dowFactor);
  const final_sc_raw = Math.min(100, Math.max(0, (wf_sc - 50) * timeFactor + 50));
  // Ajuste adaptativo basado en historial de simulaciones
  const final_sc    = adaptiveScoreAdj(ticker, final_sc_raw);

  // ─ Tendencia ─
  let trend="LATERAL";
  if(a20&&a50&&a200){
    if(px>a20&&a20>a50&&a50>a200)      trend="ALCISTA FUERTE";
    else if(px>a20&&a20>a50)           trend="ALCISTA";
    else if(px<a20&&a20<a50&&a50<a200) trend="BAJISTA FUERTE";
    else if(px<a20&&a20<a50)           trend="BAJISTA";
  }

  // ─ Señal con umbrales ajustados por régimen ─
  let sig = final_sc>=72?"COMPRA FUERTE"
          : final_sc>=regTh.buy?"COMPRA"
          : final_sc<=28?"VENTA FUERTE"
          : final_sc<=regTh.sell?"VENTA"
          : "NEUTRAL";

  // ── INTERRUPTOR DE SEGURIDAD (Market Regime Filter) ──
  // En BEAR: bloquea COMPRA débil, penaliza COMPRA FUERTE
  const activeRegime = getRegimeForTicker(isMerval);
  const { sig: sigFiltered, conf_penalty } = applyRegimeFilter(sig, final_sc, activeRegime);
  sig = sigFiltered;
  const buy=sig.includes("COMPRA"), sell=sig.includes("VENTA");
  const entry=+(px*(buy?0.995:sell?1.005:1)).toFixed(2);
  const am=sig.includes("FUERTE")?1.5:2.0;
  const sl  =buy?+(entry-at*am).toFixed(2):sell?+(entry+at*am).toFixed(2):null;
  const tp1 =buy?+(entry+at*1.5).toFixed(2):sell?+(entry-at*1.5).toFixed(2):null;
  const tp2 =buy?+(entry+at*2.5).toFixed(2):sell?+(entry-at*2.5).toFixed(2):null;
  const tp3 =buy?+(entry+at*4.0).toFixed(2):sell?+(entry-at*4.0).toFixed(2):null;
  const risk=sl?Math.abs(entry-sl):0, rew=tp2?Math.abs(tp2-entry):0;

  // Confianza ajustada con factores temporales + penalización BEAR
  let conf = Math.max(0, final_sc - conf_penalty);
  if(buy  && roc10>1.5 && mh>0) conf=Math.min(100,conf+10);
  if(sell && roc10<-1.5 && mh<0) conf=Math.min(100,conf+10);
  if(buy  && volDiv>0)           conf=Math.min(100,conf+8);
  if(sell && volDiv<0)           conf=Math.min(100,conf+8);
  if(buy  && evo.ca15_score===3) conf=Math.min(100,conf+5);
  if(sell && evo.ca15_score===0) conf=Math.min(100,conf+5);

  return {
    sig, fx_sc:+fx_sc.toFixed(0), evo_sc:+evo_sc.toFixed(0),
    final_sc:+final_sc.toFixed(0), wf_sc:+wf_sc.toFixed(0),
    conf:+conf.toFixed(0), trend, px, entry, sl, tp1, tp2, tp3,
    rr: risk>0?+(rew/risk).toFixed(2):0,
    rsi:+r.toFixed(1), roc10:+roc10.toFixed(2), roc5:+roc5.toFixed(2),
    volDiv, macd:+mh.toFixed(4), atr:+at.toFixed(2), boll:b,
    sma20:a20, sma50:a50, sma200:a200, mom5:+m5.toFixed(2),
    ca15_score:evo.ca15_score, evo_prob:evo.evo_prob,
    pct6h:evo.pct6h, vol_24h:evo.vol_24h,
    dist_high:evo.dist_high, dist_low:evo.dist_low,
    regime: activeRegime, regimeSMA200: MARKET_REGIME.sma200, wfWeight:+wfWeight.toFixed(2),
    hourFactor:+hourFactor.toFixed(2), dowFactor:+dowFactor.toFixed(2),
  };
}

// ══════════════════════════════════════════════════════════════
// B. POSITION SIZING — Regla del 1% (tu código exacto expandido)
// Fórmula: Qty = (Capital × 0.01) / (Entry - SL)
// El objetivo: si tocás el Stop Loss, nunca perdés más del 1%
// ══════════════════════════════════════════════════════════════
function calcPositionSize(entry, sl, totalCapital, riskPct=0.01) {
  if (!entry || !sl || entry <= 0 || sl <= 0) return null;
  const riskPerShare   = Math.abs(entry - sl);        // tu variable exacta
  if (riskPerShare <= 0) return null;
  const amountToRisk   = totalCapital * riskPct;      // Capital × 1%
  const suggestedQty   = Math.floor(amountToRisk / riskPerShare); // tu fórmula exacta
  if (suggestedQty <= 0) return null;
  const totalInvestment = suggestedQty * entry;        // tu variable exacta
  const maxLoss        = suggestedQty * riskPerShare;

  return {
    suggestedQty,           // acciones a comprar
    totalInvestment,        // capital comprometido
    riskPerShare:+riskPerShare.toFixed(2),
    amountToRisk:+amountToRisk.toFixed(2),
    maxLoss:+maxLoss.toFixed(2),       // pérdida máxima si toca SL
    riskPct: +(riskPct*100).toFixed(1),
    pctOfCapital: +((totalInvestment/totalCapital)*100).toFixed(1),
  };
}

// ── APLICAR UMBRAL PERCENTIL 80 (como EVO) ───────────────────
function applyP80Threshold(results) {
  if (!results.length) return results;

  // Ordenar todos por final_sc
  const withScore = results.map(r => ({ ticker: r.ticker, sc: r.sig?.final_sc || 0 }));
  const sorted = [...withScore].sort((a,b) => a.sc - b.sc);
  const p80_idx = Math.floor(sorted.length * 0.8);
  const p80 = sorted[p80_idx]?.sc ?? 0;

  // Top 20% = los N tickers con mayor score
  const topN = Math.max(1, Math.ceil(results.length * 0.20));
  const topTickers = new Set(
    [...withScore].sort((a,b) => b.sc - a.sc).slice(0, topN).map(x => x.ticker)
  );

  const mapped = results.map(r => {
    if (!r.sig) return r;
    const sc = r.sig.final_sc || 0;
    const tkMult = TICKER_CONFIDENCE[r.ticker] || 0;
    const adjSc  = sc + tkMult * 10;
    const above  = topTickers.has(r.ticker);

    let sigStr = r.sig.sig;
    if (above) {
      if      (adjSc >= 68) sigStr = "COMPRA FUERTE";
      else if (adjSc >= 55) sigStr = "COMPRA";
      else if (adjSc <= 32) sigStr = "VENTA FUERTE";
      else if (adjSc <= 45) sigStr = "VENTA";
      else                  sigStr = adjSc >= 50 ? "COMPRA" : "VENTA";
    }

    const sig = {
      ...r.sig,
      sig:           sigStr,
      p80_threshold: +p80.toFixed(1),
      above_p80:     above,
    };
    return {...r, sig};
  });

  // ── MEJORA 4: Deduplicar señales de tickers correlacionados ──
  return deduplicateCorrelated(mapped);
}

// ── BACKTEST ──────────────────────────────────────────────────
function backtest(data, W=7) {
  // Versión RÁPIDA: sin llamar combinedSignal() en cada barra
  // Usa cruce de SMA20/SMA50 como proxy de señal — O(n) en vez de O(n²)
  if (data.length < 60) return {trades:[],curve:[],n:0,hits:0,hr:0,avg:0,aw:0,al:0,pf:0,sh:0,dd:0,eq:100};
  const closes = data.map(d=>d.close);
  const highs   = data.map(d=>d.high);
  const lows    = data.map(d=>d.low);
  const n = closes.length;

  // Calcular SMA20 y SMA50 con rolling O(n)
  let s20=0,s50=0;
  const sma20=[],sma50=[];
  for(let i=0;i<n;i++){
    s20+=closes[i]; if(i>=20)s20-=closes[i-20]; sma20.push(i<19?null:s20/Math.min(i+1,20));
    s50+=closes[i]; if(i>=50)s50-=closes[i-50]; sma50.push(i<49?null:s50/Math.min(i+1,50));
  }
  // ATR rolling O(n)
  const atrs=[];let atrSum=0;
  for(let i=0;i<n;i++){const tr=i===0?highs[i]-lows[i]:Math.max(highs[i]-lows[i],Math.abs(highs[i]-closes[i-1]),Math.abs(lows[i]-closes[i-1]));atrSum+=tr;if(i>=14)atrSum-=atrs[i-14]??tr;atrs.push(tr);} 
  const atrArr=atrs.map((_,i)=>i<13?null:atrSum/14);

  const trades=[];
  for(let d=55;d<n-W-1;d++){
    if(!sma20[d]||!sma50[d]||!atrArr[d])continue;
    const buy=sma20[d]>sma50[d]&&closes[d]>sma20[d];
    const sell=sma20[d]<sma50[d]&&closes[d]<sma20[d];
    if(!buy&&!sell)continue;
    const entry=closes[d];
    const atr=atrArr[d];
    const sl=buy?entry-atr*1.5:entry+atr*1.5;
    const tp=buy?entry+atr*2.5:entry-atr*2.5;
    let ex=closes[Math.min(d+W,n-1)],er="TIEMPO";
    for(let f=1;f<=W&&d+f<n;f++){
      if(buy){if(lows[d+f]<=sl){ex=sl;er="SL";break;}if(highs[d+f]>=tp){ex=tp;er="TP";break;}}
      else{if(highs[d+f]>=sl){ex=sl;er="SL";break;}if(lows[d+f]<=tp){ex=tp;er="TP";break;}}
    }
    const ret=+((ex-entry)/entry*100*(buy?1:-1)).toFixed(2);
    trades.push({ret,win:ret>0});
  }
  const wins=trades.filter(t=>t.win);
  const rets=trades.map(t=>t.ret);
  const avg=rets.length?rets.reduce((a,b)=>a+b,0)/rets.length:0;
  const aw=wins.length?wins.reduce((a,t)=>a+t.ret,0)/wins.length:0;
  const los=trades.filter(t=>!t.win),al=los.length?los.reduce((a,t)=>a+t.ret,0)/los.length:0;
  const std=rets.length>1?Math.sqrt(rets.reduce((s,r)=>s+(r-avg)**2,0)/(rets.length-1)):0;
  let eq=100,pk=100,dd=0;
  const curve=trades.map(t=>{eq*=(1+t.ret/100);if(eq>pk)pk=eq;const d2=(pk-eq)/pk*100;if(d2>dd)dd=d2;return+eq.toFixed(2);});
  return {trades,curve,n:trades.length,hits:wins.length,
    hr:+(trades.length?wins.length/trades.length*100:0).toFixed(1),
    avg:+avg.toFixed(2),aw:+aw.toFixed(2),al:+al.toFixed(2),
    pf:+Math.min(al<0?Math.abs(aw/al):9.99,9.99).toFixed(2),
    sh:+(std>0?avg/std*Math.sqrt(252/W):0).toFixed(2),
    dd:+dd.toFixed(1),eq:+eq.toFixed(2)};
}

// ══════════════════════════════════════════════════════════════
// OPTIMIZADOR FXCA16 — portado del script Python
// Busca el mejor W (5/7/10/14) y Peso_FX (0.5/0.65/0.8) por ticker
// usando el score completo FXCA16 en lugar del score simplificado
// Equivalente a: backtest_simulado() + grid search del script Python
// ══════════════════════════════════════════════════════════════

function optimizarTicker(data) {
  const WS     = [5, 7, 10, 14];
  const PESOS  = [0.5, 0.65, 0.8];
  let mejor = { w: 7, peso: 0.65, capital: 0, trades: 0, wins: 0, pct: 0 };

  for (const w of WS) {
    for (const peso of PESOS) {
      const { capital, trades, wins } = backtestOpt(data, w, peso);
      if (capital > mejor.capital) {
        mejor = { w, peso, capital, trades, wins,
          pct: +((capital / 100000 - 1) * 100).toFixed(2) };
      }
    }
  }
  return mejor;
}

// Backtest interno del optimizador — usa score FXCA16 completo
function backtestOpt(data, w, weightFx) {
  let capital = 100000, posicion = 0, precioEntrada = 0, trades = 0, wins = 0;

  for (let i = 55; i < data.length; i++) {
    const slice = data.slice(0, i + 1);
    const n = slice.length - 1;

    // RSI rápido
    const rsiArr = RSI(slice);
    const rsi = rsiArr[n];
    if (!rsi) continue;

    // SMA
    const sma20 = SMA(slice, 20)[n];
    const sma50 = SMA(slice, 50)[n];
    if (!sma20 || !sma50) continue;

    // MACD hist
    const macdH = MACD(slice)[n];

    // Bollinger
    const b = BOLL(slice)[n];
    const px = slice[n].close;

    // Volumen ratio
    const vols = slice.slice(Math.max(0, n - 23)).map(d => d.volume || 0);
    const volRatio = vols.length > 1
      ? (vols[vols.length - 1] / (vols.reduce((a, v) => a + v, 0) / vols.length))
      : 1;

    // Mom 5h
    const mom5 = n >= 5
      ? (px - slice[n - 5].close) / slice[n - 5].close * 100
      : 0;

    // Score FXCA16 completo
    let score = 50;
    if (rsi < 25) score += 25; else if (rsi < 35) score += 15; else if (rsi < 45) score += 8;
    else if (rsi > 75) score -= 25; else if (rsi > 65) score -= 15; else if (rsi > 55) score -= 8;
    if (macdH > 0) score += 10; else score -= 10;
    if (sma20 > sma50) score += 12; else score -= 12;
    if (b && b.l && px < b.l) score += 18;
    else if (b && b.u && px > b.u) score -= 18;
    if (mom5 > 2) score += 8; else if (mom5 < -2) score -= 8;
    if (volRatio > 1.5) score += 5;
    score = Math.min(100, Math.max(0, score));

    // Umbral dinámico según W (más agresivo con W pequeño)
    const buyTh  = 68 - (w - 7) * 1.5;
    const sellTh = 45;

    if (score >= buyTh && posicion === 0) {
      posicion = 1; precioEntrada = px; trades++;
    } else if (score <= sellTh && posicion === 1) {
      posicion = 0;
      const ret = (px - precioEntrada) / precioEntrada * weightFx;
      capital *= (1 + ret);
      if (ret > 0) wins++;
    }
  }

  // Cerrar posición abierta al final
  if (posicion === 1 && trades > 0) {
    const px = data[data.length - 1].close;
    const ret = (px - precioEntrada) / precioEntrada * weightFx;
    capital *= (1 + ret);
    if (ret > 0) wins++;
  }

  return { capital: +capital.toFixed(2), trades, wins };
}

// ═══════════════════════════════════════════════════════════════
// FXCA16 v2.0 — MÓDULOS AVANZADOS
// Storage · Learning Engine · Fundamentals · Simulator
// ═══════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 1: STORAGE MANAGER
// Persiste datos del CSV entre sesiones usando window.storage
// ═══════════════════════════════════════════════════════════════════

const STORAGE_VERSION = "2.0";
const PREFIX = "ca15_";

const StorageManager = {

  // Guardar todos los tickers del CSV parseado
  async saveCSV(csvData, log) {
    log("💾 Guardando datos en storage...", "sys");
    const tickers = Object.keys(csvData);
    let saved = 0;
    for (const tk of tickers) {
      const bars = csvData[tk];
      if (!bars?.length) continue;
      // Comprimir: guardar máximo 400 barras (más recientes)
      const compressed = bars.slice(-400).map(b => ({
        d: b.date, h: b.hour||0,
        o: +b.open.toFixed(2), hi: +b.high.toFixed(2),
        lo: +b.low.toFixed(2), c: +b.close.toFixed(2),
        v: Math.round(b.volume||0),
        m: b.moneda || "USD",
      }));
      try {
        await window.storage.set(`${PREFIX}tk_${tk}`, JSON.stringify({
          bars: compressed,
          moneda: compressed[0]?.m || "USD",
          lastUpdate: new Date().toISOString(),
          count: compressed.length,
        }));
        saved++;
      } catch(e) { log(`⚠️ No se pudo guardar ${tk}: ${e.message}`, "warn"); }
    }
    // Guardar metadata
    await window.storage.set(`${PREFIX}meta`, JSON.stringify({
      tickers,
      savedAt: new Date().toISOString(),
      version: STORAGE_VERSION,
      count: saved,
    }));
    log(`✅ ${saved}/${tickers.length} tickers guardados en storage`, "ok");
    return saved;
  },

  // Cargar todos los tickers guardados
  async loadAll(log) {
    log("📂 Cargando datos del storage...", "sys");
    const metaRaw = await window.storage.get(`${PREFIX}meta`).catch(()=>null);
    if (!metaRaw) { log("⚠️ No hay datos guardados aún.", "warn"); return {}; }
    const meta = JSON.parse(metaRaw.value);
    const result = {};
    let loaded = 0;
    for (const tk of meta.tickers) {
      const raw = await window.storage.get(`${PREFIX}tk_${tk}`).catch(()=>null);
      if (!raw) continue;
      const stored = JSON.parse(raw.value);
      // Descomprimir
      result[tk] = stored.bars.map(b => ({
        date: b.d, hour: b.h,
        open: b.o, high: b.hi, low: b.lo, close: b.c,
        volume: b.v, moneda: b.m,
      }));
      loaded++;
    }
    log(`✅ ${loaded} tickers cargados del storage (guardado: ${meta.savedAt?.slice(0,10)})`, "ok");
    return result;
  },

  // Listar lo que hay guardado
  async getMeta() {
    const raw = await window.storage.get(`${PREFIX}meta`).catch(()=>null);
    if (!raw) return null;
    return JSON.parse(raw.value);
  },

  // Borrar todo
  async clearAll(log) {
    const keys = await window.storage.list(PREFIX).catch(()=>({keys:[]}));
    for (const k of (keys.keys||[])) {
      await window.storage.delete(k).catch(()=>{});
    }
    log("🗑️ Storage limpiado", "warn");
  },
};

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 2: LEARNING ENGINE
// Aprende el comportamiento de cada ticker con historial de simulaciones
// ═══════════════════════════════════════════════════════════════════

const LearningEngine = {

  // Guardar resultado de una simulación
  async saveResult(ticker, result) {
    const key = `${PREFIX}learn_${ticker}`;
    let history = [];
    const raw = await window.storage.get(key).catch(()=>null);
    if (raw) {
      try { history = JSON.parse(raw.value).sessions || []; } catch(_) {}
    }
    history.push({
      date:       new Date().toISOString(),
      simDate:    result.simDate,
      predicted:  result.predicted,   // "COMPRA" / "VENTA" / "NEUTRAL"
      actual:     result.actual,      // rendimiento real % en ventana
      hit:        result.hit,         // boolean
      W:          result.W,
      score:      result.score,
      fundamentals: result.fundamentals || null,
    });
    // Mantener últimas 50 sesiones
    history = history.slice(-50);
    // Calcular parámetros óptimos aprendidos
    const wins = history.filter(s=>s.hit);
    const winRate = history.length ? wins.length/history.length : 0;
    // W más frecuente en aciertos
    const wCounts = {};
    wins.forEach(s => wCounts[s.W] = (wCounts[s.W]||0)+1);
    const bestW = Object.entries(wCounts).sort((a,b)=>b[1]-a[1])[0]?.[0] || 7;
    await window.storage.set(key, JSON.stringify({
      sessions: history,
      winRate: +winRate.toFixed(3),
      bestW: +bestW,
      totalSims: history.length,
      lastUpdate: new Date().toISOString(),
    }));
    return { winRate, bestW: +bestW, totalSims: history.length };
  },

  // Cargar historial de aprendizaje de un ticker
  async getTickerLearn(ticker) {
    const raw = await window.storage.get(`${PREFIX}learn_${ticker}`).catch(()=>null);
    if (!raw) return { winRate: null, bestW: 7, totalSims: 0, sessions: [] };
    return JSON.parse(raw.value);
  },

  // Cargar resumen de aprendizaje de todos los tickers
  async getAllLearning(tickers) {
    const result = {};
    for (const tk of tickers) {
      result[tk] = await LearningEngine.getTickerLearn(tk);
    }
    return result;
  },

  // Guardar historial del simulador
  async saveSimSession(session) {
    const key = `${PREFIX}sim_history`;
    let history = [];
    const raw = await window.storage.get(key).catch(()=>null);
    if (raw) { try { history = JSON.parse(raw.value); } catch(_) {} }
    history.push(session);
    history = history.slice(-100); // últimas 100 sesiones
    await window.storage.set(key, JSON.stringify(history));
  },

  async getSimHistory() {
    const raw = await window.storage.get(`${PREFIX}sim_history`).catch(()=>null);
    if (!raw) return [];
    return JSON.parse(raw.value);
  },
};

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 3: FUNDAMENTALS API
// Busca P/E, EPS, sector y noticias recientes via Claude web_search
// Cachea los resultados 24h para no repetir llamadas
// ═══════════════════════════════════════════════════════════════════

const FundamentalsAPI = {

  async get(ticker, moneda, log) {
    // Verificar caché (24 horas)
    const cacheKey = `${PREFIX}fund_${ticker}`;
    const cached = await window.storage.get(cacheKey).catch(()=>null);
    if (cached) {
      const data = JSON.parse(cached.value);
      const ageHours = (Date.now() - new Date(data.fetchedAt).getTime()) / 3600000;
      if (ageHours < 24) {
        log(`📊 Fundamentales ${ticker} (caché ${ageHours.toFixed(0)}h)`, "dim");
        return data;
      }
    }

    log(`🔍 Buscando fundamentales de ${ticker}...`, "sys");
    const mktLabel = moneda === "USD"
      ? "US stock on NASDAQ/NYSE"
      : "Argentine stock on Buenos Aires exchange (BCBA)";
    const currency = moneda === "USD" ? "USD" : "ARS";

    const prompt =
      `Search for current fundamental data for ${ticker} (${mktLabel}). ` +
      `Provide: P/E ratio, EPS (${currency}), revenue growth YoY %, debt-to-equity, ` +
      `recent news sentiment (positive/neutral/negative), analyst consensus (buy/hold/sell). ` +
      `Reply ONLY with JSON: {"pe":25.3,"eps":6.12,"rev_growth":8.5,"de_ratio":0.45,` +
      `"news_sentiment":"positive","analyst":"buy","summary":"one line summary"}`;

    const messages = [{ role:"user", content: prompt }];
    const TOOLS = [{ type:"web_search_20250305", name:"web_search" }];

    for (let turn=0; turn<4; turn++) {
      const ctrl = new AbortController();
      const tid = setTimeout(()=>ctrl.abort(), 40000);
      let resp;
      try {
        resp = await fetch("https://api.anthropic.com/v1/messages", {
          method:"POST", signal:ctrl.signal,
          headers:{"Content-Type":"application/json"},
          body: JSON.stringify({ model:"claude-sonnet-4-20250514", max_tokens:400, tools:TOOLS, messages }),
        });
      } catch(e) { clearTimeout(tid); break; }
      clearTimeout(tid);
      if (!resp.ok) break;
      const data = await resp.json();
      messages.push({ role:"assistant", content:data.content });
      if (data.stop_reason === "end_turn") {
        const txt = data.content.filter(b=>b.type==="text").map(b=>b.text).join("\n");
        const jm = txt.match(/\{[\s\S]*?\}/);
        if (jm) {
          try {
            const fund = JSON.parse(jm[0]);
            fund.ticker = ticker;
            fund.moneda = moneda;
            fund.fetchedAt = new Date().toISOString();
            // Calcular score fundamental (-10 a +10)
            let fscore = 0;
            if (fund.pe > 0 && fund.pe < 25)  fscore += 3;
            else if (fund.pe > 35)              fscore -= 3;
            if (fund.rev_growth > 10)           fscore += 3;
            else if (fund.rev_growth < 0)       fscore -= 3;
            if (fund.news_sentiment==="positive") fscore += 2;
            else if (fund.news_sentiment==="negative") fscore -= 2;
            if (fund.analyst==="buy")           fscore += 2;
            else if (fund.analyst==="sell")     fscore -= 2;
            fund.fscore = fscore;
            await window.storage.set(cacheKey, JSON.stringify(fund));
            log(`✅ Fundamentales ${ticker}: PE=${fund.pe} Rev=${fund.rev_growth}% score=${fscore}`, "ok");
            return fund;
          } catch(_) {}
        }
      }
      if (data.stop_reason === "tool_use") {
        const tus = data.content.filter(b=>b.type==="tool_use");
        messages.push({ role:"user", content: tus.map(tu=>({ type:"tool_result", tool_use_id:tu.id, content:"Done." })) });
      } else break;
    }
    log(`⚠️ No se obtuvieron fundamentales de ${ticker}`, "warn");
    return { ticker, fscore:0, fetchedAt: new Date().toISOString() };
  },
};

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 4: SIMULATOR
// Toma 5 tickers random de cada panel, elige fecha pasada aleatoria,
// corre predicción y mide accuracy vs lo que realmente ocurrió
// ═══════════════════════════════════════════════════════════════════

function runSimulation(allData, combinedSignalFn, learningData, W=7) {
  // Elegir 5 tickers random de cada panel (con datos suficientes)
  const usaTks    = TICKERS_USA.map(t=>t.ticker).filter(t => allData[t]?.length >= 100);
  const mervalTks = TICKERS_MERVAL.map(t=>t.ticker).filter(t => allData[t]?.length >= 100);
  const shuffle   = arr => [...arr].sort(()=>Math.random()-0.5);
  const selected  = [...shuffle(usaTks).slice(0,5), ...shuffle(mervalTks).slice(0,5)];

  const results = [];

  for (const tk of selected) {
    const bars = allData[tk];
    if (!bars || bars.length < 100) continue;

    // ── FECHA ALEATORIA INDEPENDIENTE POR TICKER ──────────────────
    // Espectro: 0 a 1.5 años (18 meses) hacia atrás desde el final
    // Necesitamos dejar al menos W barras futuras para medir el resultado
    // y al menos 60 barras de historia para los indicadores

    const isHourly    = bars.length > 500;
    const barsPerDay  = isHourly ? 7 : 1;          // ~7 velas 1h por día bursátil
    const barsPerMonth = barsPerDay * 21;            // ~21 días hábiles por mes
    const maxMonths   = 18;                          // espectro 1.5 años
    const minHistory  = 60;                          // mín barras para indicadores
    const futureW     = isHourly ? barsPerMonth : Math.max(W, 10); // ventana de evaluación

    // Rango válido de cutIdx:
    //   - mínimo: necesitamos minHistory barras antes
    //   - máximo: necesitamos futureW barras después
    const idxMin = minHistory;
    const idxMax = bars.length - futureW - 1;

    if (idxMax <= idxMin) continue;

    // Acotar al espectro de 1.5 años
    const maxBarsBack = Math.round(maxMonths * barsPerMonth);
    const idxEarliest = Math.max(idxMin, bars.length - maxBarsBack);

    // Elegir cutIdx completamente random dentro del rango válido
    const cutIdx = idxEarliest + Math.floor(Math.random() * (idxMax - idxEarliest + 1));

    // Calcular cuántos meses atrás es ese punto
    const barsBack  = bars.length - 1 - cutIdx;
    const mesesBack = +(barsBack / barsPerMonth).toFixed(1);

    // Ventana futura de evaluación (1 mes de barras hacia adelante)
    const futureIdx = Math.min(bars.length - 1, cutIdx + futureW);

    // ── SEÑAL EN EL PUNTO HISTÓRICO (sin lookahead) ───────────────
    const histData = bars.slice(0, cutIdx + 1).map(r => ({...r, _ticker: tk}));
    const learn     = learningData[tk];
    const adaptiveW = learn?.bestW || W;
    const sig       = combinedSignalFn(histData, adaptiveW);
    if (!sig) continue;
    // Mejora 1: score muy bajo (<15) → señal poco confiable, descartar
    if ((sig.final_sc || 0) < 15) continue;

    // ── LO QUE REALMENTE PASÓ ─────────────────────────────────────
    const priceAtSim    = bars[cutIdx].close;
    const priceAtFuture = bars[futureIdx].close;
    const actualRet     = +((priceAtFuture - priceAtSim) / priceAtSim * 100).toFixed(2);

    // ── EVALUACIÓN DE ACIERTO ─────────────────────────────────────
    // Umbral adaptativo por volatilidad del ticker (mejora 2 del análisis)
    const volatility = bars.slice(-20).reduce((a,b,i,arr)=>
      i===0?0:a+Math.abs(b.close-arr[i-1].close)/arr[i-1].close*100, 0) / 19;
    // Baja vol (<1%/barra) → umbral 0.5%, alta vol → umbral 2%
    const threshold = volatility < 1.0 ? 0.5 : sig.final_sc >= 70 ? 2.0 : 1.0;
    const predicted = sig.sig;
    let hit = false;
    if (predicted.includes("COMPRA") && actualRet >  threshold) hit = true;
    if (predicted.includes("VENTA")  && actualRet < -threshold) hit = true;
    if (predicted === "NEUTRAL" && Math.abs(actualRet) < threshold) hit = true;

    const moneda = bars[0]?.moneda || (TICKERS_USA.find(t=>t.ticker===tk) ? "USD" : "ARS");

    results.push({
      ticker:       tk,
      moneda,
      simDate:      bars[cutIdx]?.date || "—",
      simDateLabel: `Hace ${mesesBack}m`,
      mesesBack,
      predicted,
      score:        sig.final_sc,
      conf:         sig.conf,
      evoProb:      sig.evo_prob,
      ca15Score:    sig.ca15_score,
      priceAtSim,
      priceAtFuture,
      actualRet,
      hit,
      W:            adaptiveW,
      fundamentals: null,
      panel:        TICKERS_USA.find(t=>t.ticker===tk) ? "USA" : "MERVAL",
    });
  }

  const hits     = results.filter(r=>r.hit).length;
  const accuracy = results.length ? +(hits/results.length*100).toFixed(1) : 0;
  // Rango de fechas usadas en esta simulación
  const mesesRange = results.length
    ? `${Math.min(...results.map(r=>r.mesesBack)).toFixed(1)}m – ${Math.max(...results.map(r=>r.mesesBack)).toFixed(1)}m`
    : "—";

  return {
    id:         Date.now(),
    runAt:      new Date().toISOString(),
    mesesRange,
    results,
    accuracy,
    hits,
    total:      results.length,
    selected,
  };
}


// ── UI ────────────────────────────────────────────────────────
const SC={"COMPRA FUERTE":"#00ff9d","COMPRA":"#5dffb0","NEUTRAL":"#ffd700","VENTA":"#ff9040","VENTA FUERTE":"#ff3355"};
const TC={"ALCISTA FUERTE":"#00ff9d","ALCISTA":"#7dffb8","LATERAL":"#ffd700","BAJISTA":"#ff9040","BAJISTA FUERTE":"#ff3355"};
const TI={"ALCISTA FUERTE":"▲▲","ALCISTA":"▲","LATERAL":"◆","BAJISTA":"▼","BAJISTA FUERTE":"▼▼"};
const GR=r=>r>=72?{l:"A+",c:"#00ff9d"}:r>=62?{l:"A",c:"#5dffb0"}:r>=52?{l:"B+",c:"#ffd700"}:r>=44?{l:"B",c:"#f59e0b"}:{l:"C",c:"#ff3355"};
const F=n=>n?.toLocaleString("es-AR")??"─";
// FP: usa moneda del ticker cuando está disponible, sino el mercado global
const FP=(n, mktOrMoneda)=>{
  if(n==null) return "─";
  const isUSD = mktOrMoneda==="USD" || mktOrMoneda==="USA";
  return isUSD
    ? "$"+n.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})
    : "$"+n.toLocaleString("es-AR");
};
const MONEDA=(r,mkt)=> r?.moneda || (mkt==="USA"?"USD":"ARS");

function Curve({curve,w=80,h=32}) {
  if (!curve?.length) return null;
  const mn=Math.min(...curve)*0.98,mx=Math.max(...curve)*1.02,rng=mx-mn||1;
  const pts=curve.map((v,i)=>`${i/(curve.length-1)*w},${h-(v-mn)/rng*h}`).join(" ");
  const up=curve[curve.length-1]>=100;
  const id="g"+Math.random().toString(36).slice(2,8);
  return <svg width={w} height={h} style={{display:"block"}}>
    <defs><linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stopColor={up?"#00ff9d":"#ff3355"} stopOpacity=".4"/>
      <stop offset="100%" stopColor={up?"#00ff9d":"#ff3355"} stopOpacity="0"/>
    </linearGradient></defs>
    <polygon points={`0,${h} ${pts} ${w},${h}`} fill={`url(#${id})`}/>
    <polyline points={pts} fill="none" stroke={up?"#00ff9d":"#ff3355"} strokeWidth="1.5"/>
    <line x1="0" y1={h-(100-mn)/rng*h} x2={w} y2={h-(100-mn)/rng*h} stroke="#fff2" strokeDasharray="2,3"/>
  </svg>;
}

// Score bar visual (FX vs EVO breakdown)
function ScoreBar({fx, evo, final_sc}) {
  return (
    <div style={{marginTop:"6px"}}>
      <div style={{display:"flex",justifyContent:"space-between",fontSize:"8px",color:"#2e5468",marginBottom:"3px"}}>
        <span>FX <span style={{color:"#00d4ff"}}>{fx}</span></span>
        <span>EVO <span style={{color:"#ff9040"}}>{evo}</span></span>
        <span>COMBINADO <span style={{color:"#00ff9d"}}>{final_sc}</span></span>
      </div>
      <div style={{height:"4px",background:"#0c1826",borderRadius:"2px",overflow:"hidden",display:"flex"}}>
        <div style={{width:`${fx*0.65}%`,background:"#00d4ff",opacity:.7}}/>
        <div style={{width:`${evo*0.35}%`,background:"#ff9040",opacity:.7}}/>
      </div>
    </div>
  );
}

function FXCA16Badge({score}) {
  const c = score===3?"#00ff9d":score===2?"#ffd700":score===1?"#ff9040":"#ff3355";
  return <span style={{display:"inline-flex",alignItems:"center",gap:"3px",background:c+"15",color:c,border:`1px solid ${c}30`,padding:"1px 7px",borderRadius:"3px",fontSize:"9px",fontWeight:700}}>
    FXCA16 {score}/3
  </span>;
}

// ── MAIN APP ──────────────────────────────────────────────────

// ── CSV LOADER — textarea paste, funciona en desktop y mobile ──
function CsvLoader({ onLoad, csvStatus, onClear, embeddedDate }) {
  const [csvText, setCsvText] = useState("");
  const [msg,     setMsg]     = useState("");
  const [loading, setLoading] = useState(false);

  const processText = () => {
    const text = csvText.trim();
    if (!text) { setMsg("Pegá el contenido del CSV primero"); return; }
    if (text.split("\n").length < 10) { setMsg("Texto muy corto — ¿pegaste todo el CSV?"); return; }
    setLoading(true); setMsg("");
    try {
      onLoad(text, "pegado");
      setCsvText("");
      setLoading(false);
    } catch(e) {
      setMsg("Error: " + e.message);
      setLoading(false);
    }
  };

  if (csvStatus) return (
    <div style={{marginBottom:"20px",padding:"12px 16px",background:"#07101a",border:"1px solid #00ff9d40",borderRadius:"6px",maxWidth:"420px",margin:"0 auto 20px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
      <div>
        <div style={{fontSize:"10px",color:"#00ff9d",fontWeight:700}}>✅ {csvStatus.n} tickers cargados</div>
        <div style={{fontSize:"8px",color:"#2e5068"}}>{csvStatus.rows.toLocaleString()} barras · hasta {csvStatus.lastDate||""}</div>
      </div>
      <button className="btn off" onClick={onClear} style={{fontSize:"8px",padding:"3px 10px",color:"#ff3355"}}>✕</button>
    </div>
  );

  return (
    <div style={{marginBottom:"20px",padding:"14px 16px",background:"#07101a",border:"1px solid #0f2235",borderRadius:"6px",maxWidth:"480px",margin:"0 auto 20px"}}>
      <div style={{fontSize:"9px",color:"#1e4058",letterSpacing:".12em",marginBottom:"8px"}}>📋 CARGAR CSV (OPCIONAL)</div>
      <div style={{fontSize:"9px",color:"#2e5068",marginBottom:"10px",lineHeight:"1.8",background:"#050c15",padding:"8px",borderRadius:"4px"}}>
        <strong style={{color:"#7ab0c8"}}>Cómo cargar:</strong><br/>
        1. Abrí el CSV en Notepad / VS Code<br/>
        2. <kbd style={{background:"#0c1826",padding:"1px 5px",borderRadius:"2px",color:"#00d4ff"}}>Ctrl+A</kbd> → <kbd style={{background:"#0c1826",padding:"1px 5px",borderRadius:"2px",color:"#00d4ff"}}>Ctrl+C</kbd><br/>
        3. Tocá abajo y pegá → <strong style={{color:"#00ff9d"}}>⚙️ PROCESAR</strong>
      </div>
      <textarea
        value={csvText}
        onChange={e=>{setCsvText(e.target.value);setMsg("");}}
        placeholder="Pegá el contenido del CSV acá (Ctrl+V / mantené presionado en móvil)..."
        rows={4}
        style={{width:"100%",boxSizing:"border-box",background:"#020508",color:"#7ab0c8",
          border:`1px solid ${csvText?"#00d4ff60":"#0f2235"}`,borderRadius:"4px",
          padding:"8px",fontSize:"8px",fontFamily:"monospace",resize:"vertical",outline:"none"}}
      />
      {csvText.trim()&&<div style={{fontSize:"8px",color:"#2e5068",marginTop:"3px"}}>{csvText.trim().split("\n").length.toLocaleString()} líneas</div>}
      {msg&&<div style={{color:"#ff3355",fontSize:"8px",marginTop:"4px"}}>{msg}</div>}
      <button
        className={`btn ${csvText.trim()&&!loading?"on":"off"}`}
        onClick={processText}
        disabled={!csvText.trim()||loading}
        style={{marginTop:"8px",width:"100%",opacity:csvText.trim()?1:0.5}}
      >
        {loading?"⏳ Procesando...":"⚙️ PROCESAR CSV"}
      </button>
      <div style={{marginTop:"8px",fontSize:"8px",color:"#142030",textAlign:"center"}}>
        Sin CSV → usa datos USA embebidos hasta {embeddedDate}
      </div>
    </div>
  );
}

export default function App() {
  const [fase,  setFase]  = useState("init");
  const [mkt,   setMkt]   = useState("USA");   // "USA" | "MERVAL" | "TODOS"
  const TICKERS = mkt === "USA" ? TICKERS_USA.map(t=>({...t,moneda:"USD"})) : mkt === "MERVAL" ? TICKERS_MERVAL.map(t=>({...t,moneda:"ARS"})) : TICKERS_TODOS;
  const [W,     setW]     = useState(7);
  const [rows,  setRows]  = useState([]);
  const [logs,  setLogs]  = useState([]);
  const [sel,   setSel]   = useState(null);
  const [tab,   setTab]   = useState("opp");
  const [sort,  setSort]  = useState("conf");
  const [secs,  setSecs]  = useState(0);
  const [nReal, setNReal] = useState(0);
  const [priceSrc, setPriceSrc] = useState("—");
  const [optResults,  setOptResults]  = useState([]);
  const [learnView,   setLearnView]   = useState("tickers"); // "tickers"|"history"
  const [userCapital, setUserCapital] = useState(1000000); // capital del usuario (editable)
  const [optParams,   setOptParams]   = useState({}); // { AAPL:{w:7,peso:0.65}, ... }
  const [optApplied,  setOptApplied]  = useState(false); // si los params están activos
  // Parámetros DINÁMICOS aprendidos de simulaciones (el cerebro del sistema)
  const dynParamsRef = useRef({}); // { AAPL:{w,conf,p80adj,evoW,sims,wr}, ... }
  const [dynParamsVersion, setDynParamsVersion] = useState(0); // trigger de re-render
  const [autoSim,      setAutoSim]      = useState(false);   // auto-simulación activa
  const [autoInterval, setAutoInterval] = useState(5);       // minutos entre simulaciones
  const [autoCount,    setAutoCount]    = useState(0);       // simulaciones auto ejecutadas
  const [autoNext,     setAutoNext]     = useState(null);    // timestamp próxima sim
  const [autoCountdown,setAutoCountdown]= useState(0);        // segundos restantes
  const embeddedDataRef = useRef(null); // cache de datos embebidos expandidos
  const autoTimerRef = useRef(null);
  const autoCountRef = useRef(0);
  const [optRunning,  setOptRunning]  = useState(false);
  // v2.0 — Storage, Simulator, Learning
  const [storedMeta,  setStoredMeta]  = useState(null);  // metadata del storage
  const [simResults,  setSimResults]  = useState([]);    // última simulación
  const [simRunning,  setSimRunning]  = useState(false);
  const [simHistory,  setSimHistory]  = useState([]);    // historial de simulaciones
  const [learningData,setLearningData]= useState({});    // aprendizaje por ticker
  const [fundData,    setFundData]    = useState({});    // fundamentales
  const [allStoredData,setAllStoredData] = useState({}); // datos del storage
  const logRef=useRef(null), tmRef=useRef(null);
  const csvDataRef  = useRef({});
  const rowDataRef  = useRef({}); // barras por ticker — fuera del estado React        // { AAPL: [{date,open,high,low,close,volume},...] }
  const [csvStatus, setCsvStatus] = useState(null); // null | {n, tickers, rows}

  const LC={sys:"#00d4ff",ok:"#00ff9d",warn:"#ffd700",err:"#ff3355",info:"#7ab0c8",dim:"#2e5468"};
  const lg=useCallback((msg,type="info")=>{
    const t=new Date().toLocaleTimeString("es-AR");
    setLogs(p=>[...p.slice(-250),{msg,type,t}]);
    setTimeout(()=>{if(logRef.current)logRef.current.scrollTop=logRef.current.scrollHeight;},20);
  },[]);

  // Pre-expandir datos embebidos UNA SOLA VEZ al montar (evita re-expandir en cada run)
  useEffect(()=>{
    // Pre-cargar datos del storage al iniciar (en background)
(async () => {
      try {
        const meta = await window.storage.get('ca15_meta').catch(()=>null);
        if (!meta) return;
        const tickers = JSON.parse(meta.value).tickers || [];
        const result = {};
        await Promise.all(tickers.map(async tk => {
          const raw = await window.storage.get(`ca15_tk_${tk}`).catch(()=>null);
          if (raw) {
            const stored = JSON.parse(raw.value);
            result[tk] = stored.bars.map(b=>({date:b.d,hour:b.h,open:b.o,high:b.hi,low:b.lo,close:b.c,volume:b.v,moneda:b.m,_ticker:tk}));
          }
        }));
        if (Object.keys(result).length > 0) {
          Object.assign(csvDataRef.current, result);
          lg('Storage: ' + Object.keys(result).length + ' tickers pre-cargados', "dim");
        }
      } catch(e) {}
    })();
  },[]);

  // Cargar storage, learning e historial al iniciar
  // eslint-disable-next-line
  useEffect(()=>{
    (async()=>{
      try {
        const meta = await StorageManager.getMeta();
        if (meta) { setStoredMeta(meta); lg(`📂 Storage: ${meta.count} tickers (${meta.savedAt?.slice(0,10)})`, "info"); }
        const hist = await LearningEngine.getSimHistory();
        if (hist.length) setSimHistory(hist);
      } catch(e) {}
    })();
  },[]);

  // ── PARSEAR CSV (formato del script Python: ticker,datetime,hour,open,high,low,close,volume) ──
  const handleCsvUpload = useCallback((e) => {
    const file = e.target?.files?.[0];
    if (!file) return;
    lg(`📂 Leyendo ${file.name} (${(file.size/1024/1024).toFixed(1)} MB)...`, "sys");
    const reader = new FileReader();
    reader.onload = (ev) => {
      processCsvText(ev.target.result);
    };
    reader.readAsText(file);
  }, [lg]);

  const processCsvText = useCallback((text, filename='') => {
      const lines = text.trim().split("\n");
      const header = lines[0].toLowerCase().split(",");
      const iT = header.indexOf("ticker");
      const iD = header.indexOf("datetime") !== -1 ? header.indexOf("datetime") : header.indexOf("date");
      const iO = header.indexOf("open");
      const iH = header.indexOf("high");
      const iL = header.indexOf("low");
      const iC = header.indexOf("close");
      const iV = header.indexOf("volume");
      if (iT < 0 || iC < 0) { lg("❌ CSV inválido: faltan columnas", "err"); return; }
      const parsed = {};
      let total = 0;
      for (let i = 1; i < lines.length; i++) {
        const cols = lines[i].split(",");
        if (cols.length < 6) continue;
        const tk = cols[iT]?.trim().toUpperCase();
        if (!tk) continue;
        const iM = header.indexOf("moneda");
        const row = {
          date:   (cols[iD] || "").slice(0, 10),
          hour:   parseInt((cols[iD] || "").slice(11, 13)) || 0,
          open:   parseFloat(cols[iO]),
          high:   parseFloat(cols[iH]),
          low:    parseFloat(cols[iL]),
          close:  parseFloat(cols[iC]),
          volume: parseInt(cols[iV]) || 0,
          moneda: iM >= 0 ? (cols[iM]?.trim() || "USD") : "USD",
        };
        if (isNaN(row.close) || row.close <= 0) continue;
        if (!parsed[tk]) parsed[tk] = [];
        parsed[tk].push(row);
        total++;
      }
      // Ordenar por fecha
      for (const tk of Object.keys(parsed)) {
        parsed[tk].sort((a, b) => a.date.localeCompare(b.date));
      }
      csvDataRef.current = parsed;
      const tickers = Object.keys(parsed);
      // Calcular fecha más reciente del CSV
      let lastDate = "";
      for (const bars of Object.values(parsed)) {
        const d = bars[bars.length-1]?.date || "";
        if (d > lastDate) lastDate = d;
      }
      setCsvStatus({ n: tickers.length, tickers, rows: total, lastDate });
      lg(`✅ CSV cargado: ${tickers.length} tickers · ${total.toLocaleString()} barras · hasta ${lastDate}`, "ok");
      // Guardar en storage persistente
      StorageManager.saveCSV(parsed, lg).then(n => {
        StorageManager.getMeta().then(m => { if(m) setStoredMeta(m); });
      }).catch(()=>{});
      lg(`   Tickers: ${tickers.join(", ")}`, "info");
  }, [lg]);

  // buildRows — optimizado: batch de 10 tickers + yield cada batch
  const buildRows = useCallback(async (prices, label) => {
    const csv    = csvDataRef.current;
    const yield_ = () => new Promise(r => setTimeout(r, 0));
    const raw    = [];
    const BATCH  = 20;

    for (let i = 0; i < TICKERS.length; i++) {
      const tk      = TICKERS[i];
      const csvRows = csv[tk.ticker];
      const px      = prices[tk.ticker];
      const fromCsv = !!(csvRows && csvRows.length >= 60);
      const hasReal = !!px || fromCsv;

      // Solo usar datos reales del CSV — sin sintéticos
      let data;
      if (fromCsv) {
        data = csvRows;
        data[data.length-1]._ticker = tk.ticker;
      } else if (px) {
        // Sin CSV pero con precio: usar datos embebidos si están disponibles
        const embBars = expandEmbedded(CSV_DATA_EMBEDDED)[tk.ticker];
        if (embBars && embBars.length >= 60) {
          data = embBars;
          data[data.length-1]._ticker = tk.ticker;
        } else {
          data = makeHistory(tk.ticker, px);
          data[data.length-1]._ticker = tk.ticker;
        }
      } else {
        data = null;
      }
      if (!data) continue; // saltar tickers sin datos

      const optW    = optApplied && optParams[tk.ticker]?.w;
      const tickerW = optW ? optW : adaptiveW(tk.ticker, W);
      const sig     = combinedSignal(data, tickerW);
      // bt calculado lazy en tab Detalle — no en el loop principal
      const bt      = {trades:[],curve:[],n:0,hits:0,hr:0,avg:0,aw:0,al:0,pf:0,sh:0,dd:0,eq:100};
      const ps      = null;

      rowDataRef.current[tk.ticker] = data;
      raw.push({ ...tk, sig, bt,
        price: fromCsv ? data[data.length-1].close : (px||null),
        real: hasReal, fromCsv, priceReal: hasReal, ps });

      if ((i+1) % 20 === 0) await yield_();
    }

    const final = applyP80Threshold(raw);
    setRows(final);
    lg(`✅ ${label} · ${raw.length} tickers`, "ok");
    return final;
  }, [W, lg, TICKERS, optApplied, optParams, userCapital]);

  // buildRowsConHistorico: descarga velas 1h reales de Yahoo Finance
  // equivalente al yf.download(..., interval="1h") del script Python
  const buildRowsConHistorico = useCallback(async (prices, market) => {
    lg("📊 Descargando histórico 1h (yfinance compat.)...", "sys");
    const results = await Promise.allSettled(
      TICKERS.map(async tk => {
        const px = prices[tk.ticker];
        let data;
        try {
          data = await fetchHistoricoCompleto(tk.ticker, market, px);
          lg(`📥 ${tk.ticker} ${data.length}b (${data.filter(r=>!r._synth).length} reales)`, "ok");
        } catch(e) {
          lg(`⚠️ ${tk.ticker} sin histórico: ${e.message}`, "warn");
          data = px ? makeHistory(tk.ticker, px) : makeFallback(tk.ticker);
        }
        const sig = combinedSignal(data, W);
        const bt  = backtest(data, W);
        return { ...tk, data, sig, bt, price: data[data.length-1].close, real: !!px };
      })
    );
    const raw = results.map(r => r.status === "fulfilled" ? r.value : null).filter(Boolean);
    const withPrice = raw.filter(r => r.price != null);
    const final = applyP80Threshold(withPrice);
    setRows(final);
    const nHist = withPrice.filter(r => (rowDataRef.current[r.ticker]||[]).some(d => d.hour !== undefined)).length;
    lg(`✅ Histórico 1h listo | ${nHist}/${TICKERS.length} con precios reales`, "ok");
    return final;
  }, [W, lg, TICKERS]);

  // ── AUTO-SIMULADOR EN BACKGROUND ──
  const stopAutoSim = useCallback(() => {
    clearInterval(autoTimerRef.current);
    clearTimeout(autoTimerRef.current);
    setAutoSim(false);
    setAutoNext(null);
    lg("⏹ Auto-simulación detenida", "warn");
  }, [lg]);

  const startAutoSim = useCallback((intervalMin) => {
    clearInterval(autoTimerRef.current);
    setAutoSim(true);
    lg(`🤖 Auto-simulación iniciada — cada ${intervalMin} min`, "ok");

    const tick = async () => {
      setAutoNext(Date.now() + intervalMin * 60000);
      lg(`🔄 Auto-sim #${autoCountRef.current + 1} corriendo...`, "sys");
      // Reusar la misma lógica del simulador manual
      // (dispara el evento como si el usuario hubiera presionado el botón)
      document.dispatchEvent(new CustomEvent("fxca16:autosim"));
    };

    // Primera ejecución inmediata
    tick();
    // Loop periódico
    autoTimerRef.current = setInterval(tick, intervalMin * 60000);
  }, [lg]);

  // Escuchar el evento de auto-sim
  useEffect(() => {
    const handler = () => {
      autoCountRef.current += 1;
      setAutoCount(autoCountRef.current);
      // Ejecutar simulación (mismo código que runSimulator)
      const embData    = embeddedDataRef.current || {};
      const dataSource = Object.keys(csvDataRef.current).length
        ? csvDataRef.current : embData;
      if (Object.keys(dataSource).length < 5) return;
      const session = runSimulation(dataSource, combinedSignal, dynParamsRef.current.learningData || {}, W);
      // Actualizar historial en memoria
      setSimHistory(prev => [...prev, {
        runAt: session.runAt, mesesRange: session.mesesRange,
        accuracy: session.accuracy, hits: session.hits, total: session.total,
        results: session.results, auto: true,
      }].slice(-100));
      // Guardar aprendizaje en background
      Promise.all(session.results.map(r =>
        LearningEngine.saveResult(r.ticker, {
          simDate: r.simDate, predicted: r.predicted, actual: r.actualRet,
          hit: r.hit, W: r.W, score: r.score,
        }).catch(()=>{})
      )).then(async () => {
        const learns = await Promise.all(
          session.results.map(r => LearningEngine.getTickerLearn(r.ticker).catch(()=>null))
        );
        const updatedLearn = {};
        session.results.forEach((r,i) => { if (learns[i]) updatedLearn[r.ticker] = learns[i]; });
        setLearningData(prev => ({...prev, ...updatedLearn}));
        // Actualizar dynParams
        const newDyn = {...dynParamsRef.current};
        for (const r of session.results) {
          const learn = updatedLearn[r.ticker];
          if (!learn || learn.totalSims < 1) continue;
          const wr  = learn.winRate || 0.5;
          const sims = learn.totalSims || 0;
          const w   = Math.min(sims/15,1.0);
          newDyn[r.ticker] = {
            w:    learn.bestW || 7,
            conf: +((TICKER_CONFIDENCE[r.ticker]||0) + (wr-0.5)*0.4*w).toFixed(3),
            p80adj: wr>=0.65?-3:wr<=0.35?3:0,
            evoW:  0.35, sims, wr,
          };
        }
        dynParamsRef.current = newDyn;
        setDynParams(newDyn);
        setDynParamsVersion(v=>v+1);
        LearningEngine.saveSimSession({
          runAt: session.runAt, mesesRange: session.mesesRange,
          accuracy: session.accuracy, hits: session.hits, total: session.total,
          results: session.results.map(r=>({t:r.ticker,p:r.panel,m:r.moneda,
            d:r.simDate,mb:+(r.mesesBack||0).toFixed(1),
            pr:r.predicted?.slice(0,2),ar:r.actualRet,h:r.hit?1:0,s:r.score,e:r.evoProb})),
        }).catch(()=>{});
      });
    };
    document.addEventListener("fxca16:autosim", handler);
    return () => document.removeEventListener("fxca16:autosim", handler);
  }, [W]);

  // Limpiar timer al desmontar
  useEffect(() => () => clearInterval(autoTimerRef.current), []);

  // Countdown en tiempo real
  useEffect(() => {
    if (!autoSim || !autoNext) return;
    const t = setInterval(() => {
      setAutoCountdown(Math.max(0, Math.round((autoNext - Date.now()) / 1000)));
    }, 1000);
    return () => clearInterval(t);
  }, [autoSim, autoNext]);

  // ── CARGAR DATOS DESDE STORAGE ──
  const loadFromStorage = useCallback(async () => {
    setFase("load"); setRows([]); setLogs([]); setSecs(0); setNReal(0); setPriceSrc("—");
    const stored = await StorageManager.loadAll(lg);
    if (!Object.keys(stored).length) {
      lg("⚠️ No hay datos guardados. Subí un CSV primero.", "warn");
      setFase("done"); return;
    }
    setAllStoredData(stored);
    // Cargar aprendizaje
    const allTks = Object.keys(stored);
    const learn = await LearningEngine.getAllLearning(allTks);
    setLearningData(learn);
    // Usar los datos como si fueran del CSV
    Object.assign(csvDataRef.current, stored);
    const prices = {};
    for (const [tk, bars] of Object.entries(stored)) {
      if (bars.length > 0) prices[tk] = bars[bars.length-1].close;
    }
    const n = Object.keys(prices).length;
    setNReal(n);
    setPriceSrc(`Storage · ${n} tickers`);
    await buildRows(prices, "Storage");
    setFase("done");
    lg(`✅ ${n} tickers cargados del storage`, "ok");
  }, [lg, buildRows]);

  // ── SIMULADOR ──
  const runSimulator = useCallback(async () => {
    // Usar datos embebidos si no hay stored data
    const embData    = embeddedDataRef.current || {};
    const dataSource = Object.keys(allStoredData).length
      ? allStoredData
      : Object.keys(csvDataRef.current).length
        ? csvDataRef.current
        : embData;

    if (Object.keys(dataSource).length < 5) {
      lg("⚠️ Sin datos suficientes para simular.", "warn");
      return;
    }
    setSimRunning(true);
    lg("🎲 Simulando...", "sys");

    // ── PASO 1: cálculo puro — rápido, sin I/O ──
    const session = runSimulation(dataSource, combinedSignal, learningData, W);
    setSimResults(session.results);
    // Agregar al historial en memoria INMEDIATAMENTE (sin esperar storage)
    setSimHistory(prev => [...prev, {
      runAt: session.runAt, mesesRange: session.mesesRange,
      accuracy: session.accuracy, hits: session.hits, total: session.total,
      results: session.results,
    }].slice(-50));
    lg(`📊 Rango ${session.mesesRange} | Accuracy: ${session.accuracy}% (${session.hits}/${session.total})`,
       session.accuracy >= 60 ? "ok" : "warn");
    setSimRunning(false); // ← mostrar resultados YA, sin esperar storage ni fundamentales

    // ── PASO 2: storage y aprendizaje en background (no bloquea UI) ──
    Promise.resolve().then(async () => {
      // Guardar aprendizaje — todas las escrituras en paralelo
      const saves = session.results.map(r =>
        LearningEngine.saveResult(r.ticker, {
          simDate: r.simDate, predicted: r.predicted, actual: r.actualRet,
          hit: r.hit, W: r.W, score: r.score,
        }).catch(() => {})
      );
      await Promise.all(saves);

      // Leer estado actualizado de aprendizaje — en paralelo
      const learns = await Promise.all(
        session.results.map(r => LearningEngine.getTickerLearn(r.ticker).catch(() => null))
      );
      const updatedLearn = {...learningData};
      session.results.forEach((r, i) => { if (learns[i]) updatedLearn[r.ticker] = learns[i]; });
      setLearningData(updatedLearn);

      // Guardar sesión en historial
      try {
        await LearningEngine.saveSimSession({
          runAt: session.runAt, mesesRange: session.mesesRange,
          accuracy: session.accuracy, hits: session.hits, total: session.total,
          results: session.results.map(r=>({
            t: r.ticker, p: r.panel, m: r.moneda,
            d: r.simDate, mb: +(r.mesesBack||0).toFixed(1),
            pr: r.predicted?.slice(0,2), ar: r.actualRet,
            h: r.hit?1:0, s: r.score, e: r.evoProb,
          })),
        });
        const hist = await LearningEngine.getSimHistory();
        if (hist?.length) {
          // Expandir campos comprimidos para la UI
          const expanded = hist.map(s=>({...s,
            results: s.results?.map(r=>({
              ticker:r.t, panel:r.p, moneda:r.m,
              simDate:r.d, mesesBack:r.mb,
              predicted: r.pr==="CO"?"COMPRA":r.pr==="VE"?"VENTA":r.pr==="CF"?"COMPRA FUERTE":r.pr==="VF"?"VENTA FUERTE":"NEUTRAL",
              actualRet:r.ar, hit:r.h===1, score:r.s, evoProb:r.e,
            }))
          }));
          setSimHistory(expanded);
        }
      } catch(e) {
        // Si storage falla, mantener en memoria igual
        setSimHistory(prev => [...prev, {
          runAt: session.runAt, mesesRange: session.mesesRange,
          accuracy: session.accuracy, hits: session.hits, total: session.total,
          results: session.results,
        }].slice(-50));
      }
    });

    // ── PASO 3: fundamentales en background — solo si no están cacheados ──
    const fundResults = {...fundData};
    const missing = session.results.filter(r => !fundResults[r.ticker]);
    if (missing.length) {
      lg(`🔍 Buscando fundamentales (${missing.length} tickers)...`, "dim");
      missing.forEach(r => {
        FundamentalsAPI.get(r.ticker, r.moneda, lg)
          .then(f => {
            fundResults[r.ticker] = f;
            setFundData({...fundResults});
            setSimResults(prev => prev.map(p =>
              p.ticker === r.ticker ? {...p, fundamentals: f} : p
            ));
          })
          .catch(() => {});
      });
    }
  }, [allStoredData, learningData, W, lg, fundData]);

  // ── OPTIMIZADOR — portado del script Python ──
  const runOptimizer = useCallback(async () => {
    if (!rows.length) { lg("Primero ejecutá el sistema principal", "warn"); return; }
    setOptRunning(true);
    lg("🔬 Iniciando optimización FXCA16 (W × Peso_FX)...", "sys");
    const results = [];
    for (const row of rows) {
      lg(`📊 Optimizando ${row.ticker}...`, "dim");
      await new Promise(r => setTimeout(r, 2)); // yield para no bloquear UI
      const opt = optimizarTicker(rowDataRef.current[row.ticker] || []);
      const wr  = opt.trades > 0 ? +(opt.wins / opt.trades * 100).toFixed(1) : 0;
      results.push({
        ticker:  row.ticker,
        name:    row.name,
        sector:  row.sector,
        w_opt:   opt.w,
        peso_fx: opt.peso,
        capital: opt.capital,
        trades:  opt.trades,
        wins:    opt.wins,
        wr,
        pct:     opt.pct,
      });
      lg(`✅ ${row.ticker} → W=${opt.w} PesoFX=${opt.peso} Rend=${opt.pct}%`, opt.pct >= 0 ? "ok" : "warn");
    }
    results.sort((a, b) => b.pct - a.pct);
    setOptResults(results);

    // Guardar mapa de parámetros óptimos por ticker
    const params = {};
    for (const r of results) {
      params[r.ticker] = { w: r.w_opt, peso: r.peso_fx, pct: r.pct };
    }
    setOptParams(params);

    setOptRunning(false);
    lg(`🏆 Optimización completada. Mejor: ${results[0]?.ticker} ${results[0]?.pct}%`, "ok");
    lg(`💡 Podés aplicar estos parámetros al sistema desde la tab ⚙️ Optimizar`, "info");
    setTab("opt");
  }, [rows, lg]);

    const run=useCallback(async ()=>{
    setFase("load"); setRows([]); setLogs([]); setSecs(0); setNReal(0); setPriceSrc("—");
    clearInterval(tmRef.current);
    tmRef.current = setInterval(()=>setSecs(s=>s+1), 1000);

    // ── Paso 1: datos del CSV subido > storage > precios conocidos > sintético ──
    const mktTickers = TICKERS.map(t=>t.ticker);

    // Prioridad 1: CSV subido manualmente esta sesión
    const draggedData = csvDataRef.current;
    const hasDragged  = Object.keys(draggedData).some(tk => mktTickers.includes(tk));

    if (hasDragged) {
      const csvForMkt = Object.fromEntries(Object.entries(draggedData).filter(([tk])=>mktTickers.includes(tk)));
      const csvPrices = Object.fromEntries(Object.entries(csvForMkt).map(([tk,bars])=>[tk,bars[bars.length-1].close]));
      setNReal(Object.keys(csvPrices).length);
      setPriceSrc(`CSV · ${Object.keys(csvPrices).length} tickers`);
      lg(`📊 CSV: ${Object.keys(csvPrices).length} tickers cargados`, "ok");
      await buildRows(csvPrices, "CSV");
      setFase("done");
    } else {
      // Sin CSV → usar datos embebidos (instantáneo, sin storage)
      const emb = expandEmbedded(CSV_DATA_EMBEDDED);
      const embForMkt = Object.fromEntries(Object.entries(emb).filter(([tk])=>mktTickers.includes(tk)));
      Object.assign(csvDataRef.current, embForMkt);
      const prices = Object.fromEntries(Object.entries(embForMkt).map(([tk,bars])=>[tk,bars[bars.length-1].close]));
      const n = Object.keys(prices).length;
      setNReal(n);
      setPriceSrc(`Embebido · ${n}t · 25/03`);
      lg(`📊 ${n} tickers cargados`, "ok");
      await buildRows(prices, "Embebido");
      setFase("done");
    }

    clearInterval(tmRef.current);
    setSecs(0);

    // Web search desactivado en auto-run (tarda 40s+)
    // El usuario puede activarlo manualmente con el botón "↺ + Live"
  },[W, lg, buildRows, TICKERS, mkt]);

  const opps=useMemo(()=>rows.filter(r=>r.sig&&r.sig.sig!=="NEUTRAL"&&r.sig.above_p80).sort((a,b)=>b.sig.conf-a.sig.conf),[rows]);
  const srtd=useMemo(()=>[...rows].sort((a,b)=>{
    if(sort==="conf")return(b.sig?.conf||0)-(a.sig?.conf||0);
    if(sort==="hr")return b.bt.hr-a.bt.hr;
    if(sort==="sh")return b.bt.sh-a.bt.sh;
    if(sort==="ca15")return(b.sig?.ca15_score||0)-(a.sig?.ca15_score||0);
    if(sort==="evo")return(b.sig?.evo_prob||0)-(a.sig?.evo_prob||0);
    return 0;
  }),[rows,sort]);
  const stats=useMemo(()=>{
    if(!rows.length)return null;
    const p80=rows[0]?.sig?.p80_threshold||0;
    const allSignals = rows.filter(r=>r.sig?.above_p80);
    return{ef:0,buy:allSignals.filter(r=>r.sig.sig.includes("COMPRA")).length,sell:allSignals.filter(r=>r.sig.sig.includes("VENTA")).length,p80:p80.toFixed(0)};
  },[rows,opps]);

  const CSS=`
    @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Space+Mono:wght@400;700&display=swap');
    *{box-sizing:border-box;margin:0;padding:0}
    .card{background:#07101a;border:1px solid #0f2235;border-radius:5px}
    .btn{cursor:pointer;border:none;font-family:'Space Mono',monospace;font-size:9px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:5px 11px;border-radius:3px;transition:all .15s}
    .off{background:#00ff9d12;color:#00ff9d;border:1px solid #00ff9d28}.off:hover{background:#00ff9d22}
    .on{background:#00ff9d;color:#03070e;font-weight:700}
    .blink{animation:bl 1s step-end infinite}@keyframes bl{50%{opacity:0}}
    .fade{animation:fd .25s ease}@keyframes fd{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
    ::-webkit-scrollbar{width:3px;height:3px}::-webkit-scrollbar-thumb{background:#1a3048;border-radius:2px}
    table{border-collapse:collapse;width:100%}
    th{padding:6px 9px;font-size:8px;color:#1e4058;letter-spacing:.12em;border-bottom:1px solid #0f2235;text-align:left;white-space:nowrap;background:#040a12;font-family:'Space Mono',monospace}
    td{padding:6px 9px;font-size:11px;border-bottom:1px solid #091520}
    tr:hover td{background:#0c1c2e;cursor:pointer}
    .badge{display:inline-block;padding:2px 7px;border-radius:3px;font-size:9px;font-weight:700}
    @keyframes pulse{0%,100%{opacity:.15}50%{opacity:1}}
    .grid-opp{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:10px}
  `;

  return (
    <div style={{fontFamily:"'Space Mono',monospace",background:"#03070e",minHeight:"100vh",color:"#8ab0c8"}}>
      <style>{CSS}</style>

      {/* NAV */}
      <div style={{background:"#040a12",borderBottom:"1px solid #0f2235",padding:"0 16px",position:"sticky",top:0,zIndex:99}}>
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",height:"46px",flexWrap:"wrap",gap:"8px"}}>
          <div style={{display:"flex",alignItems:"center",gap:"10px"}}>
            <div style={{background:"linear-gradient(135deg,#00ff9d,#00d4ff)",borderRadius:"4px",width:"28px",height:"28px",display:"flex",alignItems:"center",justifyContent:"center",fontFamily:"'Bebas Neue'",fontSize:"12px",color:"#030810",fontWeight:700,letterSpacing:".05em"}}>CA</div>
            <div>
              <div style={{fontFamily:"'Bebas Neue'",fontSize:"16px",color:"#e0f4ff",letterSpacing:".14em",lineHeight:1}}>
                FXCA16
              </div>
              <div style={{fontSize:"8px",color:"#1a3a50",letterSpacing:".12em"}}>MERVAL · SISTEMA COMBINADO · P80 THRESHOLD</div>
            </div>
          </div>
          <div style={{display:"flex",gap:"14px",fontSize:"9px",color:"#1e4058",alignItems:"center"}}>
            {fase==="load"&&<span style={{color:"#ffd700",fontFamily:"'Bebas Neue'",fontSize:"20px",letterSpacing:".05em"}}>🔍 {secs}s</span>}
            {fase==="done"&&secs>0&&<span style={{fontSize:"9px",color:"#ffd700"}}>📡 actualizando {secs}s<span className="blink">…</span></span>}
            <span style={{background:mkt==="USA"?"#00d4ff18":mkt==="MERVAL"?"#ffd70018":"#00ff9d18",color:mkt==="USA"?"#00d4ff":mkt==="MERVAL"?"#ffd700":"#00ff9d",border:`1px solid ${mkt==="USA"?"#00d4ff30":mkt==="MERVAL"?"#ffd70030":"#00ff9d30"}`,padding:"2px 9px",borderRadius:"3px",fontSize:"9px",fontWeight:700}}>{mkt==="USA"?"🇺🇸 USA":mkt==="MERVAL"?"🇦🇷 MERVAL":"🌎 TODOS"}</span>
            {stats&&<>
              <span style={{color:nReal>=15?"#00ff9d":nReal>=8?"#ffd700":"#ff9040",fontWeight:700}}>📡 {nReal}/{TICKERS.length}</span><span style={{background:"#00d4ff12",color:"#00d4ff",border:"1px solid #00d4ff25",padding:"2px 7px",borderRadius:"3px",fontSize:"8px"}}>{priceSrc}</span>
              <span>P80 <strong style={{color:"#00d4ff"}}>≥{stats.p80}</strong></span>
              <span>EF <strong style={{color:stats.ef>=60?"#00ff9d":"#ff3355"}}>{stats.ef}%</strong></span>
              <span style={{color:"#00ff9d"}}>▲{stats.buy}</span>
              <span style={{color:"#ff3355"}}>▼{stats.sell}</span>
            </>}
          </div>
        </div>
      </div>

      <div style={{padding:"14px 16px"}}>

        {/* INICIO */}
        {fase==="init"&&(
          <div className="fade" style={{textAlign:"center",padding:"50px 16px"}}>
            <div style={{display:"inline-block",position:"relative",marginBottom:"16px"}}>
              <div style={{fontFamily:"'Bebas Neue'",fontSize:"72px",color:"#e0f4ff",letterSpacing:".06em",lineHeight:1}}>FXCA16</div>
              <div style={{position:"absolute",bottom:"-2px",right:"-4px",fontFamily:"'Bebas Neue'",fontSize:"22px",color:"#00d4ff",opacity:.8}}>v2</div>
            </div>
            <div style={{fontSize:"9px",color:"#1e4058",letterSpacing:".2em",marginBottom:"6px"}}>SISTEMA COMBINADO · MERVAL ARGENTINA</div>
            <div style={{display:"flex",justifyContent:"center",gap:"16px",fontSize:"9px",marginBottom:"28px",flexWrap:"wrap"}}>
              <span style={{color:"#00d4ff"}}>FX-TÉCNICO <span style={{color:"#2e5060"}}>65% · RSI+MACD+Bollinger</span></span>
              <span style={{color:"#1e4058"}}>|</span>
              <span style={{color:"#ff9040"}}>EVO-SCORE <span style={{color:"#2e5060"}}>35% · Score+Vol+Momentum</span></span>
              <span style={{color:"#1e4058"}}>|</span>
              <span style={{color:"#ffd700"}}>Umbral P80</span>
            </div>
            <div style={{display:"flex",justifyContent:"center",gap:"8px",marginBottom:"20px"}}>
              {[["USA","🇺🇸 USA · 28"],["MERVAL","🇦🇷 Merval · 52"],["TODOS","🌎 Todos · 80"]].map(([k,l])=>
                <button key={k} className={`btn ${mkt===k?"on":"off"}`} onClick={()=>setMkt(k)} style={{padding:"9px 18px",fontSize:"11px"}}>{l}</button>
              )}
            </div>
            {/* Capital del usuario */}
            <div style={{marginBottom:"16px",maxWidth:"420px",margin:"0 auto 16px"}}>
              <div style={{fontSize:"8px",color:"#1a3a50",marginBottom:"6px",letterSpacing:".12em"}}>💰 CAPITAL TOTAL (para position sizing)</div>
              <div style={{display:"flex",gap:"6px",alignItems:"center",flexWrap:"wrap"}}>
                {[100000,500000,1000000,5000000].map(v=>(
                  <button key={v} className={`btn ${userCapital===v?"on":"off"}`}
                    onClick={()=>setUserCapital(v)}
                    style={{fontSize:"9px",padding:"5px 10px"}}>
                    {v>=1000000?`$${v/1000000}M`:`$${v/1000}K`}
                  </button>
                ))}
                <input
                  type="number"
                  value={userCapital}
                  onChange={e=>setUserCapital(+e.target.value||1000000)}
                  style={{width:"90px",background:"#020508",color:"#00d4ff",
                    border:"1px solid #00d4ff40",borderRadius:"4px",
                    padding:"5px 8px",fontSize:"9px",textAlign:"right"}}
                />
              </div>
              <div style={{fontSize:"8px",color:"#1e4058",marginTop:"4px"}}>
                Riesgo máximo por operación: <strong style={{color:"#ffd700"}}>${(userCapital*0.01).toLocaleString()}</strong> (1%)
              </div>
            </div>

            <div style={{marginBottom:"22px"}}>
              <div style={{fontSize:"8px",color:"#1a3a50",marginBottom:"8px",letterSpacing:".12em"}}>VENTANA ANÁLISIS</div>
              <div style={{display:"flex",gap:"6px",justifyContent:"center"}}>
                {[5,6,7,8,9,10].map(d=><button key={d} className={`btn ${W===d?"on":"off"}`} onClick={()=>setW(d)} style={{padding:"7px 14px",fontSize:"11px"}}>{d}d</button>)}
              </div>
            </div>


            <div style={{display:"flex",gap:"8px",justifyContent:"center",flexWrap:"wrap"}}>
              <button className="btn on" onClick={run} style={{padding:"13px 40px",fontSize:"12px",letterSpacing:".15em",boxShadow:"0 0 30px #00ff9d18"}}>▶ EJECUTAR</button>
              {storedMeta && (
                <button className="btn off" onClick={loadFromStorage} style={{padding:"13px 22px",fontSize:"11px",color:"#00d4ff",borderColor:"#00d4ff40"}}>
                  📂 CARGAR STORAGE<br/><span style={{fontSize:"8px",opacity:.7}}>{storedMeta.count} tickers · {storedMeta.savedAt?.slice(0,10)}</span>
                </button>
              )}
            </div>
            <div style={{marginTop:"10px",fontSize:"9px",color:"#142030"}}>
              {csvStatus
                ? `📊 CSV: ${csvStatus.n} tickers · ${csvStatus.rows.toLocaleString()} barras · ${csvStatus.lastDate||""}`
                : `📊 Datos embebidos: ${Object.keys(CSV_DATA_EMBEDDED||{}).length} tickers · 25/03/2026`}
            </div>
          </div>
        )}

        {/* LOADING */}
        {fase==="load"&&(
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"14px",maxWidth:"900px",margin:"0 auto"}}>
            <div style={{textAlign:"center",padding:"20px 0"}}>
              <div style={{fontFamily:"'Bebas Neue'",fontSize:"26px",color:"#00ff9d",marginBottom:"8px",letterSpacing:".1em"}}>PROCESANDO <span className="blink">█</span></div>
              <div style={{fontFamily:"'Bebas Neue'",fontSize:"62px",color:"#ffd700",lineHeight:1,marginBottom:"6px"}}>{secs}s</div>
              <div style={{maxWidth:"240px",margin:"0 auto 14px"}}>
                <div style={{background:"#07101a",border:"1px solid #0f2235",borderRadius:"3px",height:"3px",overflow:"hidden"}}>
                  <div style={{width:rows.length?`${rows.length/TICKERS.length*100}%`:"100%",height:"100%",background:"linear-gradient(90deg,#00d4ff,#00ff9d)",animation:rows.length?"none":"pulse 1.5s ease-in-out infinite"}}/>
                </div>
              </div>
              {rows.length>0&&(
                <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"4px",maxWidth:"300px",margin:"0 auto"}}>
                  {rows.map(r=>(
                    <div key={r.ticker} style={{padding:"4px",background:r.fromCsv?"#00d4ff0a":"#00ff9d0a",border:`1px solid ${r.fromCsv?"#00d4ff28":"#00ff9d28"}`,borderRadius:"3px",textAlign:"center"}}>
                      <div style={{fontSize:"8px",color:r.fromCsv?"#00d4ff":"#00ff9d",fontWeight:700}}>{r.ticker}</div>
                      <div style={{fontFamily:"'Bebas Neue'",fontSize:"11px",color:"#d0ecff"}}>{FP(r.price,mkt)}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div>
              <div style={{fontSize:"8px",color:"#1e4058",letterSpacing:".12em",marginBottom:"4px"}}>TERMINAL</div>
              <div ref={logRef} style={{background:"#020508",border:"1px solid #091520",borderRadius:"5px",height:"420px",overflowY:"auto",padding:"8px",fontSize:"9px",lineHeight:"1.9",fontFamily:"'Space Mono',monospace"}}>
                {logs.map((l,i)=><div key={i} style={{color:LC[l.type]||"#7ab0c8",wordBreak:"break-all"}}><span style={{color:"#142030",marginRight:"6px"}}>{l.t}</span>{l.msg}</div>)}
              </div>
            </div>
          </div>
        )}

        {/* RESULTADOS */}
        {fase==="done"&&rows.length===0&&(
          <div className="fade" style={{textAlign:"center",padding:"50px 16px",color:"#1e4058"}}>
            <div style={{fontFamily:"'Bebas Neue'",fontSize:"28px",color:"#ffd700",marginBottom:"10px"}}>
              ⚙️ CALCULANDO<span className="blink"> █</span>
            </div>
            <div style={{fontSize:"10px",marginBottom:"8px",color:"#2e5468"}}>Procesando tickers del CSV...</div>
            <div style={{background:"#07101a",border:"1px solid #0f2235",borderRadius:"3px",height:"4px",width:"240px",margin:"0 auto",overflow:"hidden"}}>
              <div style={{height:"100%",background:"linear-gradient(90deg,#00d4ff,#00ff9d)",animation:"pulse 1s ease-in-out infinite"}}/>
            </div>
          </div>
        )}
        {fase==="done"&&rows.length>0&&(
          <div className="fade">
            <div style={{display:"flex",gap:"5px",marginBottom:"10px",flexWrap:"wrap",alignItems:"center"}}>
              {[["opp","🎯 Top P80"],["rank","🏆 Ranking"],["det","🔍 Detalle"],["opt","⚙️ Optimizar"],["sim","💡 Simulador"],["learn","🧠 Aprendizaje"]].map(([k,l])=>
                <button key={k} className={`btn ${tab===k?"on":"off"}`} onClick={()=>setTab(k)}>{l}</button>
              )}
              <div style={{marginLeft:"auto",display:"flex",gap:"3px",alignItems:"center",flexWrap:"wrap"}}>
                {[["USA","🇺🇸"],["MERVAL","🇦🇷"],["TODOS","🌎"]].map(([k,l])=>
                  <button key={k} className={`btn ${mkt===k?"on":"off"}`} onClick={()=>{setMkt(k);setRows([]);}} style={{padding:"2px 8px",fontSize:"10px"}}>{l}</button>
                )}
                <span style={{color:"#0f2235",margin:"0 2px"}}>|</span>
                {[5,6,7,8,9,10].map(d=><button key={d} className={`btn ${W===d?"on":"off"}`} onClick={()=>setW(d)} style={{padding:"2px 8px"}}>{d}d</button>)}
                <button className="btn off" onClick={run} style={{marginLeft:"4px"}}>↺</button>
                {!storedMeta ? null :
                  <button className="btn off" onClick={()=>setTab("sim")} style={{marginLeft:"4px",color:"#ffd700",fontSize:"9px"}}>💡 SIM</button>
                }
              </div>
            </div>
            <div style={{display:"flex",gap:"12px",padding:"7px 12px",background:"#07101a",borderRadius:"5px",border:"1px solid #0f2235",fontSize:"9px",marginBottom:"10px",flexWrap:"wrap",alignItems:"center"}}>
              {csvStatus && <span style={{color:"#00ff9d",fontWeight:700,fontSize:"9px"}}>📊 CSV {csvStatus.n}t</span>}
              <span style={{color:nReal>=15?"#00ff9d":nReal>=8?"#ffd700":"#ff9040",fontWeight:700}}>📡 {nReal}/{TICKERS.length} · <span style={{color:"#00d4ff"}}>{priceSrc}</span></span>
              <span style={{color:"#0f2235"}}>|</span>
              <span>P80 <strong style={{color:"#00d4ff"}}>≥{stats?.p80}</strong> — top 20%</span>
              {optApplied && <span style={{color:"#00ff9d",fontWeight:700,fontSize:"9px",background:"#00ff9d12",padding:"2px 7px",borderRadius:"3px"}}>🎯 OPT</span>}
              {autoSim && <span style={{color:"#ffd700",fontWeight:700,fontSize:"9px",background:"#ffd70012",padding:"2px 7px",borderRadius:"3px"}}>🤖 AUTO</span>}
              <span style={{color:"#0f2235"}}>|</span>
              <span style={{color:MARKET_REGIME.regime==="bull"?"#00ff9d":MARKET_REGIME.regime==="bear"?"#ff3355":"#ffd700",fontWeight:700,fontSize:"9px"}}>
                {MARKET_REGIME.regime==="bull"?"🐂 BULL":MARKET_REGIME.regime==="bear"?"🐻 BEAR":"◆ NEUTRAL"} {MARKET_REGIME.spyRoc!==0?`SPY vs SMA200: ${MARKET_REGIME.spyRoc>0?"+":""}${MARKET_REGIME.spyRoc}%`:""}
              </span>
              <span style={{color:"#0f2235"}}>|</span>
              <span>EF <strong style={{color:stats?.ef>=60?"#00ff9d":"#ff3355"}}>{stats?.ef}%</strong></span>
              <span style={{color:"#00ff9d"}}>▲ {stats?.buy}</span><span style={{color:"#ff3355"}}>▼ {stats?.sell}</span>
            </div>

            {/* OPORTUNIDADES TOP P80 */}
            {tab==="opp"&&(
              <div className="fade">
                {opps.length===0&&<div style={{textAlign:"center",padding:"40px",color:"#1a3848",fontSize:"11px"}}>Sin señales en el top P80 con ventana {W}d.</div>}
                <div className="grid-opp">
                  {opps.map(r=>{
                    const s=r.sig,buy=s.sig.includes("COMPRA"),g=GR(r.bt.hr);
                    return (
                      <div key={r.ticker} className="card" style={{padding:"13px",cursor:"pointer",borderLeft:`3px solid ${SC[s.sig]}`}} onClick={()=>{setSel(r);setTab("det");}}>
                        <div style={{display:"flex",justifyContent:"space-between",marginBottom:"8px"}}>
                          <div>
                            <div style={{display:"flex",alignItems:"center",gap:"8px",marginBottom:"2px"}}>
                              <span style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color:SC[s.sig],letterSpacing:".06em"}}>{r.ticker}</span>
                              <span style={{fontSize:"9px",color:r.fromCsv?"#00d4ff":r.real?"#00ff9d":"#ffd700",fontWeight:700}}>{r.fromCsv?"📊":r.real?"📡":"🔬"}</span>
                          <span style={{fontSize:"8px",color:MONEDA(r,mkt)==="USD"?"#00d4ff":"#ffd700",background:MONEDA(r,mkt)==="USD"?"#00d4ff12":"#ffd70012",padding:"1px 5px",borderRadius:"3px",fontWeight:700}}>{MONEDA(r,mkt)}</span>
                              <FXCA16Badge score={s.ca15_score}/>
                            </div>
                            <div style={{fontSize:"8px",color:"#2e5060"}}>{r.name}</div>
                          </div>
                          <div style={{textAlign:"right"}}>
                            <span className="badge" style={{background:SC[s.sig]+"20",color:SC[s.sig],border:`1px solid ${SC[s.sig]}40`,display:"block",marginBottom:"3px"}}>{s.sig}</span>
                            <span style={{fontSize:"8px",color:TC[s.trend]}}>{TI[s.trend]} {s.trend}</span>
                          </div>
                        </div>

                        {/* Score breakdown FX vs EVO */}
                        <ScoreBar fx={s.fx_sc} evo={s.evo_sc} final_sc={s.final_sc}/>

                        <div style={{background:"#050c15",borderRadius:"4px",padding:"6px 9px",margin:"8px 0",display:"flex",justifyContent:"space-between",alignItems:"baseline"}}>
                          <span style={{fontSize:"8px",color:"#1e4058"}}>{MONEDA(r,mkt)==="USD"?"PRECIO USD":"PRECIO ARS"}</span>
                          {r.price != null
                            ? <span style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color:r.fromCsv?"#00d4ff":r.real?"#d0ecff":"#2a4a5a"}}>{FP(r.price,MONEDA(r,mkt))}</span>
                            : <span style={{fontSize:"10px",color:"#1e4058",fontStyle:"italic"}}>buscando precio…</span>
                          }
                        </div>

                        {[{l:"→ ENTRADA",v:s.entry,c:buy?"#00ff9d":"#ff9040"},{l:"🛡 STOP",v:s.sl,c:"#ff3355"},{l:"TP2",v:s.tp2,c:"#00ff9d"},{l:"TP3",v:s.tp3,c:"#00d4ff"}].filter(x=>x.v).map(x=>{
                          const pct=((x.v-r.price)/r.price*100).toFixed(1);
                          return <div key={x.l} style={{display:"flex",justifyContent:"space-between",padding:"3px 0",borderBottom:"1px solid #091520",fontSize:"9px"}}>
                            <span style={{color:"#2e5060"}}>{x.l}</span>
                            <div style={{display:"flex",gap:"7px"}}>
                              <span style={{color:x.c,fontWeight:700}}>{FP(x.v,MONEDA(r,mkt))}</span>
                              <span style={{fontSize:"8px",color:+pct>0?"#00ff9d":+pct<0?"#ff3355":"#ffd700"}}>{+pct>0?"+":""}{pct}%</span>
                            </div>
                          </div>;
                        })}

                        <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:"3px",marginTop:"8px"}}>
                          {[{l:"R/R",v:`${s.rr}x`,c:s.rr>=2?"#00ff9d":"#ffd700"},{l:"RSI",v:s.rsi,c:s.rsi>70?"#ff3355":s.rsi<30?"#00ff9d":"#ffd700"},{l:"EVO",v:s.evo_prob,c:s.evo_prob>=0.6?"#ff9040":"#ffd700"},{l:"CONF",v:`${s.conf}%`,c:SC[s.sig]},{l:"EF",v:`${r.bt.hr}%`,c:g.c}].map(m=>
                            <div key={m.l} style={{textAlign:"center",padding:"3px",background:"#050c15",borderRadius:"3px",border:"1px solid #0a1d2e"}}>
                              <div style={{fontSize:"7px",color:"#1e4058"}}>{m.l}</div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:m.c}}>{m.v}</div>
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* RANKING */}
            {tab==="rank"&&(
              <div className="fade">
                <div style={{display:"flex",gap:"4px",marginBottom:"8px",flexWrap:"wrap"}}>
                  {[["conf","Confianza"],["hr","Eficacia"],["sh","Sharpe"],["ca15","FXCA16 Score"],["evo","EVO Prob"]].map(([k,l])=>
                    <button key={k} className={`btn ${sort===k?"on":"off"}`} onClick={()=>setSort(k)}>{l}</button>
                  )}
                </div>
                <div className="card" style={{overflowX:"auto"}}>
                  <table>
                    <thead><tr><th>#</th><th>TICKER</th><th>MKT</th><th>PRECIO</th><th>SEÑAL</th><th>P80</th><th>FX</th><th>EVO</th><th>FXCA16</th><th>CONF</th><th>TREND</th><th>EF%</th><th>SHARPE</th><th>EQ</th><th>CURVA</th></tr></thead>
                    <tbody>
                      {srtd.map((r,i)=>{
                        const s=r.sig,g=GR(r.bt.hr);
                        return <tr key={r.ticker} onClick={()=>{setSel(r);setTab("det");}}>
                          <td style={{color:"#1e4058"}}>{i+1}</td>
                          <td style={{color:"#00ff9d",fontWeight:700,fontSize:"11px"}}>{r.ticker}</td>
                          <td><span style={{fontSize:"8px",color:MONEDA(r,mkt)==="USD"?"#00d4ff":"#ffd700",fontWeight:700}}>{MONEDA(r,mkt)}</span></td>
                          <td style={{fontFamily:"'Bebas Neue'",fontSize:"13px",color:r.fromCsv?"#00d4ff":r.real?"#d0ecff":"#2e5060"}}>{r.price!=null?FP(r.price,MONEDA(r,mkt)):"—"}</td>
                          <td>{s?.sig&&s.sig!=="NEUTRAL"?<span className="badge" style={{background:SC[s.sig]+"18",color:SC[s.sig],border:`1px solid ${SC[s.sig]}35`}}>{s.sig}</span>:<span style={{color:s?.corr_dup?"#ff9040":"#ffd700",fontSize:"8px"}}>{s?.corr_dup?`CORR(${s.corr_dup})`:"NEUTRAL"}</span>}</td>
                          <td style={{fontSize:"9px"}}>{s?.above_p80?<span style={{color:"#00ff9d",fontWeight:700}}>✓</span>:<span style={{color:"#1e4058"}}>─</span>}</td>
                          <td style={{color:"#00d4ff",fontSize:"10px"}}>{s?.fx_sc??"─"}</td>
                          <td style={{color:"#ff9040",fontSize:"10px"}}>{s?.evo_sc??"─"}</td>
                          <td><FXCA16Badge score={s?.ca15_score??0}/></td>
                          <td style={{color:s?SC[s.sig]:"#2e5060",fontWeight:700}}>{s?.conf??"─"}%</td>
                          <td style={{fontSize:"8px",color:s?TC[s.trend]:"#2e5060"}}>{s?`${TI[s.trend]} ${s.trend}`:"─"}</td>
                          <td style={{color:g.c,fontWeight:700}}>{r.bt.hr}%</td>
                          <td style={{color:r.bt.sh>=1?"#00ff9d":"#ffd700"}}>{r.bt.sh}</td>
                          <td style={{color:r.bt.eq>=100?"#00ff9d":"#ff3355",fontWeight:600}}>{r.bt.eq}</td>
                          <td><Curve curve={r.bt.curve}/></td>
                        </tr>;
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* DETALLE */}
            {tab==="det"&&(
              <div className="fade">
                <div style={{display:"flex",gap:"3px",flexWrap:"wrap",marginBottom:"10px"}}>
                  {rows.map(r=>{const g=GR(r.bt.hr);return <button key={r.ticker} className={`btn ${sel?.ticker===r.ticker?"on":"off"}`} onClick={()=>{
  if (!r.bt || r.bt.n === 0) {
    const data = rowDataRef.current[r.ticker];
    if (data) {
      const W2 = (optApplied && optParams[r.ticker]?.w) || W;
      const bt = backtest(data, W2);
      setRows(prev => prev.map(p => p.ticker===r.ticker ? {...p, bt} : p));
      setSel({...r, bt});
      return;
    }
  }
  setSel(r);
}} style={{color:sel?.ticker===r.ticker?undefined:g.c}}>{r.ticker}{r.real?" 📡":""}</button>;})}
                </div>
                {!sel?<div style={{textAlign:"center",padding:"40px",color:"#1a3848"}}>Seleccioná una acción arriba</div>:
                (()=>{
                  const s=sel.sig,g=GR(sel.bt.hr),buy=s?.sig?.includes("COMPRA");
                  return <div>
                    {/* Header */}
                    <div style={{display:"flex",gap:"12px",alignItems:"flex-start",marginBottom:"12px",flexWrap:"wrap"}}>
                      <div style={{flex:1}}>
                        <div style={{display:"flex",alignItems:"center",gap:"10px",marginBottom:"4px",flexWrap:"wrap"}}>
                          <span style={{fontFamily:"'Bebas Neue'",fontSize:"38px",color:"#00ff9d",letterSpacing:".06em",lineHeight:1}}>{sel.ticker}</span>
                          <FXCA16Badge score={s?.ca15_score??0}/>
                          <span style={{fontSize:"9px",color:sel.real?"#00ff9d":"#ffd700",fontWeight:700}}>{sel.real?"📡 REAL":"🔬 SIM"}</span>
                          {s?.above_p80&&<span style={{fontSize:"9px",color:"#ffd700",fontWeight:700,background:"#ffd70015",border:"1px solid #ffd70030",padding:"1px 7px",borderRadius:"3px"}}>TOP P80 ★</span>}
                        </div>
                        <div style={{fontSize:"9px",color:"#2e5060",marginBottom:"4px"}}>{sel.name} · {sel.sector}</div>
                        {sel.price!=null
  ? <div style={{fontFamily:"'Bebas Neue'",fontSize:"32px",color:sel.fromCsv?"#00d4ff":sel.real?"#d0ecff":"#2a4a5a"}}>{FP(sel.price,MONEDA(sel,mkt))}</div>
  : <div style={{fontSize:"11px",color:"#1e4058",padding:"8px 0",fontStyle:"italic"}}>⏳ buscando precio real…</div>}
                        {s&&<div style={{marginTop:"6px",display:"flex",gap:"8px",flexWrap:"wrap"}}>
                          <span className="badge" style={{background:SC[s.sig]+"20",color:SC[s.sig],border:`1px solid ${SC[s.sig]}40`}}>{s.sig}</span>
                          <span style={{color:TC[s.trend],fontSize:"10px"}}>{TI[s.trend]} {s.trend}</span>
                        </div>}
                      </div>
                      <div style={{textAlign:"center",padding:"12px 20px",background:g.c+"10",border:`1px solid ${g.c}30`,borderRadius:"6px"}}>
                        <div style={{fontFamily:"'Bebas Neue'",fontSize:"38px",color:g.c,lineHeight:1}}>{g.l}</div>
                        <div style={{fontSize:"9px",color:g.c}}>{sel.bt.hr}%</div>
                      </div>
                    </div>

                    {/* Score breakdown */}
                    {s&&<div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                      <div style={{fontSize:"8px",color:"#1e4058",letterSpacing:".12em",marginBottom:"10px"}}>SCORE COMBINADO FXCA16</div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"6px",marginBottom:"10px"}}>
                        {[{l:"FX-TÉCNICO",v:s.fx_sc,c:"#00d4ff",sub:"RSI+MACD+BB+ATR"},{l:"EVO-SCORE",v:s.evo_sc,c:"#ff9040",sub:"Score+Vol+Mom"},{l:"COMBINADO",v:s.final_sc,c:"#00ff9d",sub:"65% FX + 35% EVO"}].map(x=>
                          <div key={x.l} style={{textAlign:"center",padding:"10px 8px",background:"#050c15",borderRadius:"4px",border:`1px solid ${x.c}20`}}>
                            <div style={{fontSize:"7px",color:"#1e4058",marginBottom:"2px"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"28px",color:x.c,lineHeight:1}}>{x.v}</div>
                            <div style={{fontSize:"7px",color:x.c,opacity:.6,marginTop:"2px"}}>{x.sub}</div>
                          </div>
                        )}
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"6px"}}>
                        {[{l:"FXCA16 SCORE",v:`${s.ca15_score}/3`,c:s.ca15_score===3?"#00ff9d":s.ca15_score===2?"#ffd700":"#ff9040"},{l:"EVO PROB",v:s.evo_prob,c:s.evo_prob>=0.65?"#00ff9d":s.evo_prob>=0.5?"#ffd700":"#ff9040"},{l:"VOL 24H",v:`${s.vol_24h}x`,c:s.vol_24h>=1.5?"#00ff9d":s.vol_24h>=1?"#ffd700":"#ff9040"},{l:"PCT 6H",v:`${s.pct6h>=0?"+":""}${(s.pct6h*100).toFixed(2)}%`,c:s.pct6h>0?"#00ff9d":"#ff3355"}].map(x=>
                          <div key={x.l} style={{textAlign:"center",padding:"7px",background:"#050c15",borderRadius:"3px"}}>
                            <div style={{fontSize:"7px",color:"#1e4058",marginBottom:"2px"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.v}</div>
                          </div>
                        )}
                      </div>
                    </div>}

                    {s&&<div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"10px",marginBottom:"10px"}}>
                      <div className="card" style={{padding:"12px"}}>
                        <div style={{fontSize:"8px",color:"#1e4058",letterSpacing:".12em",marginBottom:"8px"}}>NIVELES · {W}D</div>
                        {[{l:"🛡 STOP",v:s.sl,c:"#ff3355"},{l:"◎ PRECIO",v:s.px,c:"#aec8d8"},{l:"→ ENTRADA",v:s.entry,c:buy?"#00ff9d":"#ff9040"},{l:"TP1",v:s.tp1,c:"#5dffb0"},{l:"TP2",v:s.tp2,c:"#00ff9d"},{l:"TP3",v:s.tp3,c:"#00d4ff"}].filter(x=>x.v).map(x=>{
                          const pct=((x.v-s.px)/s.px*100).toFixed(1);
                          return <div key={x.l} style={{display:"flex",justifyContent:"space-between",padding:"5px 7px",background:"#050c15",borderRadius:"3px",marginBottom:"3px",fontSize:"9px",border:`1px solid ${x.c}15`}}>
                            <span style={{color:"#2e5060"}}>{x.l}</span>
                            <div style={{display:"flex",gap:"6px"}}>
                              <span style={{color:x.c,fontFamily:"'Bebas Neue'",fontSize:"13px"}}>${F(x.v)}</span>
                              <span style={{fontSize:"8px",color:+pct>0?"#00ff9d":+pct<0?"#ff3355":"#ffd700"}}>{+pct>0?"+":""}{pct}%</span>
                            </div>
                          </div>;
                        })}
                        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"4px",marginTop:"8px"}}>
                          {[{l:"R/R",v:`${s.rr}x`,c:s.rr>=2?"#00ff9d":"#ffd700"},{l:"ATR",v:`$${s.atr}`,c:"#7ab0c8"},{l:"CONF",v:`${s.conf}%`,c:SC[s.sig]}].map(x=>
                            <div key={x.l} style={{textAlign:"center",padding:"5px",background:"#050c15",borderRadius:"3px"}}>
                              <div style={{fontSize:"7px",color:"#1e4058"}}>{x.l}</div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"13px",color:x.c}}>{x.v}</div>
                            </div>
                          )}
                        </div>
                      </div>
                      <div className="card" style={{padding:"12px"}}>
                        <div style={{fontSize:"8px",color:"#1e4058",letterSpacing:".12em",marginBottom:"8px"}}>INDICADORES FXCA16</div>
                        {[
                          {l:"ROC 10h",v:`${s.roc10>=0?"+":""}${s.roc10}%`,c:s.roc10>1.5?"#00ff9d":s.roc10<-1.5?"#ff3355":"#ffd700"},
                          {l:"ROC 5h", v:`${s.roc5>=0?"+":""}${s.roc5}%`, c:s.roc5>1?"#00ff9d":s.roc5<-1?"#ff3355":"#ffd700"},
                          {l:"Vol.Div.",v:s.volDiv>0?"▲ ACUM":s.volDiv<0?"▼ DIST":"─",c:s.volDiv>0?"#00ff9d":s.volDiv<0?"#ff3355":"#ffd700"},
                          {l:"MACD",   v:(s.macd>0?"▲ ":"▼ ")+Math.abs(s.macd),c:s.macd>0?"#00ff9d":"#ff3355"},
                          {l:"Mom. 5h",v:`${s.mom5>=0?"+":""}${s.mom5}%`,c:s.mom5>=0?"#00ff9d":"#ff3355"},
                          {l:"Régimen",v:s.regime||"neutral",c:s.regime==="bull"?"#00ff9d":s.regime==="bear"?"#ff3355":"#ffd700"},
                          {l:"WF peso", v:s.wfWeight,c:s.wfWeight>=1.05?"#00ff9d":s.wfWeight<=0.95?"#ff3355":"#ffd700"},
                          {l:"H-Factor",v:s.hourFactor,c:s.hourFactor>=1?"#00ff9d":s.hourFactor<0.9?"#ff3355":"#ffd700"},
                          {l:"RSI ref.", v:s.rsi,c:s.rsi>70?"#ff3355":s.rsi<30?"#00ff9d":"#2e5468"},
                          {l:"SMA 20",  v:`$${s.sma20?.toFixed(0)??"─"}`,c:"#8b5cf6"},
                          {l:"SMA 50",  v:`$${s.sma50?.toFixed(0)??"─"}`,c:"#f59e0b"},
                          {l:"BB Sup.", v:`$${s.boll?.u?.toFixed(0)??"─"}`,c:"#3b82f6"},
                          {l:"BB Inf.", v:`$${s.boll?.l?.toFixed(0)??"─"}`,c:"#3b82f6"},
                        ].map(x=>
                          <div key={x.l} style={{display:"flex",justifyContent:"space-between",padding:"4px 0",borderBottom:"1px solid #091520",fontSize:"9px"}}>
                            <span style={{color:"#2e5060"}}>{x.l}</span>
                            <span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                          </div>
                        )}
                      </div>
                    </div>}

                    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(85px,1fr))",gap:"5px",marginBottom:"9px"}}>
                      {[{l:"TRADES",v:sel.bt.n,c:"#8ab0c8"},{l:"WINS",v:sel.bt.hits,c:"#00ff9d"},{l:"LOSSES",v:sel.bt.n-sel.bt.hits,c:"#ff3355"},{l:"EF%",v:`${sel.bt.hr}%`,c:g.c},{l:"AVG RET",v:`${sel.bt.avg>=0?"+":""}${sel.bt.avg}%`,c:sel.bt.avg>=0?"#00ff9d":"#ff3355"},{l:"P.FACTOR",v:`${sel.bt.pf}x`,c:sel.bt.pf>=1.5?"#00ff9d":"#ffd700"},{l:"SHARPE",v:sel.bt.sh,c:sel.bt.sh>=1?"#00ff9d":"#ffd700"},{l:"MAX DD",v:`${sel.bt.dd}%`,c:sel.bt.dd<15?"#00ff9d":"#ff3355"},{l:"EQUITY",v:sel.bt.eq,c:sel.bt.eq>=100?"#00ff9d":"#ff3355"}].map(x=>
                        <div key={x.l} className="card" style={{padding:"7px"}}>
                          <div style={{fontSize:"7px",color:"#1e4058",marginBottom:"2px"}}>{x.l}</div>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.v}</div>
                        </div>
                      )}
                    </div>

                    <div className="card" style={{padding:"10px",marginBottom:"9px"}}>
                      <div style={{fontSize:"8px",color:"#1e4058",marginBottom:"6px"}}>EQUITY CURVE</div>
                      <Curve curve={sel.bt.curve} w={560} h={80}/>
                    </div>

                    <div className="card" style={{padding:"10px"}}>
                      <div style={{fontSize:"8px",color:"#1e4058",marginBottom:"6px"}}>OPERACIONES · {sel.bt.trades.length}</div>
                      <div style={{overflowX:"auto",maxHeight:"220px",overflowY:"auto"}}>
                        <table>
                          <thead><tr><th>#</th><th>SEÑAL</th><th>FXCA16</th><th>ENTRADA</th><th>STOP</th><th>SALIDA</th><th>DÍAS</th><th>MOTIVO</th><th>RET</th><th>RES</th></tr></thead>
                          <tbody>{sel.bt.trades.map((t,i)=>
                            <tr key={i} style={{background:t.win?"#00ff9d06":t.reason==="STOP LOSS"?"#ff335506":"transparent"}}>
                              <td style={{color:"#1e4058"}}>{i+1}</td>
                              <td><span className="badge" style={{background:SC[t.sig]+"15",color:SC[t.sig],border:`1px solid ${SC[t.sig]}30`,fontSize:"8px"}}>{t.sig}</span></td>
                              <td><FXCA16Badge score={t.ca15}/></td>
                              <td style={{color:"#5dffb0",fontSize:"10px"}}>${F(t.entry)}</td>
                              <td style={{color:"#ff3355",fontSize:"9px"}}>{t.sl?`$${F(t.sl)}`:"─"}</td>
                              <td style={{fontWeight:600,fontSize:"10px"}}>${F(t.exit)}</td>
                              <td style={{color:"#1e4058"}}>{t.days}d</td>
                              <td style={{fontSize:"8px",color:t.reason==="TAKE PROFIT"?"#00d4ff":t.reason==="STOP LOSS"?"#ff3355":"#2e5060"}}>{t.reason}</td>
                              <td style={{color:t.ret>=0?"#00ff9d":"#ff3355",fontWeight:700}}>{t.ret>=0?"+":""}{t.ret}%</td>
                              <td><span className="badge" style={{background:t.win?"#00ff9d12":"#ff335512",color:t.win?"#00ff9d":"#ff3355",border:`1px solid ${t.win?"#00ff9d28":"#ff335528"}`,fontSize:"8px"}}>{t.win?"WIN":"LOSS"}</span></td>
                            </tr>
                          )}</tbody>
                        </table>
                      </div>
                    </div>
                  </div>;
                })()}
              </div>
            )}

            {/* OPTIMIZADOR */}
            {tab==="opt"&&(
              <div className="fade">
                <div style={{display:"flex",gap:"8px",alignItems:"center",marginBottom:"10px",flexWrap:"wrap"}}>
                  <button className={`btn ${optRunning?"on":"off"}`} onClick={runOptimizer} disabled={optRunning} style={{padding:"8px 20px"}}>
                    {optRunning?"⏳ Optimizando...":"▶ Ejecutar Optimización"}
                  </button>
                  <div style={{fontSize:"9px",color:"#1e4058"}}>
                    Grid: W × [5,7,10,14] | Peso_FX × [0.5,0.65,0.8] = 12 configs por ticker
                  </div>
                </div>
                {optResults.length > 0 && (
                  <>
                    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",gap:"8px",marginBottom:"12px"}}>
                      {optResults.slice(0,3).map((r,i)=>(
                        <div key={r.ticker} className="card" style={{padding:"11px",borderLeft:`3px solid ${i===0?"#ffd700":i===1?"#aab0c8":"#7a6040"}`}}>
                          <div style={{display:"flex",justifyContent:"space-between",marginBottom:"6px"}}>
                            <span style={{fontFamily:"'Bebas Neue'",fontSize:"20px",color:"#e0f4ff"}}>{r.ticker}</span>
                            <span style={{fontSize:"9px",color:i===0?"#ffd700":i===1?"#aab0c8":"#7a6040",fontWeight:700}}>#{i+1}</span>
                          </div>
                          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"4px",fontSize:"9px"}}>
                            {[{l:"W óptimo",v:r.w_opt,c:"#00d4ff"},{l:"Peso FX",v:r.peso_fx,c:"#ff9040"},{l:"Rendimiento",v:`${r.pct>=0?"+":""}${r.pct}%`,c:r.pct>=0?"#00ff9d":"#ff3355"},{l:"Win Rate",v:`${r.wr}%`,c:r.wr>=50?"#00ff9d":"#ffd700"}].map(x=>(
                              <div key={x.l} style={{padding:"4px 6px",background:"#050c15",borderRadius:"3px"}}>
                                <div style={{fontSize:"7px",color:"#1e4058"}}>{x.l}</div>
                                <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.v}</div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                    <div className="card" style={{overflowX:"auto"}}>
                      <table>
                        <thead><tr><th>#</th><th>TICKER</th><th>W ÓPT</th><th>PESO FX</th><th>TRADES</th><th>WINS</th><th>WIN RATE</th><th>CAPITAL FINAL</th><th>RENDIMIENTO</th></tr></thead>
                        <tbody>
                          {optResults.map((r,i)=>(
                            <tr key={r.ticker} style={{background: optApplied && optParams[r.ticker] ? "#00ff9d05" : "transparent"}}>
                              <td style={{color:"#1e4058"}}>{i+1}</td>
                              <td style={{color:"#00ff9d",fontWeight:700}}>{r.ticker}</td>
                              <td style={{color: optApplied?"#00ff9d":"#00d4ff",fontFamily:"'Bebas Neue'",fontSize:"14px"}}>{r.w_opt}</td>
                              <td style={{color:"#ff9040"}}>{r.peso_fx}</td>
                              <td>{r.trades}</td>
                              <td style={{color:"#00ff9d"}}>{r.wins}</td>
                              <td style={{color:r.wr>=50?"#00ff9d":"#ffd700",fontWeight:700}}>{r.wr}%</td>
                              <td style={{fontFamily:"'Bebas Neue'",fontSize:"13px",color:"#d0ecff"}}>{mkt==="USA"?"$"+r.capital.toLocaleString("en-US",{minimumFractionDigits:0}):"$"+r.capital.toLocaleString("es-AR")}</td>
                              <td style={{color:r.pct>=0?"#00ff9d":"#ff3355",fontWeight:700}}>{r.pct>=0?"+":""}{r.pct}%</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <div style={{marginTop:"8px",fontSize:"8px",color:"#1e4058"}}>
                      💡 Capital inicial simulado: $100.000 | Mismo criterio que el script Python original
                    </div>
                  </>
                )}
                {!optResults.length && !optRunning && (
                  <div style={{textAlign:"center",padding:"40px",color:"#1a3848",fontSize:"11px"}}>
                    Ejecutá el sistema principal primero, luego presioná "Ejecutar Optimización"
                  </div>
                )}
              </div>
            )}

                        {/* ══ TAB: SIMULADOR ══ */}
            {tab==="sim"&&(
              <div className="fade">
                {/* Controles manuales y auto */}
                <div style={{display:"flex",gap:"8px",alignItems:"center",marginBottom:"8px",flexWrap:"wrap"}}>
                  <button className={`btn ${simRunning?"on":"off"}`} onClick={runSimulator} disabled={simRunning||autoSim} style={{padding:"10px 20px",fontSize:"11px"}}>
                    {simRunning?"⏳ Simulando...":"🎲 SIMULAR"}
                  </button>

                  {/* Auto-simulador */}
                  <div style={{display:"flex",gap:"4px",alignItems:"center",background:"#07101a",border:"1px solid #0f2235",borderRadius:"4px",padding:"4px 8px"}}>
                    <span style={{fontSize:"8px",color:"#1e4058"}}>AUTO cada</span>
                    {[1,5,10,30].map(m=>(
                      <button key={m}
                        className={`btn ${autoInterval===m?"on":"off"}`}
                        onClick={()=>setAutoInterval(m)}
                        disabled={autoSim}
                        style={{padding:"3px 7px",fontSize:"9px"}}
                      >{m}m</button>
                    ))}
                  </div>

                  <button
                    className={`btn ${autoSim?"on":"off"}`}
                    onClick={()=> autoSim ? stopAutoSim() : startAutoSim(autoInterval)}
                    style={{
                      padding:"10px 16px",fontSize:"11px",
                      color: autoSim?"#03070e":"#ffd700",
                      borderColor:"#ffd70040",
                      background: autoSim?"#ffd700":undefined,
                      animation: autoSim?"pulse 2s infinite":undefined,
                    }}
                  >
                    {autoSim ? "⏹ DETENER AUTO" : "🤖 AUTO"}
                  </button>
                </div>

                {/* Status auto-sim */}
                <div style={{marginBottom:"12px",fontSize:"9px",display:"flex",gap:"12px",flexWrap:"wrap"}}>
                  {autoSim && autoNext && (
                    <span style={{color:"#ffd700"}}>
                      🤖 Auto-sim activa · próxima en {autoCountdown}s
                    </span>
                  )}
                  {autoCount>0 && <span style={{color:"#2e5068"}}>Auto-sims ejecutadas: <strong style={{color:"#00d4ff"}}>{autoCount}</strong></span>}
                  <span style={{color:"#1e4058"}}>5 USA + 5 Merval aleatorios · espectro 1.5 años · evalúa vs realidad</span>
                </div>

                {/* Historial de accuracy */}
                {simHistory.length>0&&(
                  <div className="card" style={{padding:"10px",marginBottom:"10px"}}>
                    <div style={{fontSize:"8px",color:"#1e4058",marginBottom:"6px"}}>📈 HISTORIAL DE SIMULACIONES ({simHistory.length})</div>
                    <div style={{display:"flex",gap:"6px",flexWrap:"wrap",marginBottom:"6px"}}>
                      {simHistory.slice(-20).reverse().map((s,i)=>(
                        <div key={i} style={{textAlign:"center",padding:"4px 8px",background:s.accuracy>=60?"#00ff9d0a":"#ff335508",border:`1px solid ${s.accuracy>=60?"#00ff9d30":"#ff335530"}`,borderRadius:"3px",minWidth:"52px"}}>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"16px",color:s.accuracy>=60?"#00ff9d":"#ff3355"}}>{s.accuracy}%</div>
                          <div style={{fontSize:"7px",color:"#1e4058"}}>{s.mesesRange||`${s.meses||"?"}m`}</div>
                          <div style={{fontSize:"6px",color:"#142030"}}>{s.runAt?.slice(5,10)}</div>
                        </div>
                      ))}
                    </div>
                    {(()=>{
                      const n = simHistory.length;
                      if (!n) return null;
                      const avgAcc = (simHistory.reduce((a,s)=>a+s.accuracy,0)/n).toFixed(1);
                      const recent5 = simHistory.slice(-5);
                      const old5    = simHistory.slice(0, Math.min(5, Math.floor(n/2)));
                      const trendVal = n>=6
                        ? (recent5.reduce((a,s)=>a+s.accuracy,0)/recent5.length
                         - old5.reduce((a,s)=>a+s.accuracy,0)/old5.length).toFixed(1)
                        : null;
                      const learning = n>=3 && trendVal!==null && +trendVal>0;
                      const nDyn = Object.keys(dynParamsRef.current).length;
                      return (
                        <div>
                          <div style={{fontSize:"9px",color:"#2e5468",display:"flex",gap:"12px",flexWrap:"wrap"}}>
                            <span>Promedio: <strong style={{color:"#00d4ff"}}>{avgAcc}%</strong></span>
                            {trendVal && <span style={{color:+trendVal>=0?"#00ff9d":"#ff3355"}}>
                              Tendencia: {+trendVal>=0?"+":""}{trendVal}% {learning?"📈 APRENDIENDO":"📉"}
                            </span>}
                            <span style={{color:"#ffd700"}}>Tickers calibrados: <strong>{nDyn}</strong></span>
                          </div>
                          {/* Mini curva de accuracy */}
                          {n>=3&&<div style={{marginTop:"6px"}}>
                            <svg width="100%" height="28" style={{display:"block"}}>
                              {simHistory.map((s,i)=>{
                                const x=`${(i/(n-1||1))*100}%`;
                                const y=28-(s.accuracy/100)*24;
                                const c=s.accuracy>=60?"#00ff9d":"#ff3355";
                                return <circle key={i} cx={x} cy={y} r="3" fill={c} opacity="0.8"/>;
                              })}
                              {simHistory.length>=2&&<polyline
                                points={simHistory.map((s,i)=>`${(i/(n-1))*100}%,${28-(s.accuracy/100)*24}`).join(" ")}
                                fill="none" stroke="#00d4ff" strokeWidth="1" opacity="0.4"
                              />}
                            </svg>
                            <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px",color:"#1e4058"}}>
                              <span>Sim 1</span><span>Sim {n}</span>
                            </div>
                          </div>}
                        </div>
                      );
                    })()}
                  </div>
                )}

                {/* Resultados de la última simulación */}
                {simResults.length>0&&(
                  <div>
                    <div style={{fontSize:"9px",color:"#1e4058",marginBottom:"6px"}}>
                      ÚLTIMA SIMULACIÓN — fechas individuales por ticker — Accuracy: <strong style={{color:simResults.filter(r=>r.hit).length/simResults.length>=0.6?"#00ff9d":"#ffd700"}}>{(simResults.filter(r=>r.hit).length/simResults.length*100).toFixed(0)}%</strong>
                    </div>
                    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(260px,1fr))",gap:"8px"}}>
                      {simResults.map(r=>{
                        const fund = r.fundamentals;
                        return (
                          <div key={r.ticker} className="card" style={{padding:"10px",borderLeft:`3px solid ${r.hit?"#00ff9d":"#ff3355"}`}}>
                            <div style={{display:"flex",justifyContent:"space-between",marginBottom:"6px"}}>
                              <div>
                                <span style={{fontFamily:"'Bebas Neue'",fontSize:"20px",color:"#e0f4ff"}}>{r.ticker}</span>
                                <span style={{fontSize:"8px",color:r.moneda==="USD"?"#00d4ff":"#ffd700",marginLeft:"6px",fontWeight:700}}>{r.moneda}</span>
                                <span style={{fontSize:"8px",color:"#2e5068",marginLeft:"4px"}}>{r.panel}</span>
                                <div style={{fontSize:"7px",color:"#1e4058",marginTop:"2px"}}>📅 {r.simDate} · <span style={{color:"#ffd700"}}>{r.simDateLabel}</span></div>
                              </div>
                              <span style={{fontSize:"9px",color:r.hit?"#00ff9d":"#ff3355",fontWeight:700}}>{r.hit?"✓ ACIERTO":"✗ FALLO"}</span>
                            </div>
                            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"4px",fontSize:"9px",marginBottom:"6px"}}>
                              <div style={{padding:"4px",background:"#050c15",borderRadius:"3px"}}>
                                <div style={{color:"#1e4058",fontSize:"7px"}}>PREDICCIÓN</div>
                                <div style={{color:r.predicted.includes("COMPRA")?"#00ff9d":r.predicted.includes("VENTA")?"#ff3355":"#ffd700",fontWeight:700}}>{r.predicted}</div>
                              </div>
                              <div style={{padding:"4px",background:"#050c15",borderRadius:"3px"}}>
                                <div style={{color:"#1e4058",fontSize:"7px"}}>RETORNO REAL</div>
                                <div style={{color:r.actualRet>=0?"#00ff9d":"#ff3355",fontWeight:700}}>{r.actualRet>=0?"+":""}{r.actualRet}%</div>
                              </div>
                              <div style={{padding:"4px",background:"#050c15",borderRadius:"3px"}}>
                                <div style={{color:"#1e4058",fontSize:"7px"}}>SCORE</div>
                                <div style={{color:"#00d4ff"}}>{r.score}</div>
                              </div>
                              <div style={{padding:"4px",background:"#050c15",borderRadius:"3px"}}>
                                <div style={{color:"#1e4058",fontSize:"7px"}}>EVO PROB</div>
                                <div style={{color:"#ff9040"}}>{r.evoProb}</div>
                              </div>
                            </div>
                            {fund&&(
                              <div style={{padding:"5px",background:"#050c15",borderRadius:"3px",fontSize:"8px"}}>
                                <div style={{color:"#1e4058",marginBottom:"3px"}}>FUNDAMENTALES</div>
                                <div style={{display:"flex",gap:"6px",flexWrap:"wrap"}}>
                                  {fund.pe && <span style={{color:"#7ab0c8"}}>P/E: <strong>{fund.pe}</strong></span>}
                                  {fund.rev_growth!=null && <span style={{color:fund.rev_growth>0?"#00ff9d":"#ff3355"}}>Rev: <strong>{fund.rev_growth}%</strong></span>}
                                  {fund.news_sentiment && <span style={{color:fund.news_sentiment==="positive"?"#00ff9d":fund.news_sentiment==="negative"?"#ff3355":"#ffd700"}}>📰 {fund.news_sentiment}</span>}
                                  {fund.analyst && <span style={{color:fund.analyst==="buy"?"#00ff9d":fund.analyst==="sell"?"#ff3355":"#ffd700"}}>👥 {fund.analyst}</span>}
                                </div>
                                {fund.summary&&<div style={{color:"#2e5468",marginTop:"3px",fontSize:"7px"}}>{fund.summary}</div>}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}

                {!simResults.length&&!simRunning&&(
                  <div style={{textAlign:"center",padding:"40px",color:"#1a3848",fontSize:"11px"}}>
                    Presioná "🎲 NUEVA SIMULACIÓN" para empezar
                  </div>
                )}
              </div>
            )}

            {/* ══ TAB: APRENDIZAJE ══ */}
            {tab==="learn"&&(()=>{
              const hasSims = simHistory.length > 0;
              const hasTickers = Object.keys(learningData).filter(k=>learningData[k].totalSims>0).length>0;
              return (
                <div className="fade">
                  {/* Sub-tabs */}
                  <div style={{display:"flex",gap:"5px",marginBottom:"12px"}}>
                    <button className={`btn ${learnView==="tickers"?"on":"off"}`} onClick={()=>setLearnView("tickers")}>📊 Por Ticker</button>
                    <button className={`btn ${learnView==="history"?"on":"off"}`} onClick={()=>setLearnView("history")}>📋 Historial de Simulaciones ({simHistory.length})</button>
                  </div>

                  {/* Vista por ticker */}
                  {learnView==="tickers"&&(
                    !hasTickers ? (
                      <div style={{textAlign:"center",padding:"40px",color:"#1a3848",fontSize:"11px"}}>
                        Ejecutá simulaciones para generar aprendizaje
                      </div>
                    ) : (
                      <div className="card" style={{overflowX:"auto"}}>
                        <table>
                          <thead><tr><th>TICKER</th><th>PANEL</th><th>SIMS</th><th>WIN RATE</th><th>MEJOR W</th><th>CONF.</th><th>ÚLTIMA SIM</th><th>DYN</th></tr></thead>
                          <tbody>
                            {Object.entries(learningData)
                              .filter(([,v])=>v.totalSims>0)
                              .sort(([,a],[,b])=>b.winRate-a.winRate)
                              .map(([tk,v])=>{
                                const isUSA=TICKERS_USA.find(t=>t.ticker===tk);
                                const base=TICKER_CONFIDENCE[tk]||0;
                                const dynConf=v.winRate>=0.6?base+0.05:v.winRate<=0.3?base-0.05:base;
                                return (
                                  <tr key={tk}>
                                    <td style={{color:"#00ff9d",fontWeight:700}}>{tk}</td>
                                    <td style={{fontSize:"9px",color:isUSA?"#00d4ff":"#ffd700"}}>{isUSA?"🇺🇸":"🇦🇷"}</td>
                                    <td style={{color:"#7ab0c8"}}>{v.totalSims}</td>
                                    <td style={{color:v.winRate>=0.6?"#00ff9d":v.winRate>=0.4?"#ffd700":"#ff3355",fontWeight:700}}>{(v.winRate*100).toFixed(0)}%</td>
                                    <td style={{color:"#00d4ff",fontFamily:"'Bebas Neue'",fontSize:"14px"}}>{v.bestW}</td>
                                    <td style={{color:dynConf>0?"#00ff9d":dynConf<0?"#ff3355":"#ffd700"}}>{dynConf>0?"+":""}{dynConf.toFixed(2)}</td>
                                    <td style={{fontSize:"8px",color:"#1e4058"}}>{v.sessions?.slice(-1)[0]?.simDate||"—"}</td>
                                    <td style={{fontSize:"8px"}}>
                                      {dynParamsRef.current[tk]
                                        ? <span style={{color:"#00ff9d",fontSize:"7px",fontWeight:700}}>✓ ACTIVO</span>
                                        : <span style={{color:"#1e4058",fontSize:"7px"}}>pendiente</span>}
                                    </td>
                                  </tr>
                                );
                              })}
                          </tbody>
                        </table>
                      </div>
                    )
                  )}

                  {/* Historial de simulaciones */}
                  {learnView==="history"&&(
                    !hasSims ? (
                      <div style={{textAlign:"center",padding:"40px",color:"#1a3848",fontSize:"11px"}}>
                        Sin historial aún
                      </div>
                    ) : (
                      <div>
                        {[...simHistory].reverse().map((s,i)=>(
                          <div key={i} className="card" style={{padding:"12px",marginBottom:"10px",borderLeft:`3px solid ${s.accuracy>=60?"#00ff9d":"#ff3355"}`}}>
                            {/* Header sesión */}
                            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"8px",flexWrap:"wrap",gap:"6px"}}>
                              <div>
                                <span style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color:s.accuracy>=60?"#00ff9d":"#ff3355"}}>{s.accuracy}%</span>
                                <span style={{fontSize:"8px",color:"#2e5068",marginLeft:"8px"}}>{s.hits}/{s.total} aciertos</span>
                              </div>
                              <div style={{fontSize:"8px",color:"#1e4058",textAlign:"right"}}>
                                <div>{s.runAt?.slice(0,16).replace("T"," ")}</div>
                                <div style={{color:"#2e5068"}}>Rango: {s.mesesRange||"—"}</div>
                              </div>
                            </div>
                            {/* Resultados detallados */}
                            {s.results?.length>0&&(
                              <div style={{overflowX:"auto"}}>
                                <table style={{fontSize:"8px"}}>
                                  <thead>
                                    <tr>
                                      <th>TICKER</th><th>MKT</th><th>FECHA</th><th>ATRÁS</th>
                                      <th>PRED.</th><th>REAL</th><th>SCORE</th><th>EVO</th><th>RES.</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {s.results.map((r,j)=>(
                                      <tr key={j} style={{background:r.hit?"#00ff9d06":"#ff335506"}}>
                                        <td style={{color:"#00ff9d",fontWeight:700}}>{r.ticker}</td>
                                        <td style={{color:r.moneda==="USD"?"#00d4ff":"#ffd700",fontSize:"7px"}}>{r.moneda==="USD"?"🇺🇸":"🇦🇷"}</td>
                                        <td style={{color:"#2e5068"}}>{r.simDate}</td>
                                        <td style={{color:"#1e4058"}}>{r.mesesBack?.toFixed(1)||"—"}m</td>
                                        <td style={{color:r.predicted?.includes("COMPRA")?"#00ff9d":r.predicted?.includes("VENTA")?"#ff3355":"#ffd700",fontWeight:700,fontSize:"7px"}}>{r.predicted?.replace(" FUERTE","★")}</td>
                                        <td style={{color:r.actualRet>0?"#00ff9d":r.actualRet<0?"#ff3355":"#ffd700",fontWeight:700}}>{r.actualRet>0?"+":""}{r.actualRet}%</td>
                                        <td style={{color:"#00d4ff"}}>{r.score}</td>
                                        <td style={{color:"#ff9040"}}>{r.evoProb}</td>
                                        <td><span style={{color:r.hit?"#00ff9d":"#ff3355",fontWeight:700}}>{r.hit?"✓":"✗"}</span></td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    )
                  )}
                </div>
              );
            })()}

                        <div style={{marginTop:"12px",padding:"6px 10px",background:"#050c15",borderRadius:"4px",fontSize:"8px",color:"#0f2235"}}>
              ⚠️ FXCA16 · Precios vía Anthropic Web Search · Histórico sintético · Umbral dinámico P80 · No es asesoramiento financiero.
            </div>
          </div>
        )}
      </div>
    </div>
  );
}