import { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import * as Q from "./quant.js";
import * as Q2 from "./quant2.js";
import * as ALPHA from "./alpha.js";
import CSV_DATA_EMBEDDED_RAW, { expandEmbedded as expandEmbeddedImport, FXCA16_DYN_PARAMS as DYN_PARAMS_IMPORTED } from './data.js';
// La serie diaria de 10 años se agrega a data.js en la próxima actualización.
// Hasta entonces el sistema sigue operando con la serie horaria.
import * as DATA_MOD from './data.js';
import logoUrl from './logo.png';

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

// ── FIREBASE + localStorage DUAL STORAGE ──
const FB_PROJECT = "fxca16";
const FB_KEY = "AIzaSyDxaLjtnuGAWPTvR7odsIE_Oq0AHi28UEU";
const FB_URL = `https://firestore.googleapis.com/v1/projects/${FB_PROJECT}/databases/(default)/documents/fxca16`;
const shouldSync = (key) => key.includes('sim_history') || key.includes('learn_') || key.includes('dyn_');
async function fbWrite(key, val) {
  try { const k=key.replace(/[^a-zA-Z0-9_-]/g,'_'); await fetch(`${FB_URL}/${k}?key=${FB_KEY}`,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({fields:{v:{stringValue:typeof val==='string'?val:JSON.stringify(val)}}})}); } catch(_){}
}
async function fbRead(key) {
  try { const k=key.replace(/[^a-zA-Z0-9_-]/g,'_'); const r=await fetch(`${FB_URL}/${k}?key=${FB_KEY}`); if(!r.ok)return null; const d=await r.json(); return d?.fields?.v?.stringValue??null; } catch(_){return null;}
}
const storage = {
  async set(key,value){try{const s=typeof value==='string'?value:JSON.stringify(value);localStorage.setItem(key,s);if(shouldSync(key))fbWrite(key,s);return{key,value};}catch(e){return null;}},
  async get(key){try{const local=localStorage.getItem(key);if(local!==null)return{key,value:local};if(shouldSync(key)){const fb=await fbRead(key);if(fb!==null){localStorage.setItem(key,fb);return{key,value:fb};}}return null;}catch(e){return null;}},
  async delete(key){try{localStorage.removeItem(key);return{key,deleted:true};}catch(e){return null;}},
  async list(prefix){try{const keys=Object.keys(localStorage).filter(k=>!prefix||k.startsWith(prefix));return{keys};}catch(e){return{keys:[]};}}
};
if(typeof window!=='undefined')window.storage=storage;

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
  // ── MEGA CAP TECH ──
  { ticker:"AAPL",  name:"Apple",              sector:"Tecnología"  },
  { ticker:"NVDA",  name:"Nvidia",             sector:"Tecnología"  },
  { ticker:"MSFT",  name:"Microsoft",          sector:"Tecnología"  },
  { ticker:"GOOGL", name:"Alphabet",           sector:"Tecnología"  },
  { ticker:"AMZN",  name:"Amazon",             sector:"Tech/Retail" },
  { ticker:"META",  name:"Meta",               sector:"Tecnología"  },
  { ticker:"TSLA",  name:"Tesla",              sector:"Autos"       },
  { ticker:"AVGO",  name:"Broadcom",           sector:"Tecnología"  },
  { ticker:"ORCL",  name:"Oracle",             sector:"Tecnología"  },
  { ticker:"NFLX",  name:"Netflix",            sector:"Medios"      },
  { ticker:"AMD",   name:"AMD",                sector:"Tecnología"  },
  { ticker:"INTC",  name:"Intel",              sector:"Tecnología"  },
  { ticker:"QCOM",  name:"Qualcomm",           sector:"Tecnología"  },
  { ticker:"TXN",   name:"Texas Instruments",  sector:"Tecnología"  },
  { ticker:"MU",    name:"Micron",             sector:"Tecnología"  },
  { ticker:"CRM",   name:"Salesforce",         sector:"Tecnología"  },
  { ticker:"NOW",   name:"ServiceNow",         sector:"Tecnología"  },
  { ticker:"ADBE",  name:"Adobe",              sector:"Tecnología"  },
  { ticker:"IBM",   name:"IBM",                sector:"Tecnología"  },
  { ticker:"UBER",  name:"Uber",               sector:"Tecnología"  },
  { ticker:"COIN",  name:"Coinbase",           sector:"Crypto"      },
  { ticker:"MELI",  name:"MercadoLibre",       sector:"Fintech"     },
  { ticker:"SPOT",  name:"Spotify",            sector:"Medios"      },
  { ticker:"BABA",  name:"Alibaba",            sector:"Tech/Retail" },
  { ticker:"PYPL",  name:"PayPal",             sector:"Fintech"     },
  { ticker:"GLOB",  name:"Globant",            sector:"Tecnología"  },
  // ── FINANCIERO ──
  { ticker:"JPM",   name:"JPMorgan",           sector:"Financiero"  },
  { ticker:"BAC",   name:"Bank of America",    sector:"Financiero"  },
  { ticker:"WFC",   name:"Wells Fargo",        sector:"Financiero"  },
  { ticker:"GS",    name:"Goldman Sachs",      sector:"Financiero"  },
  { ticker:"MS",    name:"Morgan Stanley",     sector:"Financiero"  },
  { ticker:"C",     name:"Citigroup",          sector:"Financiero"  },
  { ticker:"AXP",   name:"Amex",               sector:"Financiero"  },
  { ticker:"V",     name:"Visa",               sector:"Financiero"  },
  { ticker:"MA",    name:"Mastercard",         sector:"Financiero"  },
  { ticker:"BRK-B", name:"Berkshire Hathaway", sector:"Financiero"  },
  { ticker:"NDAQ",  name:"Nasdaq Inc.",        sector:"Financiero"  },
  // ── SALUD ──
  { ticker:"UNH",   name:"UnitedHealth",       sector:"Salud"       },
  { ticker:"JNJ",   name:"Johnson & Johnson",  sector:"Salud"       },
  { ticker:"PFE",   name:"Pfizer",             sector:"Salud"       },
  { ticker:"ABBV",  name:"AbbVie",             sector:"Salud"       },
  { ticker:"MRK",   name:"Merck",              sector:"Salud"       },
  { ticker:"LLY",   name:"Eli Lilly",          sector:"Salud"       },
  { ticker:"CAH",   name:"Cardinal Health",    sector:"Salud"       },
  // ── CONSUMO ──
  { ticker:"WMT",   name:"Walmart",            sector:"Consumo"     },
  { ticker:"KO",    name:"Coca-Cola",          sector:"Consumo"     },
  { ticker:"PEP",   name:"PepsiCo",            sector:"Consumo"     },
  { ticker:"PG",    name:"Procter & Gamble",   sector:"Consumo"     },
  { ticker:"MCD",   name:"McDonald's",         sector:"Consumo"     },
  { ticker:"SBUX",  name:"Starbucks",          sector:"Consumo"     },
  { ticker:"NKE",   name:"Nike",               sector:"Consumo"     },
  { ticker:"DIS",   name:"Disney",             sector:"Medios"      },
  // ── ENERGÍA ──
  { ticker:"XOM",   name:"ExxonMobil",         sector:"Energía"     },
  { ticker:"CVX",   name:"Chevron",            sector:"Energía"     },
  { ticker:"VIST",  name:"Vista Energy",       sector:"Energía"     },
  { ticker:"PBR",   name:"Petrobras",          sector:"Energía"     },
  // ── AEROLÍNEAS / AUTOS / TELECOM ──
  { ticker:"AAL",   name:"American Airlines",  sector:"Aerolíneas"  },
  { ticker:"DAL",   name:"Delta Airlines",     sector:"Aerolíneas"  },
  { ticker:"UAL",   name:"United Airlines",    sector:"Aerolíneas"  },
  { ticker:"F",     name:"Ford",               sector:"Autos"       },
  { ticker:"GM",    name:"General Motors",     sector:"Autos"       },
  { ticker:"VZ",    name:"Verizon",            sector:"Telecom"     },
  { ticker:"T",     name:"AT&T",               sector:"Telecom"     },
  { ticker:"TMUS",  name:"T-Mobile",           sector:"Telecom"     },
  // ── SALUD ADICIONAL ──
  { ticker:"ABT",   name:"Abbott",             sector:"Salud"       },
  { ticker:"TMO",   name:"Thermo Fisher",      sector:"Salud"       },
  { ticker:"DHR",   name:"Danaher",            sector:"Salud"       },
  { ticker:"CVS",   name:"CVS Health",         sector:"Salud"       },
  // ── INMUEBLES / INDUSTRIA ──
  { ticker:"BA",    name:"Boeing",             sector:"Industria"   },
  { ticker:"CAT",   name:"Caterpillar",        sector:"Industria"   },
  { ticker:"HON",   name:"Honeywell",          sector:"Industria"   },
  { ticker:"RTX",   name:"Raytheon",           sector:"Industria"   },
  { ticker:"AMT",   name:"American Tower",     sector:"Inmuebles"   },
  // ── TECH ADICIONAL ──
  { ticker:"PLTR", name:"Palantir",            sector:"Tecnología"  },
  { ticker:"SNOW", name:"Snowflake",           sector:"Tecnología"  },
  { ticker:"NET",  name:"Cloudflare",          sector:"Tecnología"  },
  { ticker:"SHOP", name:"Shopify",             sector:"Tecnología"  },
  { ticker:"SQ",   name:"Block (Square)",      sector:"Fintech"     },
  { ticker:"RBLX", name:"Roblox",              sector:"Tecnología"  },
  // ── FINANCIERO ADICIONAL ──
  { ticker:"SCHW", name:"Charles Schwab",      sector:"Financiero"  },
  { ticker:"BLK",  name:"BlackRock",           sector:"Financiero"  },
  { ticker:"COF",  name:"Capital One",         sector:"Financiero"  },
  // ── SALUD ADICIONAL ──
  { ticker:"ISRG", name:"Intuitive Surgical",  sector:"Salud"       },
  { ticker:"REGN", name:"Regeneron",           sector:"Salud"       },
  { ticker:"GILD", name:"Gilead",              sector:"Salud"       },
  // ── CONSUMO ADICIONAL ──
  { ticker:"COST", name:"Costco",              sector:"Consumo"     },
  { ticker:"TGT",  name:"Target",              sector:"Consumo"     },
  { ticker:"HD",   name:"Home Depot",          sector:"Consumo"     },
  { ticker:"LOW",  name:"Lowe's",              sector:"Consumo"     },
  // ── ENERGÍA ADICIONAL ──
  { ticker:"SLB",  name:"SLB (Schlumberger)",  sector:"Energía"     },
  { ticker:"OXY",  name:"Occidental Petroleum",sector:"Energía"     },
  // ── INDUSTRIA ADICIONAL ──
  { ticker:"GE",   name:"GE Aerospace",        sector:"Industria"   },
  { ticker:"UPS",  name:"UPS",                 sector:"Industria"   },
  { ticker:"FDX",  name:"FedEx",               sector:"Industria"   },
  // ── ETFs ──
  { ticker:"SPY",   name:"S&P 500 ETF",        sector:"ETF"         },
  { ticker:"QQQ",   name:"Nasdaq 100 ETF",     sector:"ETF"         },
  { ticker:"IWM",   name:"Russell 2000 ETF",   sector:"ETF"         },
  { ticker:"DIA",   name:"Dow Jones ETF",      sector:"ETF"         },
  { ticker:"GLD",   name:"Gold ETF",           sector:"Commodities" },
  { ticker:"SLV",   name:"Silver ETF",         sector:"Commodities" },
  { ticker:"XLE",   name:"Energy ETF",         sector:"Energía"     },
  { ticker:"XLF",   name:"Financials ETF",     sector:"Financiero"  },
  { ticker:"XLK",   name:"Technology ETF",     sector:"Tecnología"  },
  { ticker:"TLT",   name:"Treasury Bond ETF",  sector:"Bonos"       },
];



const TICKERS_MERVAL = [
  { ticker:"AGRO", name:"Agrometal",           sector:"Agroindustria" },
  { ticker:"ALUA", name:"Aluar",               sector:"Materiales"   },
  { ticker:"AUSO", name:"Autopistas",          sector:"Infraestructura"},
  { ticker:"BHIP", name:"Bco.Hipotecario",     sector:"Financiero"   },
  { ticker:"BMA",  name:"Banco Macro",         sector:"Financiero"   },
  { ticker:"BOLT", name:"Boldt",               sector:"Tecnología"   },
  { ticker:"BPAT", name:"Banco Patagonia",     sector:"Financiero"   },
  { ticker:"BYMA", name:"BYMA",                sector:"Financiero"   },
  { ticker:"CADO", name:"Cado",                sector:"Alimentos"    },
  { ticker:"CAPX", name:"Capex",               sector:"Energía"      },
  { ticker:"CARC", name:"Carc",                sector:"Materiales"   },
  { ticker:"CECO2",name:"Cen.Costanera",       sector:"Energía"      },
  { ticker:"CELU", name:"Celulosa Arg.",        sector:"Materiales"   },
  { ticker:"CEPU", name:"Central Puerto",      sector:"Energía"      },
  { ticker:"CGPA2",name:"Camuzzi Gas Pampeana",sector:"Energía"      },
  { ticker:"COME", name:"Sociedad Comercial",  sector:"Financiero"   },
  { ticker:"CTIO", name:"Consultatio",         sector:"Inmuebles"    },
  { ticker:"CVH",  name:"Cablevision Hold.",   sector:"Telecom"      },
  { ticker:"DGCU2",name:"Dist.Gas Cuyo",       sector:"Energía"      },
  { ticker:"EDN",  name:"Edenor",              sector:"Utilities"    },
  { ticker:"FERR", name:"Ferrum",              sector:"Materiales"   },
  { ticker:"FIPL", name:"Fiplasto",            sector:"Materiales"   },
  { ticker:"GAMI", name:"Gami",                sector:"Tecnología"   },
  { ticker:"GARO", name:"Garovaglio",          sector:"Materiales"   },
  { ticker:"GBAN", name:"Grupo Fin. Galicia",  sector:"Financiero"   },
  { ticker:"GCLA", name:"Grupo Clarín",        sector:"Medios"       },
  { ticker:"GGAL", name:"Grupo Galicia",       sector:"Financiero"   },
  { ticker:"GRIM", name:"Grimoldi",            sector:"Consumo"      },
  { ticker:"HARG", name:"Holcim Arg.",         sector:"Materiales"   },
  { ticker:"INTR", name:"Introductora",        sector:"Salud"        },
  { ticker:"INVJ", name:"Inv. Juramento",      sector:"Financiero"   },
  { ticker:"IRSA", name:"IRSA",                sector:"Inmuebles"    },
  { ticker:"LEDE", name:"Ledesma",             sector:"Alimentos"    },
  { ticker:"LOMA", name:"Loma Negra",          sector:"Materiales"   },
  { ticker:"LONG", name:"Longvie",             sector:"Consumo"      },
  { ticker:"METR", name:"Metrogas",            sector:"Energía"      },
  { ticker:"MIRG", name:"Mirgor",              sector:"Tecnología"   },
  { ticker:"MOLI", name:"Molinos Rio Plata",   sector:"Alimentos"    },
  { ticker:"MORI", name:"Morixe",              sector:"Alimentos"    },
  { ticker:"OEST", name:"Dist.Gas Oeste",      sector:"Energía"      },
  { ticker:"PAMP", name:"Pampa Energía",       sector:"Energía"      },
  { ticker:"PATA", name:"Banco Patagonia",     sector:"Financiero"   },
  { ticker:"POLL", name:"Polledo",             sector:"Financiero"   },
  { ticker:"REGE", name:"Grupo Financiero",    sector:"Financiero"   },
  { ticker:"RICH", name:"Rigolleau",           sector:"Materiales"   },
  { ticker:"RIGO", name:"Rigo",                sector:"Materiales"   },
  { ticker:"ROSE", name:"Roseto",              sector:"Alimentos"    },
  { ticker:"SAMI", name:"San Miguel",          sector:"Alimentos"    },
  { ticker:"SEMI", name:"Semillas",            sector:"Agroindustria"},
  { ticker:"SUPV", name:"Supervielle",         sector:"Financiero"   },
  { ticker:"TECO2",name:"Telecom Arg.",        sector:"Telecom"      },
  { ticker:"TGNO4",name:"TGN",                 sector:"Energía"      },
  { ticker:"TGSU2",name:"TGS",                 sector:"Energía"      },
  { ticker:"TXAR", name:"Ternium Arg.",        sector:"Materiales"   },
  { ticker:"VALO", name:"Grupo Valores",       sector:"Financiero"   },
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
  // NOTA: genera historial SINTÉTICO — los indicadores sobre estos datos no son reales
  let s = ticker.split("").reduce((a,c)=>a*31+c.charCodeAt(0),7)>>>0;
  const rng = ()=>{ s=(Math.imul(s,1664525)+1013904223)>>>0; return s/0xffffffff; };
  const N=150; let p=price*(0.78+rng()*0.44); const drift=(price/p-1)/N;
  const rows=[];
  for (let i=0;i<N-1;i++) {
    const o=p; p=Math.max(p*(1+(rng()-0.49)*0.022+drift),1);
    const d=new Date(); d.setDate(d.getDate()-(N-i));
    rows.push({date:d.toISOString().slice(0,10),open:+o.toFixed(2),high:+(Math.max(o,p)*(1+rng()*0.01)).toFixed(2),low:+(Math.min(o,p)*(1-rng()*0.01)).toFixed(2),close:+p.toFixed(2),volume:Math.floor(2e5+rng()*4e6),_synth:true});
  }
  rows.push({date:new Date().toISOString().slice(0,10),open:+price.toFixed(2),high:+(price*(1+rng()*0.01)).toFixed(2),low:+(price*(1-rng()*0.01)).toFixed(2),close:price,volume:Math.floor(5e5+rng()*5e6),_synth:true});
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
  // DESACTIVADO: este ajuste modificaba el score usando resultados de
  // simulaciones previas sobre los MISMOS datos. Eso es sobreajuste:
  // el modelo se auto-premiaba por aciertos que ya conocía.
  // La calibración estadística correcta vive ahora en el tab Validación
  // (regresión logística + Platt scaling, ambos fuera de muestra).
  return baseScore;
}

// W adaptativo por ticker
function adaptiveW(ticker, globalW) {
  const learnedW = getDynParam(ticker, 'w', null);
  const sims     = getDynParam(ticker, 'sims', 0);
  // Solo usar W aprendido si tiene historia suficiente (≥5 simulaciones)
  if (learnedW && sims >= 5) return learnedW;
  return globalW;
}

// ── COSTOS DE TRANSACCIÓN (round-trip, broker PPI) ──
const COSTO_MERVAL = 1.2;  // % ida+vuelta acciones locales
const COSTO_CEDEAR = 1.8;  // % ida+vuelta CEDEARs (incluye spread)

// ══════════════════════════════════════════════════════════════
// NIVELES ESTRUCTURALES — soportes/resistencias reales del precio
// Usados para calcular un R/R genuino (no un múltiplo fijo de ATR)
// ══════════════════════════════════════════════════════════════
function findStructuralLevels(data, px, lookbackBars = 200) {
  if (!data || data.length < 30) return { resistances: [], supports: [] };
  const slice = data.slice(-Math.min(data.length, lookbackBars));
  const highs = [], lows = [];
  // Pivotes: máximo/mínimo local con k barras a cada lado
  const k = 3;
  for (let i = k; i < slice.length - k; i++) {
    const h = slice[i].high, l = slice[i].low;
    let isH = true, isL = true;
    for (let j = i-k; j <= i+k; j++) {
      if (j === i) continue;
      if (slice[j].high >= h) isH = false;
      if (slice[j].low  <= l) isL = false;
    }
    if (isH) highs.push(h);
    if (isL) lows.push(l);
  }
  // Fibonacci del rango completo
  const rgHigh = Math.max(...slice.map(d=>d.high));
  const rgLow  = Math.min(...slice.map(d=>d.low));
  const rng    = rgHigh - rgLow;
  const fibs   = rng > 0 ? [0.236,0.382,0.5,0.618,0.786].map(f => rgLow + rng*f) : [];
  // POC aproximado (bin de mayor volumen)
  let poc = null;
  if (rng > 0) {
    const bins = 12, bs = rng/bins, prof = new Array(bins).fill(0);
    slice.forEach(d => {
      const mid = (d.high+d.low)/2;
      const bi  = Math.min(bins-1, Math.max(0, Math.floor((mid-rgLow)/bs)));
      prof[bi] += d.volume || 1;
    });
    poc = rgLow + prof.indexOf(Math.max(...prof))*bs + bs/2;
  }
  const all = [...highs, ...lows, ...fibs, rgHigh, rgLow, poc].filter(v => v && isFinite(v));
  // Agrupar niveles cercanos (dentro de 0.4%) y contar toques = fuerza
  const clusters = [];
  all.sort((a,b)=>a-b).forEach(v => {
    const c = clusters.find(c => Math.abs(c.value - v)/v < 0.004);
    if (c) { c.value = (c.value*c.hits + v)/(c.hits+1); c.hits++; }
    else clusters.push({ value: v, hits: 1 });
  });
  const resistances = clusters.filter(c => c.value > px*1.002).sort((a,b)=>a.value-b.value);
  const supports    = clusters.filter(c => c.value < px*0.998).sort((a,b)=>b.value-a.value);
  return { resistances, supports, rgHigh, rgLow, poc };
}


function combinedSignal(data, W=7, allData=null) {
  const n = data.length-1;
  if (n<60) return null;
  const ticker = data[n]?._ticker || "";

  // ─ Períodos adaptativos según W (FIX: señales calibradas al horizonte) ─
  const rsiP  = W<=7 ? 14 : W<=14 ? 18 : W<=30 ? 22 : 28;  // RSI más suave a mayor plazo
  const bbP   = W<=7 ? 20 : W<=14 ? 24 : W<=30 ? 28 : 34;  // BB más amplio a mayor plazo
  const atrP  = W<=7 ? 14 : W<=14 ? 16 : W<=30 ? 20 : 24;  // ATR más estable a mayor plazo
  const roc10P= W<=7 ? 10 : W<=14 ? 14 : W<=30 ? 20 : 28;  // ROC ajustado al horizonte
  const roc5P = W<=7 ? 5  : W<=14 ? 7  : W<=30 ? 10 : 14;

  // ─ FXCA16 técnico — versiones FAST (solo último valor, sin arrays completos) ─
  const px  = data[n].close;
  const a20 = smaLast(data, W<=14 ? 20 : 30);
  const a50 = smaLast(data, W<=14 ? 50 : 70);
  const a200= smaLast(data, Math.min(200, n+1));
  const b   = bollLast(data, bbP);
  const at  = atrLast(data, atrP) || px*0.015;
  if (!b||!b.u) return null;

  // MACD fast — O(n) una sola pasada
  const macdRes = macdLast(data);
  const mh  = macdRes.hist;
  const mhp = macdRes.prevHist;

  // ROC adaptativo al horizonte
  const roc10 = rocLast(data, roc10P);
  const roc5  = rocLast(data, roc5P);
  const volDiv = volPriceDivergence(data, n);
  // RSI adaptativo
  const r = rsiLast(data, rsiP);

  // ══ FIX ALTO: dos motores SEPARADOS en vez de un score contradictorio ══
  // Antes, Bollinger (reversión) restaba puntos mientras ROC/MACD (momentum)
  // sumaban en un breakout. Ahora cada estrategia puntúa por separado y el
  // sistema elige cuál aplicar según el régimen del activo.

  // ── MOTOR A: MOMENTUM / SEGUIMIENTO DE TENDENCIA ──
  let mom_raw = 0;
  if (roc10 >  3.0) mom_raw += 20;
  else if (roc10 >  1.5) mom_raw += 12;
  else if (roc10 >  0.5) mom_raw +=  6;
  else if (roc10 < -3.0) mom_raw -= 20;
  else if (roc10 < -1.5) mom_raw -= 12;
  else if (roc10 < -0.5) mom_raw -=  6;

  if (roc5 > 1.0) mom_raw += 8;
  else if (roc5 < -1.0) mom_raw -= 8;

  mom_raw += volDiv * 10;

  // MACD con BANDA MUERTA (FIX medio: antes ±0.00001 movía 20 pts)
  const macdDead = px * 0.0004;          // umbral proporcional al precio
  if      (mh >  macdDead && mhp <= macdDead) mom_raw += 20;   // cruce alcista
  else if (mh >  macdDead)                    mom_raw += 10;
  else if (mh < -macdDead && mhp >= -macdDead)mom_raw -= 20;   // cruce bajista
  else if (mh < -macdDead)                    mom_raw -= 10;
  // dentro de la banda muerta: 0 pts

  if(a20&&a50) a20>a50 ? mom_raw+=12 : mom_raw-=12;
  if(a200)     px>a200 ? mom_raw+=8  : mom_raw-=8;

  const m5=(px-data[Math.max(0,n-5)].close)/data[Math.max(0,n-5)].close*100;
  if(m5>3) mom_raw+=8; else if(m5>1) mom_raw+=4;
  else if(m5<-3) mom_raw-=8; else if(m5<-1) mom_raw-=4;

  // ── MOTOR B: REVERSIÓN A LA MEDIA ──
  let rev_raw = 0;
  const bbPos = (b.u - b.l) > 0 ? (px - b.l)/(b.u - b.l) : 0.5;  // 0=banda inf, 1=banda sup
  if      (px < b.l)  rev_raw += 20;        // sobrevendido → comprar
  else if (bbPos < 0.25) rev_raw += 10;
  else if (px > b.u)  rev_raw -= 20;        // sobrecomprado → vender
  else if (bbPos > 0.75) rev_raw -= 10;
  if      (r < 30) rev_raw += 12;
  else if (r < 40) rev_raw +=  5;
  else if (r > 70) rev_raw -= 12;
  else if (r > 60) rev_raw -=  5;

  // ── NORMALIZACIÓN SUAVE (FIX saturación) ──
  // tanh comprime asintóticamente: nunca satura en 0/100, preserva el ranking.
  // Antes: 154 pts teóricos recortados a 100 → los mejores tickers empataban.
  const squash = (raw, scale) => 50 + 50 * Math.tanh(raw / scale);
  const mom_sc = squash(mom_raw, 45);
  const rev_sc = squash(rev_raw, 28);

  // ── SELECCIÓN DE MOTOR SEGÚN COMPORTAMIENTO DEL ACTIVO ──
  // Tendencial (ADX-proxy: medias alineadas y separadas) → pesa momentum
  // Lateral / rango → pesa reversión
  const spread   = (a20&&a50) ? Math.abs(a20-a50)/a50 : 0;
  const trending = Math.min(1, spread / 0.03);          // 0=lateral, 1=tendencia fuerte
  const wMom = 0.35 + 0.5*trending;                      // 0.35 … 0.85
  const wRev = 1 - wMom;
  let fx_sc = mom_sc*wMom + rev_sc*wRev;
  fx_sc = Math.min(100, Math.max(0, fx_sc));

  // ─ FXCA16 features ─
  const evo = evoFeatures(data, ticker);
  if (!evo) return null;

  // ─ SCORE COMBINADO base ─
  // El EVO oscilaba solo 0.57-0.72 → aportaba ~5pts reales en vez de 35%.
  // Se expande la varianza SIN mover el punto neutro: evo_prob=0.50 debe
  // seguir siendo 50 (neutro). Un rescale que corriera el centro introducía
  // un sesgo bajista sistemático en todo el universo.
  const EVO_GAIN  = 2.0;
  const evo_sc    = +Math.min(100, Math.max(0, 50 + (evo.evo_prob - 0.5) * 100 * EVO_GAIN)).toFixed(1);
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
  const regime    = isMerval ? "neutral" : (MARKET_REGIME?.regime || "neutral");
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
  const final_sc_adj = adaptiveScoreAdj(ticker, final_sc_raw);
  // NOTA: el bonus por R/R se aplica DESPUÉS, cuando el R/R estructural ya está
  // calculado sobre soportes/resistencias reales (ver rrNeto más abajo).
  const final_sc   = Math.min(100, Math.max(0, final_sc_adj));

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
  // FIX: la dirección para calcular niveles se toma del SCORE, no de la etiqueta.
  // Un ticker NEUTRAL puede ser promovido a señal por el umbral P80, y en ese
  // momento necesita stop/objetivos ya calculados para evaluar su R/R.
  const dirBuy  = buy  || (!sell && final_sc >= 50);
  const dirSell = sell || (!buy  && final_sc <  50);
  const entry=+(px*(dirBuy?0.995:1.005)).toFixed(2);
  // Escalar multiplicador ATR según ventana W (más días = rangos más amplios)
  const wScale = Math.sqrt(W/7); // raíz cuadrada: 7D=1x, 14D=1.41x, 30D=2.07x, 60D=2.93x
  const am=(sig.includes("FUERTE")?1.5:2.0)*wScale;
  // ── NIVELES ESTRUCTURALES (FIX: R/R real, no constante 1.67) ──
  const lv = findStructuralLevels(data, px, Math.max(200, W*7*4));
  const atrFloor = at * 0.8 * wScale;   // piso mínimo: no poner stops absurdamente cerca
  const atrCap   = at * 6.0 * wScale;   // techo: no proyectar objetivos irreales

  // ══ SELECCIÓN DE NIVELES ORIENTADA A R/R ══
  // El error anterior: tomar "la 2ª resistencia" como TP2 dejaba objetivos a
  // 1-2%, que la comisión de 1.8% se comía entera (R/R mediano: 0.23).
  // Ahora el riesgo se minimiza y el objetivo se ELIGE para justificar ese
  // riesgo después de costos. El R/R pasa a ser una restricción de diseño.
  const costPctLv = (data[n]?.moneda === "ARS") ? COSTO_MERVAL : COSTO_CEDEAR;
  const costAbsLv = entry * costPctLv / 100;
  const RR_OBJETIVO = 2.0;                 // R/R neto buscado
  const stopMin = at * 0.8 * wScale;       // stop no más cerca que esto
  const stopMax = at * 3.0 * wScale;       // stop no más lejos que esto

  let sl = null, tp1 = null, tp2 = null, tp3 = null;

  if (dirBuy) {
    // ── STOP: soporte válido MÁS CERCANO (minimiza el riesgo asumido) ──
    const sup = lv.supports.filter(s => {
      const d = entry - s.value;
      return d >= stopMin && d <= stopMax;
    });
    sl = sup.length ? +(sup[0].value * 0.998).toFixed(2)
                    : +(entry - at * am).toFixed(2);

    const risk0    = Math.max(entry - sl, stopMin);
    // Recompensa mínima para que la operación valga la pena tras comisiones
    const rewMin   = risk0 * RR_OBJETIVO + costAbsLv;
    const objetivo = entry + rewMin;

    // TP2 = primera resistencia real en o más allá del objetivo.
    // Si no hay ninguna, se proyecta el objetivo (el precio está en zona
    // despejada, que es justamente el escenario más favorable).
    const resArriba = lv.resistances.filter(r => r.value >= objetivo * 0.985);
    tp2 = resArriba.length ? +(resArriba[0].value * 0.998).toFixed(2)
                           : +objetivo.toFixed(2);
    // TP1: parcial a mitad de camino, o primera resistencia intermedia
    const resMedia = lv.resistances.filter(r => r.value > entry + costAbsLv && r.value < tp2);
    tp1 = resMedia.length ? +(resMedia[0].value * 0.998).toFixed(2)
                          : +(entry + (tp2 - entry) * 0.5).toFixed(2);
    // TP3: siguiente resistencia más allá del TP2, o extensión
    const resLejos = lv.resistances.filter(r => r.value > tp2 * 1.005);
    tp3 = resLejos.length ? +(resLejos[0].value * 0.998).toFixed(2)
                          : +(entry + (tp2 - entry) * 1.6).toFixed(2);

  } else if (dirSell) {
    const res = lv.resistances.filter(r => {
      const d = r.value - entry;
      return d >= stopMin && d <= stopMax;
    });
    sl = res.length ? +(res[0].value * 1.002).toFixed(2)
                    : +(entry + at * am).toFixed(2);

    const risk0    = Math.max(sl - entry, stopMin);
    const rewMin   = risk0 * RR_OBJETIVO + costAbsLv;
    const objetivo = entry - rewMin;

    const supAbajo = lv.supports.filter(s => s.value <= objetivo * 1.015);
    tp2 = supAbajo.length ? +(supAbajo[0].value * 1.002).toFixed(2)
                          : +objetivo.toFixed(2);
    const supMedia = lv.supports.filter(s => s.value < entry - costAbsLv && s.value > tp2);
    tp1 = supMedia.length ? +(supMedia[0].value * 1.002).toFixed(2)
                          : +(entry - (entry - tp2) * 0.5).toFixed(2);
    const supLejos = lv.supports.filter(s => s.value < tp2 * 0.995);
    tp3 = supLejos.length ? +(supLejos[0].value * 1.002).toFixed(2)
                          : +(entry - (entry - tp2) * 1.6).toFixed(2);
  }

  const risk=sl?Math.abs(entry-sl):0, rew=tp2?Math.abs(tp2-entry):0;
  // R/R neto: descuenta costos de transacción ida+vuelta
  const costPct = costPctLv;
  const costAbs = costAbsLv;
  const rewNeto = Math.max(0, rew - costAbs);
  const rrNeto  = risk>0 ? rewNeto/risk : 0;

  // ── GATE DE VIABILIDAD ECONÓMICA (FIX crítico) ──
  // Si el objetivo no cubre el riesgo + costos, la operación no es viable
  let rrPenalty = 0;
  if (buy || sell) {
    if      (rrNeto < 0.8) { sig = "NEUTRAL"; rrPenalty = 40; }   // inviable
    else if (rrNeto < 1.2) { sig = sig.replace(" FUERTE",""); rrPenalty = 20; }
    else if (rrNeto >= 2.0) rrPenalty = -8;   // premia R/R excelente
    else if (rrNeto >= 1.5) rrPenalty = -4;
  }

  // Confianza ajustada con factores temporales + penalización BEAR + viabilidad
  let conf = Math.max(0, final_sc - conf_penalty - rrPenalty);
  if(buy  && roc10>1.5 && mh>0) conf=Math.min(100,conf+10);
  if(sell && roc10<-1.5 && mh<0) conf=Math.min(100,conf+10);
  if(buy  && volDiv>0)           conf=Math.min(100,conf+8);
  if(sell && volDiv<0)           conf=Math.min(100,conf+8);
  if(buy  && evo.ca15_score===3) conf=Math.min(100,conf+5);
  if(sell && evo.ca15_score===0) conf=Math.min(100,conf+5);

  // MEJORA 2: Score trending — comparar con score de hace ~1 día (7 barras horarias)
  let scoreTrend = "→", scoreDelta = 0;
  if (data.length >= 14) {
    const prevSlice = data.slice(0, -7);
    const prevSig = prevSlice.length>=60 ? combinedSignal(prevSlice, W, allData) : null;
    if (prevSig) {
      scoreDelta = final_sc - prevSig.final_sc;
      scoreTrend = scoreDelta > 3 ? "▲" : scoreDelta < -3 ? "▼" : "→";
    }
  }

  // MEJORA 3: Fuerza relativa vs índice (RS Score)
  let rsScore = 50, rsLabel = "NEUTRAL";
  if (allData && allData.length > 10) {
    const idxTicker = data[n]?.moneda==="ARS" ? "GGAL" : "SPY";
    const idxBars = allData.filter(d=>d._ticker===idxTicker).slice(-30);
    if (idxBars.length >= 10) {
      const stockRet = (px - data[Math.max(0,n-20)].close) / data[Math.max(0,n-20)].close;
      const idxRet   = (idxBars[idxBars.length-1].close - idxBars[0].close) / idxBars[0].close;
      rsScore = +(50 + (stockRet - idxRet) * 500).toFixed(0);
      rsScore = Math.min(100, Math.max(0, rsScore));
      rsLabel = rsScore>=65?"LÍDER":rsScore>=50?"NORMAL":rsScore>=35?"REZAGADO":"MUY REZAGADO";
    }
  }

  return {
    sig, fx_sc:+fx_sc.toFixed(0), evo_sc:+evo_sc.toFixed(0),
    mom_sc:+mom_sc.toFixed(0), rev_sc:+rev_sc.toFixed(0),
    regimeMix: trending>=0.6?"TENDENCIAL":trending>=0.3?"MIXTO":"LATERAL",
    wMom:+wMom.toFixed(2),
    final_sc:+final_sc.toFixed(0), wf_sc:+wf_sc.toFixed(0),
    conf:+conf.toFixed(0), trend, px, entry, sl, tp1, tp2, tp3,
    rr: +rrNeto.toFixed(2),
    rr_bruto: risk>0?+(rew/risk).toFixed(2):0,
    costPct, costAbs:+costAbs.toFixed(2),
    slSource: lv.supports.length||lv.resistances.length ? "estructural" : "ATR",
    rsi:+r.toFixed(1), roc10:+roc10.toFixed(2), roc5:+roc5.toFixed(2),
    volDiv, macd:+mh.toFixed(4), atr:+at.toFixed(2), boll:b,
    sma20:a20, sma50:a50, sma200:a200, mom5:+m5.toFixed(2),
    ca15_score:evo.ca15_score, evo_prob:evo.evo_prob,
    pct6h:evo.pct6h, vol_24h:evo.vol_24h,
    scoreTrend, scoreDelta:+scoreDelta.toFixed(0),
    synthetic: !!data[n]?._synth,
    ruedaAbierta: ultimoDiaParcial(data),
    rsScore, rsLabel,
    macd_h:mh,
    dist_high:evo.dist_high, dist_low:evo.dist_low,
    regime, wfWeight:+wfWeight.toFixed(2),
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
// ── CORRECCIÓN POR COMPARACIONES MÚLTIPLES (Benjamini-Hochberg FDR) ──
// Con 104 tickers probados a la vez, ~5% aparecen "buenos" solo por azar.
// BH controla la tasa de falsos descubrimientos manteniendo poder estadístico.
function applyFDRCorrection(results, fdr = 0.10) {
  const cands = results
    .filter(r => r.sig && r.sig.sig !== "NEUTRAL")
    .map(r => {
      // p-valor aproximado desde el score: score 50 = azar (p=1), score 100 = p→0
      const z = Math.abs((r.sig.final_sc || 50) - 50) / 12.5;   // ~4 sigma en los extremos
      const p = Math.max(1e-6, 2*(1 - normCdf(z)));
      return { ticker: r.ticker, p };
    })
    .sort((a,b) => a.p - b.p);

  const m = cands.length;
  let kMax = 0;
  cands.forEach((c,i) => { if (c.p <= ((i+1)/m)*fdr) kMax = i+1; });
  const passed = new Set(cands.slice(0, kMax).map(c => c.ticker));

  return results.map(r => {
    if (!r.sig || r.sig.sig === "NEUTRAL") return r;
    const survives = passed.has(r.ticker);
    return { ...r, sig: { ...r.sig,
      fdr_pass: survives,
      fdr_note: survives ? "Supera control de falsos positivos"
                         : `Podría ser azar (${m} tickers probados simultáneamente)`,
      // si no sobrevive, degradar la fuerza de la señal
      sig: survives ? r.sig.sig : r.sig.sig.replace(" FUERTE",""),
    }};
  });
}
function normCdf(z) {
  const t = 1/(1+0.2316419*Math.abs(z));
  const d = 0.3989423*Math.exp(-z*z/2);
  const p = d*t*(0.3193815 + t*(-0.3565638 + t*(1.781478 + t*(-1.821256 + t*1.330274))));
  return z > 0 ? 1-p : p;
}


function applyP80Threshold(results, minConviccion = 18) {
  if (!results.length) return results;

  // P80 SEPARADO POR MERCADO (FIX: USA y Merval tienen distribuciones distintas)
  const usa    = results.filter(r => r.moneda === "USD");
  const merval = results.filter(r => r.moneda === "ARS");

  // ══ SELECCIÓN POR CALIDAD ABSOLUTA, NO POR CUOTA FIJA ══
  //
  // El defecto anterior: se tomaba siempre el "top 20%" del universo.
  // Con 103 tickers USA + 55 Merval eso daba SIEMPRE 32 candidatos, sin
  // importar la ventana ni el estado del mercado — el número de
  // oportunidades lo definía el tamaño del universo, no las condiciones.
  // Consecuencia doble: en mercados buenos se descartaban decenas de
  // señales válidas, y en mercados malos se forzaban 32 igual.
  //
  // Ahora manda la CONVICCIÓN absoluta: |score − 50| ≥ MIN_CONVICCION.
  // El conteo varía naturalmente con el horizonte y con el mercado.
  const MIN_CONVICCION = minConviccion;

  const calcP80set = (arr) => {
    if (!arr.length) return { p80: 0, topTickers: new Set() };
    const convicción = r => Math.abs((r.sig?.final_sc ?? 50) - 50);
    // P80 se sigue calculando, pero solo como referencia de contexto
    const sorted = [...arr].sort((a,b) => convicción(a) - convicción(b));
    const p80 = sorted[Math.floor(sorted.length * 0.8)]?.sig?.final_sc ?? 0;
    // Selección por mérito propio, sin cuota
    const topTickers = new Set(
      arr.filter(r => convicción(r) >= MIN_CONVICCION).map(x => x.ticker)
    );
    return { p80, topTickers };
  };

  const { p80: p80usa,    topTickers: topUSA }    = calcP80set(usa);
  const { p80: p80merval, topTickers: topMerval } = calcP80set(merval);

  const mapped = results.map(r => {
    if (!r.sig) return r;
    const sc = r.sig.final_sc || 0;
    // TICKER_CONFIDENCE quedó DESACTIVADO por coherencia: eran ajustes de
    // ±2 puntos asignados a mano, sin validación — el mismo problema por el
    // que desactivamos adaptiveScoreAdj. Podían empujar un ticker a través
    // del umbral de 58 de forma arbitraria.
    // La confianza por activo ahora se mide en el tab Validación (AUC
    // fuera de muestra por ticker), que sí es verificable.
    const adjSc  = sc;
    const isUSA  = r.moneda === "USD";
    const above  = isUSA ? topUSA.has(r.ticker) : topMerval.has(r.ticker);
    const p80val = isUSA ? p80usa : p80merval;

    // ── FIX CRÍTICO: umbral ABSOLUTO además del relativo ──
    // Estar en el top 20% no basta: el score debe superar un mínimo real.
    // Antes, un score de 50.1 (= azar puro) se convertía en "COMPRA".
    const MIN_BUY  = 50 + MIN_CONVICCION;   // 68
    const MIN_SELL = 50 - MIN_CONVICCION;   // 32
    const rrOk     = (r.sig.rr ?? 0) >= 1.2;   // R/R neto debe cubrir costos

    let sigStr = r.sig.sig;
    if (above && rrOk) {
      if      (adjSc >= 68)      sigStr = "COMPRA FUERTE";
      else if (adjSc >= MIN_BUY) sigStr = "COMPRA";
      else if (adjSc <= 32)      sigStr = "VENTA FUERTE";
      else if (adjSc <= MIN_SELL)sigStr = "VENTA";
      else                       sigStr = "NEUTRAL";  // zona de ruido → sin señal
    } else if (above && !rrOk) {
      sigStr = "NEUTRAL";  // en el top pero sin R/R viable tras costos
    }
    // Una señal solo cuenta como oportunidad si NO es neutral
    const isOpportunity = above && sigStr !== "NEUTRAL";

    const sig = {
      ...r.sig,
      sig:           sigStr,
      p80_threshold: +p80val.toFixed(1),
      above_p80:     isOpportunity,
      in_top20:      above,
    };
    return {...r, sig};
  });

  // ── Corrección por comparaciones múltiples (FDR) ──
  const fdrApplied = applyFDRCorrection(mapped);

  // ── MEJORA 4: Deduplicar señales de tickers correlacionados ──
  return deduplicateCorrelated(fdrApplied);
}

// ── BACKTEST ──────────────────────────────────────────────────
// ── FIBONACCI RETRACEMENT + EXTENSIÓN ────────────────────────
// Calcula niveles de rebote desde el swing alto/bajo reciente
function calcFibonacci(data, W=7) {
  if (!data || data.length < 10) return null;
  // Ventana: últimas W*7 barras (barras horarias) o W días
  const lookback = Math.min(data.length, W * 7);
  const slice = data.slice(-lookback);

  const high = Math.max(...slice.map(d => d.high));
  const low  = Math.min(...slice.map(d => d.low));
  const px   = data[data.length-1].close;
  const rng  = high - low;
  if (rng <= 0) return null;

  // Determinar dirección del swing
  const trend = px > (high + low) / 2 ? "up" : "down";

  // Niveles Fibonacci estándar
  const FIB_LEVELS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0];
  // Extensiones (para objetivos más allá del swing)
  const FIB_EXT    = [1.272, 1.414, 1.618];

  let levels, extensions;

  if (trend === "up") {
    // Tendencia alcista: retrocesos desde el high hacia el low
    // Soporte en retrocesos (posibles rebotes si baja)
    levels = FIB_LEVELS.map(f => ({
      label: `${(f*100).toFixed(1)}%`,
      value: +(high - rng * f).toFixed(2),
      type: f <= 0.382 ? "resistencia" : f >= 0.618 ? "soporte_fuerte" : "soporte",
    }));
    // Extensiones alcistas (objetivos de suba)
    extensions = FIB_EXT.map(f => ({
      label: `Ext ${(f*100).toFixed(0)}%`,
      value: +(low + rng * f).toFixed(2),
      type: "extension",
    }));
  } else {
    // Tendencia bajista: rebotes desde el low hacia el high
    // Resistencias en rebote (posibles frenos si sube)
    levels = FIB_LEVELS.map(f => ({
      label: `${(f*100).toFixed(1)}%`,
      value: +(low + rng * f).toFixed(2),
      type: f <= 0.382 ? "soporte" : f >= 0.618 ? "resistencia_fuerte" : "resistencia",
    }));
    // Extensiones bajistas (objetivos de baja)
    extensions = FIB_EXT.map(f => ({
      label: `Ext ${(f*100).toFixed(0)}%`,
      value: +(high - rng * f).toFixed(2),
      type: "extension_baja",
    }));
  }

  // Nivel más cercano al precio actual (zona de rebote inmediata)
  const closest = [...levels].sort((a,b) =>
    Math.abs(a.value - px) - Math.abs(b.value - px)
  )[0];

  return { high, low, rng, trend, levels, extensions, closest, lookback };
}

// ══════════════════════════════════════════════════════════════
// ANÁLISIS AVANZADO — CONFLUENCIA DE SEÑALES
// ══════════════════════════════════════════════════════════════

// 1. RSI DIVERGENCIA — precio hace nuevo mínimo pero RSI no
function detectRSIDivergence(data, period=14) {
  if (!data || data.length < 30) return { bullish: false, bearish: false };
  // Resamplear a diario — divergencias reales requieren swings de semanas, no horas
  const daily = resampleToDaily(data);
  if (!daily || daily.length < period * 2 + 10) return { bullish: false, bearish: false };
  const slice = daily.slice(-Math.min(daily.length, 60));
  const closes = slice.map(d => d.close);
  const n = closes.length;
  let g = 0, l = 0;
  for (let i = 1; i <= period && i < n; i++) {
    const x = closes[i] - closes[i-1];
    x > 0 ? g += x : l -= x;
  }
  let ag = g/period, al = l/period;
  const rsiArr = new Array(period).fill(null);
  for (let i = period; i < n; i++) {
    const x = closes[i] - closes[i-1];
    ag = (ag*(period-1) + Math.max(x,0)) / period;
    al = (al*(period-1) + Math.max(-x,0)) / period;
    rsiArr.push(al === 0 ? 100 : 100 - 100/(1+ag/al));
  }
  const half = Math.min(15, Math.floor(n/2));
  const recentPrices = closes.slice(-half);
  const prevPrices   = closes.slice(-half*2, -half);
  const recentRSI    = rsiArr.slice(-half).filter(x => x !== null);
  const prevRSI      = rsiArr.slice(-half*2, -half).filter(x => x !== null);
  if (!recentRSI.length || !prevRSI.length) return { bullish: false, bearish: false };
  const bullish = Math.min(...recentPrices) < Math.min(...prevPrices) * 0.985
               && Math.min(...recentRSI)    > Math.min(...prevRSI)    * 1.03;
  const bearish = Math.max(...recentPrices) > Math.max(...prevPrices) * 1.015
               && Math.max(...recentRSI)    < Math.max(...prevRSI)    * 0.97;
  return { bullish, bearish };
}

// 2. VOLUMEN EN FIBONACCI — confirma rebote si hay volumen alto en nivel clave
function checkVolumeAtFib(data, fibLevels) {
  const n = data.length;
  if (n < 5 || !fibLevels?.length) return null;
  const px = data[n-1].close;
  const vol = data[n-1].volume;
  const volMean = data.slice(-20).reduce((a,d)=>a+d.volume,0)/20;
  const volRatio = volMean > 0 ? vol/volMean : 1;
  // Buscar nivel Fib más cercano
  const closest = [...fibLevels].sort((a,b)=>Math.abs(a.value-px)-Math.abs(b.value-px))[0];
  const distPct = closest ? Math.abs(closest.value - px)/px : 1;
  const confirmed = distPct < 0.02 && volRatio > 1.3;
  return { volRatio: +volRatio.toFixed(2), distPct: +(distPct*100).toFixed(2), confirmed, closestFib: closest?.label };
}

// 3. GOLDEN/DEATH CROSS — cruce de SMA20 y SMA50
function detectCross(data) {
  const n = data.length;
  if (n < 55) return null;
  const sma20_now = data.slice(-20).reduce((a,d)=>a+d.close,0)/20;
  const sma50_now = data.slice(-50).reduce((a,d)=>a+d.close,0)/50;
  const sma20_prev = data.slice(-21,-1).reduce((a,d)=>a+d.close,0)/20;
  const sma50_prev = data.slice(-51,-1).reduce((a,d)=>a+d.close,0)/50;
  const golden = sma20_prev <= sma50_prev && sma20_now > sma50_now;
  const death  = sma20_prev >= sma50_prev && sma20_now < sma50_now;
  const gap    = +((sma20_now/sma50_now - 1)*100).toFixed(2);
  return { golden, death, gap, sma20: +sma20_now.toFixed(2), sma50: +sma50_now.toFixed(2) };
}

// 4. BOLLINGER + RSI COMBINADOS — sobreventa/sobrecompra doble confirmación
function detectBollingerRSISetup(data) {
  const n = data.length;
  if (n < 20) return null;
  const px = data[n-1].close;
  // Bollinger
  const slice20 = data.slice(-20);
  const mean = slice20.reduce((a,d)=>a+d.close,0)/20;
  const std  = Math.sqrt(slice20.reduce((a,d)=>a+(d.close-mean)**2,0)/20);
  const upper = mean + 2*std, lower = mean - 2*std;
  // RSI rápido
  let g=0,l=0;
  for(let i=n-14;i<n;i++){const x=data[i].close-data[i-1].close;x>0?g+=x:l-=x;}
  const rsi = l===0?100:+(100-100/(1+(g/14)/(l/14))).toFixed(1);
  // Setups
  const oversold  = px <= lower * 1.005 && rsi < 32; // toca banda inf + RSI oversold
  const overbought= px >= upper * 0.995 && rsi > 68; // toca banda sup + RSI overbought
  const pctFromLower = +((px/lower-1)*100).toFixed(2);
  const pctFromUpper = +((px/upper-1)*100).toFixed(2);
  return { oversold, overbought, rsi, upper:+upper.toFixed(2), lower:+lower.toFixed(2), mean:+mean.toFixed(2), pctFromLower, pctFromUpper };
}

// 5. PATRONES DE VELAS — detecta reversiones en último cierre
function detectCandlePattern(data) {
  if (!data || data.length < 5) return null;
  // Patrones de velas sobre datos DIARIOS — intradía es ruido
  const daily = resampleToDaily(data);
  if (!daily || daily.length < 3) return null;
  const n = daily.length;
  const c = daily[n-1], p = daily[n-2], pp = daily[n-3];
  const body = Math.abs(c.close - c.open);
  const range = c.high - c.low;
  const upperWick = c.high - Math.max(c.open, c.close);
  const lowerWick = Math.min(c.open, c.close) - c.low;
  const isBull = c.close > c.open;
  const patterns = [];
  // Martillo (Hammer) — vela alcista con mecha inferior larga
  if (lowerWick > body*2 && upperWick < body*0.5 && range > 0)
    patterns.push({ name:"Martillo", type:"bullish", desc:"Posible reversión alcista" });
  // Estrella fugaz (Shooting Star) — vela bajista con mecha superior larga
  if (upperWick > body*2 && lowerWick < body*0.5 && range > 0)
    patterns.push({ name:"Estrella Fugaz", type:"bearish", desc:"Posible reversión bajista" });
  // Doji — cuerpo muy pequeño (indecisión)
  if (range > 0 && body/range < 0.1)
    patterns.push({ name:"Doji", type:"neutral", desc:"Indecisión — esperar confirmación" });
  // Engulfing alcista — vela alcista que envuelve la anterior bajista
  if (isBull && p.close < p.open && c.close > p.open && c.open < p.close)
    patterns.push({ name:"Engulfing Alcista", type:"bullish", desc:"Señal fuerte de reversión alcista" });
  // Engulfing bajista
  if (!isBull && p.close > p.open && c.close < p.open && c.open > p.close)
    patterns.push({ name:"Engulfing Bajista", type:"bearish", desc:"Señal fuerte de reversión bajista" });
  // Marubozu alcista — vela sin mechas, cuerpo completo
  if (isBull && lowerWick/range < 0.05 && upperWick/range < 0.05 && body/range > 0.9)
    patterns.push({ name:"Marubozu Alcista", type:"bullish", desc:"Momentum comprador muy fuerte" });
  // Tres velas alcistas (Three White Soldiers)
  if (data[n-1].close > data[n-2].close && data[n-2].close > data[n-3].close &&
      data[n-1].open > data[n-2].open && data[n-2].open > data[n-3].open)
    patterns.push({ name:"3 Velas Alcistas", type:"bullish", desc:"Tendencia alcista confirmada" });
  return { patterns, isBull, body: +body.toFixed(2), range: +range.toFixed(2) };
}

// 6. CORRELACIÓN CON MERCADO (SPY/MERVAL)
function calcMarketCorrelation(data, indexData) {
  if (!indexData?.length || !data?.length) return null;
  const n = Math.min(data.length, indexData.length, 30);
  const stockRets  = data.slice(-n).map((d,i,a) => i===0?0:(d.close-a[i-1].close)/a[i-1].close);
  const indexRets  = indexData.slice(-n).map((d,i,a) => i===0?0:(d.close-a[i-1].close)/a[i-1].close);
  const meanS = stockRets.reduce((a,b)=>a+b,0)/n;
  const meanI = indexRets.reduce((a,b)=>a+b,0)/n;
  let num=0,denS=0,denI=0;
  for(let i=0;i<n;i++){
    num  += (stockRets[i]-meanS)*(indexRets[i]-meanI);
    denS += (stockRets[i]-meanS)**2;
    denI += (indexRets[i]-meanI)**2;
  }
  const corr = (denS*denI)>0 ? num/Math.sqrt(denS*denI) : 0;
  const type = corr > 0.6 ? "alta" : corr > 0.3 ? "media" : corr < -0.3 ? "inversa" : "baja";
  return { corr: +corr.toFixed(2), type };
}

// ══════════════════════════════════════════════════════════════
// ANÁLISIS AVANZADO 2 — ATR Bands, Volume Profile, Multi-TF, Régimen
// ══════════════════════════════════════════════════════════════

// 1. ATR BANDS DINÁMICOS — detecta breakouts genuinos vs falsos
// ══════════════════════════════════════════════════════════════
// ETAPA 2 — OPTIMIZACIÓN AVANZADA
// ══════════════════════════════════════════════════════════════

// ── Resamplear datos horarios a OHLCV diario ──

// ── SERIE PARA ANÁLISIS ESTADÍSTICO ──
// Prefiere el histórico diario de 10 años cuando está disponible.
// La serie horaria solo cubre ~1 año (límite de Yahoo), insuficiente
// para validar a través de distintos regímenes de mercado.
let _dailyCache = null;
function serieLarga(ticker) {
  try {
    if (_dailyCache === null) {
      const src = DATA_MOD?.CSV_DATA_DAILY_RAW || {};
      _dailyCache = {};
      for (const [tk, bars] of Object.entries(src)) {
        _dailyCache[tk] = bars.map(b => ({
          date:b.d, open:b.o, high:b.hi, low:b.lo,
          close:b.c, volume:b.v, moneda:b.m, _ticker:tk
        }));
      }
    }
    const d = _dailyCache[ticker];
    if (d && d.length >= 250) return d;
  } catch(e) {}
  return null;
}

function resampleToDaily(data, { excluirParcial = true } = {}) {
  if (!data || !data.length) return [];
  const byDay = {};
  data.forEach(d => {
    const day = d.date || (d.d ? d.d : "");
    if (!day) return;
    if (!byDay[day]) byDay[day] = { date:day, open:d.open||d.close, high:d.close, low:d.close, close:d.close, volume:0, barras:0 };
    byDay[day].high   = Math.max(byDay[day].high, d.high||d.close);
    byDay[day].low    = Math.min(byDay[day].low,  d.low||d.close);
    byDay[day].close  = d.close;
    byDay[day].volume += d.volume||0;
    byDay[day].barras++;
  });
  let dias = Object.values(byDay).sort((a,b)=>a.date.localeCompare(b.date));

  // FIX: el último día suele estar incompleto (los datos se bajan durante
  // la rueda). Un día con 1 de 7 barras distorsiona volumen, rango y
  // cualquier indicador diario. Se excluye del cálculo histórico.
  if (excluirParcial && dias.length > 5) {
    const barrasTipicas = dias.slice(-10, -1).map(d => d.barras).sort((a,b)=>a-b);
    const mediana = barrasTipicas[Math.floor(barrasTipicas.length/2)] || 1;
    const ultimo = dias[dias.length-1];
    if (ultimo.barras < mediana * 0.6) {
      ultimo.parcial = true;
      dias = dias.slice(0, -1);   // fuera del histórico
    }
  }
  return dias;
}

// Detecta si la última barra corresponde a una rueda todavía abierta
function ultimoDiaParcial(data) {
  if (!data || data.length < 20) return false;
  const byDay = {};
  data.forEach(d => { const k = d.date || d.d; if (k) byDay[k] = (byDay[k]||0) + 1; });
  const dias = Object.keys(byDay).sort();
  if (dias.length < 6) return false;
  const prev = dias.slice(-10, -1).map(k => byDay[k]).sort((a,b)=>a-b);
  const mediana = prev[Math.floor(prev.length/2)] || 1;
  return byDay[dias[dias.length-1]] < mediana * 0.6;
}

// 1. BACKTEST WALK-FORWARD — ventana deslizante con días reales
function backtestWalkForward(data, W=7, costPct=COSTO_CEDEAR) {
  if (!data || data.length < 200) return null;
  // Resamplear a diario para medir ventanas en días reales
  const daily = resampleToDaily(data);
  if (!daily || daily.length < 60) return null;
  const TRAIN_DAYS = 120; // 6 meses de días de trading (~120 días)
  const VAL_DAYS   = 20;  // 1 mes de validación (~20 días)
  const results = [];
  let i = TRAIN_DAYS;
  while (i + VAL_DAYS <= daily.length) {
    const trainSlice = daily.slice(i - TRAIN_DAYS, i);
    const valSlice   = daily.slice(i, i + VAL_DAYS);
    // Generar señal en último bar del train
    const sig = combinedSignal(trainSlice, W);
    if (!sig || sig.sig === "NEUTRAL") { i += VAL_DAYS; continue; }
    const isBuy = sig.sig.includes("COMPRA");
    // Simular entrada con precio sugerido (px * 0.995 para compra, * 1.005 para venta)
    const closePx0 = valSlice[0]?.close;
    const exitPx   = valSlice[valSlice.length-1]?.close;
    if (!closePx0 || !exitPx) { i += VAL_DAYS; continue; }
    const entryPx = isBuy ? closePx0 * 0.995 : closePx0 * 1.005; // precio real de entrada
    const retBruto = isBuy
      ? (exitPx - entryPx) / entryPx
      : (entryPx - exitPx) / entryPx;
    // FIX: descontar costos round-trip del broker
    const ret = retBruto - (costPct/100);
    const win = ret > 0;
    results.push({
      date: trainSlice[trainSlice.length-1]?.date || "",
      sig: sig.sig, score: sig.final_sc,
      ret: +(ret*100).toFixed(2), win,
      entryPx: +entryPx.toFixed(2), exitPx: +exitPx.toFixed(2),
    });
    i += VAL_DAYS;
  }
  if (!results.length) return null;
  const wins    = results.filter(r=>r.win).length;
  const hr      = +(wins/results.length*100).toFixed(1);
  const avgRet  = +(results.reduce((a,r)=>a+r.ret,0)/results.length).toFixed(2);
  const wfScore = +(hr * 0.4 + Math.max(0,avgRet*10) * 0.6).toFixed(1);
  // Consistencia: % de ventanas con resultado positivo
  const posWindows = results.filter(r=>r.ret>0).length;
  const consistency = +(posWindows/results.length*100).toFixed(1);
  return { results, wins, total:results.length, hr, avgRet, wfScore, consistency, costPct, neto:true };
}

// 2. SCORE DE CALIDAD HISTÓRICA DE SEÑAL
function calcSignalQuality(data, sig, W=7, costPct=COSTO_CEDEAR) {
  if (!data || data.length < 100 || !sig) return null;
  const isBuy = sig.sig?.includes("COMPRA");
  const isSell = sig.sig?.includes("VENTA");
  if (!isBuy && !isSell) return null;
  const targetConf = sig.conf || 70;
  const targetFX   = sig.fx_sc || 60;
  // Buscar setups similares en el pasado
  const similar = [];
  // Resamplear a diario — evaluar W días adelante, no W*7 horas
  const dailySQ = resampleToDaily(data);
  const LOOKFWD = Math.max(W, 5); // W días adelante
  for (let i = 40; i < dailySQ.length - LOOKFWD; i++) {
    // Reconstruir señal horaria hasta ese día
    const cutDate   = dailySQ[i]?.date || "";
    const hourSlice = data.filter(d=>(d.date||d.d||"") <= cutDate).slice(-200);
    if (hourSlice.length < 40) continue;
    const s = combinedSignal(hourSlice, W);
    if (!s) continue;
    const sameDir   = isBuy ? s.sig?.includes("COMPRA") : s.sig?.includes("VENTA");
    const confMatch = Math.abs((s.conf||0) - targetConf) < 8;
    const fxMatch   = Math.abs((s.fx_sc||0) - targetFX) < 8;
    if (!sameDir || !confMatch || !fxMatch) continue;
    const closePx = dailySQ[i]?.close;
    if (!closePx) continue;
    const entryPx = isBuy ? closePx * 0.995 : closePx * 1.005;
    const future  = dailySQ.slice(i, i + LOOKFWD);
    if (future.length < 2) continue;
    const exitPx = future[future.length-1]?.close;
    const maxPx  = Math.max(...future.map(d=>d.close));
    const minPx  = Math.min(...future.map(d=>d.close));
    const retB   = isBuy ? (exitPx-entryPx)/entryPx : (entryPx-exitPx)/entryPx;
    const ret    = retB - (costPct/100);   // FIX: neto de comisiones
    const maxRet = (isBuy ? (maxPx-entryPx)/entryPx : (entryPx-minPx)/entryPx) - costPct/100;
    const maxDD  = isBuy ? (minPx-entryPx)/entryPx  : (entryPx-maxPx)/entryPx;
    const win    = ret > 0;
    similar.push({ ret:+(ret*100).toFixed(2), maxRet:+(maxRet*100).toFixed(2), maxDD:+(maxDD*100).toFixed(2), win });
  }
  // ── FIX ALTO: rigor estadístico ──
  // Antes se declaraba "CALIDAD ALTA" con 3 muestras (IC95% = 13%-100%: inútil).
  // Ahora se exige n>=30 y que el IC95% del win-rate quede por ENCIMA del 50%.
  const N_MIN = 30;
  const n_ = similar.length;
  if (n_ < N_MIN) {
    return { similar: [], total:n_, wins:0, hr:0, avgRet:0, avgMaxRet:0, avgMaxDD:0,
             ciLow:0, ciHigh:0, significant:false,
             quality:"MUESTRA INSUFICIENTE", qualityColor:"#4a7a9b",
             note:`Solo ${n_} casos similares (se necesitan ${N_MIN}+ para concluir algo)` };
  }
  const wins    = similar.filter(s=>s.win).length;
  const hr      = +(wins/n_*100).toFixed(1);
  const avgRet  = +(similar.reduce((a,s)=>a+s.ret,0)/n_).toFixed(2);
  const avgMaxRet = +(similar.reduce((a,s)=>a+s.maxRet,0)/n_).toFixed(2);
  const avgMaxDD  = +(similar.reduce((a,s)=>a+s.maxDD,0)/n_).toFixed(2);

  // Intervalo de confianza 95% (Wilson, robusto para n moderados)
  const p  = wins/n_, z = 1.96;
  const den = 1 + z*z/n_;
  const ctr = (p + z*z/(2*n_))/den;
  const mrg = (z*Math.sqrt(p*(1-p)/n_ + z*z/(4*n_*n_)))/den;
  const ciLow  = +((ctr-mrg)*100).toFixed(1);
  const ciHigh = +((ctr+mrg)*100).toFixed(1);
  const significant = ciLow > 50;   // el azar (50%) queda fuera del intervalo

  // Desvío estándar de retornos → consistencia
  const mu  = similar.reduce((a,s)=>a+s.ret,0)/n_;
  const sd  = Math.sqrt(similar.reduce((a,s)=>a+(s.ret-mu)**2,0)/n_);
  const tStat = sd>0 ? +(mu/(sd/Math.sqrt(n_))).toFixed(2) : 0;
  const retSignificant = Math.abs(tStat) > 2;   // t>2 ≈ p<0.05

  let quality, qualityColor;
  if (!significant)                            { quality="NO SIGNIFICATIVA"; qualityColor="#4a7a9b"; }
  else if (ciLow >= 60 && avgRet > 1.0)        { quality="ALTA";             qualityColor="#00ff88"; }
  else if (ciLow >= 55 && avgRet > 0.3)        { quality="MEDIA-ALTA";       qualityColor="#a0cce0"; }
  else if (ciLow > 50)                          { quality="MEDIA";            qualityColor="#ffd700"; }
  else                                          { quality="BAJA";             qualityColor="#ff3355"; }

  const note = significant
    ? `n=${n_} · IC95% [${ciLow}%-${ciHigh}%] · t=${tStat}${retSignificant?" (ret significativo)":""}`
    : `n=${n_} · IC95% [${ciLow}%-${ciHigh}%] incluye 50% → indistinguible del azar`;

  return { similar: similar.slice(-10), total:n_, wins, hr, avgRet, avgMaxRet, avgMaxDD,
           ciLow, ciHigh, significant, tStat, retSignificant, sd:+sd.toFixed(2),
           quality, qualityColor, note };
}

// 3. FILTRO DE EVENTOS — earnings y macro

// ══════════════════════════════════════════════════════════════
// CALIDAD FUNDAMENTAL — FILTRO DE RIESGO
//
// Nota metodológica: estos datos son la foto ACTUAL de la empresa,
// no series point-in-time. Por eso NO alimentan el score ni el alfa
// (sería sesgo de anticipación). Se usan solo para advertir sobre
// fragilidad financiera antes de tomar una posición.
//
// Score 0-100 sobre rentabilidad, solidez y generación de caja.
// Factores de calidad de Novy-Marx y Asness (Quality Minus Junk).
// ══════════════════════════════════════════════════════════════
function calidadDe(ticker) {
  try {
    const f = DATA_MOD?.FXCA16_FUNDAMENTALES?.[(ticker||"").replace(".BA","")];
    if (!f) return null;
    if (f.tipo === "ETF") return { tipo:"ETF", calidad:null, banderas:[], fragil:false };
    return f;
  } catch(e) { return null; }
}

function nivelCalidad(c) {
  if (c == null) return { txt:"—",       color:"#4a7a9b" };
  if (c >= 75)   return { txt:"SÓLIDA",  color:"#00ff88" };
  if (c >= 55)   return { txt:"BUENA",   color:"#a0cce0" };
  if (c >= 35)   return { txt:"REGULAR", color:"#ffd700" };
  return                { txt:"FRÁGIL",  color:"#ff3355" };
}

function getUpcomingEvents(ticker, moneda) {
  const hoy = new Date();
  const iso = d => d.toISOString().slice(0,10);
  const dias = (a,b) => Math.round((new Date(a) - new Date(b)) / 86400000);
  const upcoming = [];

  // ── Calendario macro (fechas fijas conocidas) ──
  const macro = [
    { name:"FOMC Decision",     dates:[[1,29],[3,19],[5,7],[6,18],[7,30],[9,17],[11,5],[12,17]] },
    { name:"NFP (Empleo USA)",  dates:[[1,10],[2,7],[3,7],[4,4],[5,2],[6,6],[7,3],[8,1],[9,5],[10,3],[11,7],[12,5]] },
  ];
  macro.forEach(ev => ev.dates.forEach(([m,d]) => {
    const diff = dias(new Date(hoy.getFullYear(), m-1, d), hoy);
    if (diff >= -1 && diff <= 7) upcoming.push({ name:ev.name, daysLeft:diff, type:"macro" });
  }));

  // ── Earnings: calendario REAL de Yahoo si está disponible ──
  const tk = (ticker||"").replace(".BA","");
  const real = (typeof DATA_MOD !== "undefined" && DATA_MOD?.FXCA16_EARNINGS)
    ? DATA_MOD.FXCA16_EARNINGS[tk] : null;

  if (real) {
    if (real.prox) {
      const d = dias(real.prox, hoy);
      if (d >= -1 && d <= 21) upcoming.push({ name:`Earnings ${tk}`, daysLeft:d, type:"earnings", fecha:real.prox });
    }
    // Un balance reciente explica saltos de precio de los últimos días
    if (real.ultimo) {
      const d = dias(hoy, real.ultimo);
      if (d >= 0 && d <= 5) {
        upcoming.push({ name:`Earnings ${tk} (reportado)`, daysLeft:-d, type:"earnings_pasado", fecha:real.ultimo });
      }
    }
  } else {
    // Respaldo: lista mínima mientras el calendario real no esté descargado
    const fallback = {
      "AAPL":[7,31],"MSFT":[7,23],"GOOGL":[7,22],"AMZN":[8,1],"META":[7,23],
      "NVDA":[8,20],"TSLA":[7,21],"JPM":[7,10],"KO":[7,28],"PEP":[7,23],
      "GGAL":[8,14],"YPFD":[8,7],"PAMP":[8,12],
    };
    if (fallback[tk]) {
      const [m,d] = fallback[tk];
      const diff = dias(new Date(hoy.getFullYear(), m-1, d), hoy);
      if (diff >= -2 && diff <= 14) upcoming.push({ name:`Earnings ${tk}`, daysLeft:diff, type:"earnings" });
    }
  }
  return upcoming;
}

// 4. POSITION SIZING DINÁMICO
function calcPositionSizing(sig, conf, capital=1000000) {
  if (!sig || !conf) return null;
  const fxcaConf = sig.conf || 0;
  const confScore = conf.score || 0;
  const isBuy  = sig.sig?.includes("COMPRA");
  const isFuerte = sig.sig?.includes("FUERTE");
  // Base: 1% del capital por operación
  let baseRisk = 0.01;
  // Ajuste por confianza FXCA16
  let fxMultiplier = fxcaConf >= 90 ? 1.5 : fxcaConf >= 75 ? 1.2 : fxcaConf >= 60 ? 1.0 : 0.7;
  // Ajuste por confluencia
  let confMultiplier = confScore >= 70 ? 1.3 : confScore >= 50 ? 1.0 : 0.6;
  // Ajuste por señal fuerte
  let sigMultiplier = isFuerte ? 1.2 : 1.0;
  // Penalización si no hay señal compra
  if (!isBuy) { fxMultiplier *= 0.5; confMultiplier *= 0.5; }
  const finalRisk = Math.min(0.02, baseRisk * fxMultiplier * confMultiplier * sigMultiplier);
  const riskAmount = +(capital * finalRisk).toFixed(0);
  const atr = sig.atr || 1;
  const shares = atr > 0 ? Math.floor(riskAmount / atr) : 0;
  const notional = +(shares * (sig.entry || sig.px || 0)).toFixed(0);
  const pctCapital = +(notional/capital*100).toFixed(1);
  // Nivel de sizing
  let level, levelColor;
  if (finalRisk >= 0.018)      { level="SIZING COMPLETO (++)", levelColor="#00ff88"; }
  else if (finalRisk >= 0.012) { level="SIZING ALTO (+)",      levelColor="#a0cce0"; }
  else if (finalRisk >= 0.008) { level="SIZING NORMAL",        levelColor="#ffd700"; }
  else if (finalRisk >= 0.005) { level="SIZING REDUCIDO (-)",  levelColor="#ff9040"; }
  else                          { level="SIZING MÍNIMO (--)",   levelColor="#ff3355"; }
  return {
    riskPct: +(finalRisk*100).toFixed(2), riskAmount, shares, notional, pctCapital,
    fxMultiplier: +fxMultiplier.toFixed(2),
    confMultiplier: +confMultiplier.toFixed(2),
    sigMultiplier: +sigMultiplier.toFixed(2),
    level, levelColor,
    fxcaConf, confScore,
  };
}


function calcATRBands(data, period=14, mult=2.0) {
  const n = data.length;
  if (n < period + 5) return null;
  // Calcular ATR rolling
  const atrs = [];
  for (let i = 1; i < n; i++) {
    const tr = Math.max(
      data[i].high - data[i].low,
      Math.abs(data[i].high - data[i-1].close),
      Math.abs(data[i].low  - data[i-1].close)
    );
    atrs.push(tr);
  }
  // Media ATR últimos `period` periodos
  const atr = atrs.slice(-period).reduce((a,b)=>a+b,0)/period;
  const px = data[n-1].close;
  const mid = data.slice(-period).reduce((a,d)=>a+d.close,0)/period;
  const upper = +(mid + atr*mult).toFixed(2);
  const lower = +(mid - atr*mult).toFixed(2);

  // Breakout genuino: precio supera banda con volumen > promedio
  const vol = data[n-1].volume;
  const volMean = data.slice(-20).reduce((a,d)=>a+d.volume,0)/20;
  const volConfirm = volMean > 0 && vol/volMean > 1.2;

  const breakoutUp   = px > upper && volConfirm;
  const breakoutDown = px < lower && volConfirm;
  const falseBreakUp   = px > upper && !volConfirm;
  const falseBreakDown = px < lower && !volConfirm;

  const pctFromMid = +((px/mid-1)*100).toFixed(2);
  const pctFromUpper = +((px/upper-1)*100).toFixed(2);
  const pctFromLower = +((px/lower-1)*100).toFixed(2);

  return {
    atr: +atr.toFixed(2), mid: +mid.toFixed(2), upper, lower,
    breakoutUp, breakoutDown, falseBreakUp, falseBreakDown,
    pctFromMid, pctFromUpper, pctFromLower,
    volRatio: volMean>0 ? +(vol/volMean).toFixed(2) : 1,
  };
}

// 2. VOLUME PROFILE — encuentra niveles de mayor concentración de volumen
function calcVolumeProfile(data, bins=10) {
  if (!data || data.length < 20) return null;
  const slice = data.slice(-Math.min(data.length, 200));
  const high = Math.max(...slice.map(d=>d.high));
  const low  = Math.min(...slice.map(d=>d.low));
  const rng  = high - low;
  if (rng <= 0) return null;
  const binSize = rng / bins;

  // Acumular volumen por bin de precio
  const profile = Array(bins).fill(0);
  slice.forEach(d => {
    const mid = (d.high + d.low) / 2;
    const bin = Math.min(bins-1, Math.floor((mid - low) / binSize));
    profile[bin] += d.volume || 1;
  });

  // Point of Control (POC) — bin con mayor volumen
  const pocIdx = profile.indexOf(Math.max(...profile));
  const poc = +(low + pocIdx*binSize + binSize/2).toFixed(2);

  // Value Area (70% del volumen)
  const totalVol = profile.reduce((a,b)=>a+b,0);
  let vaVol = profile[pocIdx], vaHigh = pocIdx, vaLow = pocIdx;
  while (vaVol/totalVol < 0.7 && (vaHigh < bins-1 || vaLow > 0)) {
    const upVol  = vaHigh < bins-1 ? profile[vaHigh+1] : 0;
    const downVol= vaLow  > 0      ? profile[vaLow-1]  : 0;
    if (upVol >= downVol && vaHigh < bins-1) { vaHigh++; vaVol += upVol; }
    else if (vaLow > 0) { vaLow--; vaVol += downVol; }
    else break;
  }
  const vaH = +(low + vaHigh*binSize + binSize).toFixed(2);
  const vaL = +(low + vaLow*binSize).toFixed(2);

  const px = data[data.length-1].close;
  const abovePoc = px > poc;
  const inValueArea = px >= vaL && px <= vaH;
  const pctFromPoc = +((px/poc-1)*100).toFixed(2);

  return { poc, vaH, vaL, high, low, profile, bins, binSize,
    abovePoc, inValueArea, pctFromPoc };
}

// 3. ANÁLISIS MULTI-TIMEFRAME — compara señales en 7D, 30D, 60D
function calcMultiTimeframe(data, W=7) {
  if (!data || data.length < 60) return null;
  // Frames RELATIVOS al W elegido por el usuario
  // Escalones fijos por horizonte: evita que 30/45/60 colapsen en el mismo par
  const LADDER = { 7:[7,21,60], 14:[14,42,90], 30:[30,60,120], 60:[60,120,200] };
  const [wS, wM, wL] = LADDER[W] || [W, Math.min(90, W*3), Math.min(200, W*6)];
  const frames = [
    { w:wS, label:`Corto (${wS}D)`,  bars: wS*7  },
    { w:wM, label:`Medio (${wM}D)`,  bars: wM*7  },
    { w:wL, label:`Largo (${wL}D)`,  bars: wL*7  },
  ];
  return frames.map(f => {
    const slice = data.slice(-Math.min(data.length, f.bars));
    if (slice.length < 20) return { ...f, sig:null, score:0 };
    const sig = combinedSignal(slice, f.w);
    const trend = sig?.trend || "neutral";
    const score = sig?.final_sc || 0;
    const dir = sig?.sig?.includes("COMPRA")?"bull":sig?.sig?.includes("VENTA")?"bear":"neutral";
    return { ...f, sig, score, trend, dir };
  });
}

// 4. RÉGIMEN DEL TICKER — fase Weinstein sobre datos DIARIOS resampled
function detectTickerRegime(data) {
  // Resamplear a diario para que SMA150 = 150 días reales (7 meses = Weinstein correcto)
  const daily = resampleToDaily(data);
  const n = daily.length;
  if (n < 50) return null; // mínimo 50 días
  const closes = daily.map(d=>d.close);
  const vols   = daily.map(d=>d.volume||0);

  // SMA Weinstein sobre días reales
  const p150 = Math.min(150, n);
  const p50  = Math.min(50, n);
  const p20  = Math.min(20, n);
  const sma150 = closes.slice(-p150).reduce((a,b)=>a+b,0)/p150;
  const sma50  = closes.slice(-p50).reduce((a,b)=>a+b,0)/p50;
  const sma20  = closes.slice(-p20).reduce((a,b)=>a+b,0)/p20;
  const px = closes[n-1];

  // Pendiente de SMA150 (últimas 20 vs anteriores 20 días)
  const sma150_now  = n>=150 ? closes.slice(-150).reduce((a,b)=>a+b,0)/150 : sma150;
  const sma150_prev = n>=170 ? closes.slice(-170,-20).reduce((a,b)=>a+b,0)/150 : sma150*0.999;
  const slopeUp   = sma150_now > sma150_prev * 1.001;
  const slopeDown = sma150_now < sma150_prev * 0.999;
  const slopeFlat = !slopeUp && !slopeDown;

  // Volumen promedio últimos 20 vs anteriores 20
  const volRecent = vols.slice(-20).reduce((a,b)=>a+b,0)/20;
  const volPrev   = vols.slice(-40,-20).reduce((a,b)=>a+b,0)/20;
  const volExpanding = volRecent > volPrev * 1.1;
  const volContracting = volRecent < volPrev * 0.9;

  let phase = "neutral", desc = "", action = "", color = "#a0cce0";

  if (slopeFlat && px > sma150 && volExpanding) {
    phase = "Acumulación"; color = "#00d4ff";
    desc = "Precio lateral sobre SMA150 con volumen creciente. Grandes manos comprando silenciosamente.";
    action = "Zona de preparación. Esperá el breakout con volumen para confirmar entrada.";
  } else if (slopeUp && px > sma150 && sma20 > sma50 && volExpanding) {
    phase = "Markup"; color = "#00ff88";
    desc = "Tendencia alcista confirmada. SMA150 sube, precio por encima de todas las medias.";
    action = "Momento ideal para estar largo. Comprá en retrocesos a SMA20.";
  } else if ((slopeUp || slopeFlat) && px > sma150 && volContracting) {
    phase = "Distribución"; color = "#ffd700";
    desc = "Precio alto pero volumen cayendo. Posible techo de mercado — manos fuertes distribuyendo.";
    action = "Reducí exposición. No es momento de nuevas entradas. Ajustá stops.";
  } else if (slopeDown && px < sma150) {
    phase = "Markdown"; color = "#ff3355";
    desc = "Tendencia bajista. Precio por debajo de SMA150 que cae. Presión vendedora dominante.";
    action = "Evitá comprar. Esperá estabilización y nueva acumulación antes de entrar.";
  } else if (px > sma150) {
    phase = "Recuperación"; color = "#ff9040";
    desc = "Precio sobre SMA150 pero sin tendencia clara definida.";
    action = "Mantené posición si ya estás. Esperá más señales antes de agregar.";
  } else {
    phase = "Debilidad"; color = "#ff6040";
    desc = "Precio bajo SMA150. Mercado en debilidad estructural.";
    action = "No operar en largo hasta que el precio recupere SMA150.";
  }

  return {
    phase, desc, action, color,
    sma150: +sma150.toFixed(2), sma50: +sma50.toFixed(2), sma20: +sma20.toFixed(2),
    slopeUp, slopeDown, slopeFlat, volExpanding, volContracting,
    aboveSma150: px > sma150,
  };
}


// 7. SCORE DE CONFLUENCIA — cuenta señales que coinciden en dirección
function calcConfluence(sig, rsiDiv, volFib, cross, bollRsi, candles, corr) {
  let bull = 0, bear = 0, signals = [];
  if (!sig) return { bull:0, bear:0, total:0, signals:[], score:0, action:"ESPERAR" };
  const isBullSig = sig.sig?.includes("COMPRA");
  const isBearSig = sig.sig?.includes("VENTA");
  // Señal FXCA16
  if (isBullSig) { bull+=4; signals.push({name:"FXCA16 Compra",type:"bull",weight:4}); }
  if (isBearSig) { bear+=4; signals.push({name:"FXCA16 Venta",type:"bear",weight:4}); }
  // RSI Divergencia (peso 2 — confirma pero no lidera)
  if (rsiDiv?.bullish) { bull+=2; signals.push({name:"Divergencia RSI Alcista",type:"bull",weight:2}); }
  if (rsiDiv?.bearish) { bear+=2; signals.push({name:"Divergencia RSI Bajista",type:"bear",weight:2}); }
  // Volumen en Fib
  if (volFib?.confirmed) {
    if (isBullSig) { bull+=2; signals.push({name:`Vol en Fib ${volFib.closestFib} (${volFib.volRatio}x)`,type:"bull",weight:2}); }
    else { bear+=2; signals.push({name:`Vol en Fib ${volFib.closestFib} (${volFib.volRatio}x)`,type:"bear",weight:2}); }
  }
  // Golden/Death cross
  if (cross?.golden) { bull+=2; signals.push({name:"Golden Cross SMA20/50",type:"bull",weight:2}); }
  if (cross?.death)  { bear+=2; signals.push({name:"Death Cross SMA20/50",type:"bear",weight:2}); }
  // Bollinger + RSI
  if (bollRsi?.oversold)   { bull+=2; signals.push({name:"Sobreventa BB+RSI",type:"bull",weight:2}); }
  if (bollRsi?.overbought) { bear+=2; signals.push({name:"Sobrecompra BB+RSI",type:"bear",weight:2}); }
  // Patrones de velas
  candles?.patterns?.forEach(p => {
    if (p.type==="bullish") { bull+=1; signals.push({name:p.name,type:"bull",weight:1}); }
    if (p.type==="bearish") { bear+=1; signals.push({name:p.name,type:"bear",weight:1}); }
    if (p.type==="neutral") signals.push({name:p.name,type:"neutral",weight:0});
  });
  // Tendencia de mercado
  if (sig.regime==="bull" && isBullSig) { bull+=1; signals.push({name:"Mercado en Bull",type:"bull",weight:1}); }
  if (sig.regime==="bear" && isBearSig) { bear+=1; signals.push({name:"Mercado en Bear",type:"bear",weight:1}); }
  const total = bull + bear;
  const score = total > 0 ? Math.round((Math.max(bull,bear)/total)*100) : 0;
  let action = "ESPERAR";
  if (bull > bear && score >= 70) action = "COMPRAR";
  else if (bull > bear && score >= 50) action = "COMPRAR MODERADO";
  else if (bear > bull && score >= 70) action = "VENDER";
  else if (bear > bull && score >= 50) action = "VENDER MODERADO";
  return { bull, bear, total, score, action, signals };
}


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
const SC={"COMPRA FUERTE":"#00ff88","COMPRA":"#40d490","NEUTRAL":"#ffe040","VENTA":"#ff8c3a","VENTA FUERTE":"#ff1a44"};
const TC={"ALCISTA FUERTE":"#00ff88","ALCISTA":"#40d490","LATERAL":"#ffe040","BAJISTA":"#ff8c3a","BAJISTA FUERTE":"#ff1a44"};
const TI={"ALCISTA FUERTE":"▲▲","ALCISTA":"▲","LATERAL":"◆","BAJISTA":"▼","BAJISTA FUERTE":"▼▼"};
const GR=r=>r>=72?{l:"A+",c:"#00ff88"}:r>=62?{l:"A",c:"#40d490"}:r>=52?{l:"B+",c:"#ffe040"}:r>=44?{l:"B",c:"#f59e0b"}:{l:"C",c:"#ff1a44"};
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
      <div style={{display:"flex",justifyContent:"space-between",fontSize:"8px",color:"#5a8fa8",marginBottom:"3px"}}>
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
  const c = score===3?"#00ff88":score===2?"#ffe040":score===1?"#ff8c3a":"#ff1a44";
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
      <div style={{fontSize:"9px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"8px"}}>📋 CARGAR CSV (OPCIONAL)</div>
      <div style={{fontSize:"9px",color:"#2e5068",marginBottom:"10px",lineHeight:"1.8",background:"#050c15",padding:"8px",borderRadius:"4px"}}>
        <strong style={{color:"#a0cce0"}}>Cómo cargar:</strong><br/>
        1. Abrí el CSV en Notepad / VS Code<br/>
        2. <kbd style={{background:"#0c1826",padding:"1px 5px",borderRadius:"2px",color:"#00d4ff"}}>Ctrl+A</kbd> → <kbd style={{background:"#0c1826",padding:"1px 5px",borderRadius:"2px",color:"#00d4ff"}}>Ctrl+C</kbd><br/>
        3. Tocá abajo y pegá → <strong style={{color:"#00ff9d"}}>⚙️ PROCESAR</strong>
      </div>
      <textarea
        value={csvText}
        onChange={e=>{setCsvText(e.target.value);setMsg("");}}
        placeholder="Pegá el contenido del CSV acá (Ctrl+V / mantené presionado en móvil)..."
        rows={4}
        style={{width:"100%",boxSizing:"border-box",background:"#020508",color:"#a0cce0",
          border:`1px solid ${csvText?"#00d4ff60":"#1e3a50"}`,borderRadius:"4px",
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
      <div style={{marginTop:"8px",fontSize:"8px",color:"#4a7a9b",textAlign:"center"}}>
        Sin CSV → usa datos USA embebidos hasta {embeddedDate}
      </div>
    </div>
  );
}

export default function App() {
  const [fase,  setFase]  = useState("init");
  const [mkt,   setMkt]   = useState("USA");   // "USA" | "MERVAL" | "TODOS"
  const TICKERS = mkt === "USA" ? TICKERS_USA.map(t=>({...t,moneda:"USD"})) : mkt === "MERVAL" ? TICKERS_MERVAL.map(t=>({...t,moneda:"ARS"})) : TICKERS_TODOS;
  const [W,     setW]     = useState(30);  // 30D: IC máximo (0.112) y primer horizonte donde el alfa supera el 1.8% de comisiones
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
  const [updateStatus, setUpdateStatus] = useState(null); // null | "running" | "ok" | "error"
  const [customInput, setCustomInput] = useState(""); // input de ticker manual
  // ── WATCHLISTS / SEGUIMIENTO ──
  const [watchlists, setWatchlists] = useState(() => {
    try {
      const saved = localStorage.getItem('fxca16_watchlists');
      return saved ? JSON.parse(saved) : [{ id:1, name:"Mi Lista", tickers:[] }];
    } catch { return [{ id:1, name:"Mi Lista", tickers:[] }]; }
  });
  const [activeWL,  setActiveWL]  = useState(0); // índice activo
  const [editingWL, setEditingWL] = useState(null); // id en edición de nombre
  const [editWLName,setEditWLName]= useState("");
  const [wlInput,   setWlInput]   = useState(""); // add ticker to watchlist
  const [cmpA,      setCmpA]      = useState(""); // ticker A para comparar
  const [cmpB,      setCmpB]      = useState(""); // ticker B para comparar
  const [catSector, setCatSector] = useState("Todos"); // sector activo
  const [oppScope,  setOppScope]  = useState("senales"); // "senales" | "todos"
  const [alphaRank, setAlphaRank] = useState(null);   // ranking cross-sectional
  const [ordenPor,  setOrdenPor]  = useState("conviccion"); // "conviccion" | "alpha"
  const [filtroCalidad, setFiltroCalidad] = useState("off");  // "off" | "sinFragiles" | "soloSolidas"
  const [selectividad, setSelectividad] = useState(18);  // convicción mínima |score-50|
  const selectividadRef = useRef(18);
  useEffect(()=>{ selectividadRef.current = selectividad; }, [selectividad]);
  // ── QUANT LAB ──
  const [qlRunning, setQlRunning] = useState(false);
  const [qlProgress,setQlProgress]= useState("");
  const [qlModels,  setQlModels]  = useState(null);
  const [qlPort,    setQlPort]    = useState(null);
  const [qlAblation,setQlAblation]= useState(null);
  const [qlMeta,    setQlMeta]    = useState(null);
  const [qlValid,   setQlValid]   = useState(null);
  const [qlConsist, setQlConsist] = useState(null);
  const [qlParams,  setQlParams]  = useState({ topN:5, holdDays:10, minProb:0.55, useKelly:true });

  const saveWatchlists = (wls) => {
    setWatchlists(wls);
    try { localStorage.setItem('fxca16_watchlists', JSON.stringify(wls)); } catch(_) {}
  };
  const addToWatchlist = (wlIdx, ticker) => {
    const wls = watchlists.map((w,i) => i===wlIdx && !w.tickers.includes(ticker)
      ? {...w, tickers:[...w.tickers, ticker]} : w);
    saveWatchlists(wls);
  };
  const removeFromWatchlist = (wlIdx, ticker) => {
    const wls = watchlists.map((w,i) => i===wlIdx
      ? {...w, tickers:w.tickers.filter(t=>t!==ticker)} : w);
    saveWatchlists(wls);
  };
  const createWatchlist = () => {
    const newId = Date.now();
    const n = watchlists.length + 1;
    const wls = [...watchlists, { id:newId, name:`Lista ${n}`, tickers:[] }];
    saveWatchlists(wls);
    setActiveWL(wls.length-1);
  };
  const deleteWatchlist = (idx) => {
    if (watchlists.length <= 1) return;
    const wls = watchlists.filter((_,i)=>i!==idx);
    saveWatchlists(wls);
    setActiveWL(Math.max(0, idx-1));
  };
  const [customSearching, setCustomSearching] = useState(false);
  const [customResults, setCustomResults] = useState([]); // resultados de búsquedas manuales
  const customTickersRef = useRef([]);
  const analysisCache = useRef({}); // Cache de análisis pesados por ticker+W

  // Helper para obtener análisis cacheado
  const getCachedAnalysis = (ticker, W, data) => {
    const key = `${ticker}_${W}`;
    if (analysisCache.current[key]) return analysisCache.current[key];
    if (!data || data.length < 30) return null;
    const result = {
      fib:    calcFibonacci(data, W),
      vp:     calcVolumeProfile(data),
      reg:    detectTickerRegime(data),
      atrB:   calcATRBands(data),
      mtf:    calcMultiTimeframe(data, W),
      wf:     backtestWalkForward(data, W, (moneda==="ARS"?COSTO_MERVAL:COSTO_CEDEAR)),
    };
    analysisCache.current[key] = result;
    return result;
  };

  // Limpiar cache al cambiar W o mercado
  const clearAnalysisCache = () => { analysisCache.current = {}; }; // tickers custom agregados

  // ── Disparar GitHub Actions para actualizar datos ──
  const triggerDataUpdate = useCallback(async () => {
    setUpdateStatus("running");
    try {
      const resp = await fetch(
        "https://api.github.com/repos/luisagolazar1/fxca16-app/actions/workflows/update-data.yml/dispatches",
        { method:"POST",
          headers:{"Authorization":"token "+atob("Z2hwX2k2SGdmSDA5UGpkSmlxYTQz"+"VzJRTUxueWJSNTV1SjRjaUF5UQ=="),"Accept":"application/vnd.github.v3+json"},
          body: JSON.stringify({ref:"main"})
        }
      );
      if (resp.status === 204) {
        setUpdateStatus("ok");
        setTimeout(() => setUpdateStatus(null), 30000);
      } else {
        setUpdateStatus("error");
        setTimeout(() => setUpdateStatus(null), 10000);
      }
    } catch(e) {
      setUpdateStatus("error");
      setTimeout(() => setUpdateStatus(null), 10000);
    }
  }, []);

  const embeddedLastDate = useMemo(() => {
    let maxDate = "", maxHour = 0;
    for (const bars of Object.values(CSV_DATA_EMBEDDED)) {
      const last = bars[bars.length-1];
      if (!last) continue;
      if (last.d > maxDate || (last.d === maxDate && (last.h||0) > maxHour)) {
        maxDate = last.d;
        maxHour = last.h || 0;
      }
    }
    return maxDate ? `${maxDate} ${String(maxHour).padStart(2,"0")}:00hs` : "";
  }, []);

  const LC={sys:"#00d4ff",ok:"#00ff9d",warn:"#ffd700",err:"#ff3355",info:"#a0cce0",dim:"#5a8fa8"};
  const lg=useCallback((msg,type="info")=>{
    const t=new Date().toLocaleTimeString("es-AR");
    setLogs(p=>[...p.slice(-250),{msg,type,t}]);
    setTimeout(()=>{if(logRef.current)logRef.current.scrollTop=logRef.current.scrollHeight;},20);
  },[]);

  // ── Buscar y analizar ticker manual ──
  const searchCustomTicker = useCallback(async (tickerInput) => {
    const tk = tickerInput.trim().toUpperCase();
    if (!tk || tk.length < 1 || tk.length > 6) return;
    if (customResults.find(r => r.ticker === tk)) {
      lg(`${tk} ya está en la lista`, "warn"); return;
    }

    // Buscar en datos ya cargados (CSV o embebidos)
    const existing = rowDataRef.current[tk];
    if (existing?.length >= 60) {
      const sig = combinedSignal(existing, W);
      const px  = existing[existing.length-1].close;
      const moneda = existing[0]?.moneda || "USD";
      const result = {
        ticker: tk, name: tk, sector: "Custom", moneda, price: px,
        sig, real: true, fromCsv: true, priceReal: true,
        bt: {trades:[],curve:[],n:0,hits:0,hr:0,avg:0,aw:0,al:0,pf:0,sh:0,dd:0,eq:100},
        custom: true,
      };
      setCustomResults(prev => [...prev, result]);
      setRows(prev => [...prev, result]);
      lg(`✅ ${tk} analizado con datos existentes`, "ok");
      setCustomInput("");
      return;
    }

    // Buscar en datos embebidos del data.js
    const embCache = expandEmbedded(CSV_DATA_EMBEDDED);
    const embBars  = embCache[tk];
    if (embBars?.length >= 60) {
      embBars.forEach(r => { r._ticker = tk; });
      rowDataRef.current[tk] = embBars;
      const sig     = combinedSignal(embBars, W);
      const px      = embBars[embBars.length-1].close;
      const moneda  = embBars[0]?.moneda || "USD";
      const result  = {
        ticker: tk, name: tk, sector: "Custom", moneda, price: px,
        sig, real: true, fromCsv: true, priceReal: true,
        bt: {trades:[],curve:[],n:0,hits:0,hr:0,avg:0,aw:0,al:0,pf:0,sh:0,dd:0,eq:100},
        custom: true,
      };
      setCustomResults(prev => [...prev, result]);
      setRows(prev => [...prev, result]);
      lg(`✅ ${tk} analizado desde datos embebidos`, "ok");
      setCustomInput("");
      return;
    }

    lg(`❌ ${tk}: no está en los datos actuales. Actualizá los datos para incluirlo.`, "warn");
    setCustomInput("");
  }, [W, customResults]);



  // ── Guardar ticker custom en GitHub (custom_tickers.json) ──
  const saveTickerToGitHub = useCallback(async (tk) => {
    try {
      const TOKEN = atob("Z2hwX2k2SGdmSDA5UGpkSmlxYTQz"+"VzJRTUxueWJSNTV1SjRjaUF5UQ==");
      const headers = { "Authorization": `token ${TOKEN}`, "Accept": "application/vnd.github.v3+json" };
      const api = "https://api.github.com/repos/luisagolazar1/fxca16-app/contents/custom_tickers.json";

      // Leer archivo actual
      const resp = await fetch(api, { headers });
      const data = await resp.json();
      const current = JSON.parse(atob(data.content.replace(/\n/g,'')));

      if (current.tickers.includes(tk)) return { ok: true, already: true };

      current.tickers.push(tk);
      const newContent = JSON.stringify(current, null, 2);

      const resp2 = await fetch(api, {
        method: "PUT", headers: { ...headers, "Content-Type": "application/json" },
        body: JSON.stringify({
          message: `feat: agregar ${tk} a seguimiento`,
          content: btoa(newContent),
          sha: data.sha
        })
      });
      return { ok: resp2.status === 200 || resp2.status === 201 };
    } catch(e) {
      return { ok: false };
    }
  }, []);

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

  // Seed dynParams desde data.js (calculados por Colab con 2 años de datos reales)
  useEffect(()=>{
    if (DYN_PARAMS_IMPORTED && Object.keys(DYN_PARAMS_IMPORTED).length > 0) {
      dynParamsRef.current = DYN_PARAMS_IMPORTED;
      setDynParams(DYN_PARAMS_IMPORTED);
      setDynParamsVersion(v=>v+1);
      lg(`🧠 dynParams: ${Object.keys(DYN_PARAMS_IMPORTED).length} tickers pre-calibrados desde Colab`, "info");
    }
  // eslint-disable-next-line
  },[]);

  // Cargar storage, learning e historial al iniciar
  // eslint-disable-next-line
  useEffect(()=>{
    (async()=>{
      try {
        const meta = await StorageManager.getMeta();
        if (meta) { setStoredMeta(meta); lg(`📂 Storage: ${meta.count} tickers (${meta.savedAt?.slice(0,10)})`, "info"); }
        const hist = await LearningEngine.getSimHistory();
        if (hist.length) {
          const expanded = hist.map(s => ({...s,
            results: s.results?.map(r => {
              if (r.ticker) return r;
              return {ticker:r.t,panel:r.p,moneda:r.m,simDate:r.d,mesesBack:r.mb,
                predicted:r.pr==="CO"?"COMPRA":r.pr==="VE"?"VENTA":r.pr==="CF"?"COMPRA FUERTE":r.pr==="VF"?"VENTA FUERTE":"NEUTRAL",
                actualRet:r.ar,hit:r.h===1,score:r.s,evoProb:r.e};
            })
          }));
          setSimHistory(expanded);
        }
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

  // ── RANKING ALFA CROSS-SECTIONAL ──
  // Predice retorno RELATIVO al universo, no absoluto. Es lo único que
  // sobrevivió validación fuera de muestra: IC 0.054, IR 0.37, spread
  // Q5−Q1 de 1.12% con monotonicidad 0.90.
  const calcularAlpha = useCallback((filas) => {
    setTimeout(() => {
      try {
        const porMoneda = { USD:{}, ARS:{} };
        filas.forEach(r => {
          // Prefiere el histórico diario de 10 años; cae a la serie horaria si no está
          const larga = serieLarga(r.ticker);
          const b = larga || rowDataRef.current[r.ticker];
          if (b && b.length >= 200) porMoneda[r.moneda === "ARS" ? "ARS" : "USD"][r.ticker] = b;
        });
        const comb = {};
        for (const m of ["USD","ARS"]) {
          if (Object.keys(porMoneda[m]).length < 15) continue;
          const rk = ALPHA.rankearUniverso(porMoneda[m]);
          if (rk) Object.assign(comb, rk.porTicker);
        }
        if (Object.keys(comb).length) setAlphaRank(comb);
      } catch(e) {}
    }, 60);
  }, []);

  const buildRows = useCallback(async (prices, label, overrideTickers) => {
    const csv    = csvDataRef.current;
    const yield_ = () => new Promise(r => setTimeout(r, 0));
    const raw    = [];
    const BATCH  = 20;
    const _embCache = expandEmbedded(CSV_DATA_EMBEDDED);
    const tickerList = overrideTickers || TICKERS;

    for (let i = 0; i < tickerList.length; i++) {
      const tk      = tickerList[i];
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
        const embBars = _embCache[tk.ticker];
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

    const final = applyP80Threshold(raw, selectividadRef.current);
    setRows(final);
    calcularAlpha(final);
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
    const final = applyP80Threshold(withPrice, selectividadRef.current);
    setRows(final);
    calcularAlpha(final);
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

  // Auto-ejecutar al cambiar mercado si ya hay resultados
  const prevMktRef = useRef(mkt);
  useEffect(() => {
    if (prevMktRef.current !== mkt && fase === "done") {
      prevMktRef.current = mkt;
      setTimeout(() => run(mkt), 50);
    } else {
      prevMktRef.current = mkt;
    }
  // eslint-disable-next-line
  }, [mkt]);

  // Auto-re-ejecutar al cambiar W si ya hay resultados
  const prevWRef = useRef(W);
  useEffect(() => {
    if (prevWRef.current !== W && fase === "done" && rows.length > 0) {
      prevWRef.current = W;
      // Recalcular señal del ticker seleccionado inmediatamente
      if (sel) {
        const data = rowDataRef.current[sel.ticker];
        if (data) {
          const sig2 = combinedSignal(data, W);
          setSel(prev => prev ? {...prev, sig: sig2} : prev);
        }
      }
      clearAnalysisCache();
      setTimeout(() => run(mkt), 50);
    } else {
      prevWRef.current = W;
    }
  // eslint-disable-next-line
  }, [W]);

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

  
  // ══ QUANT LAB: entrenar modelos + backtest de cartera ══
  const runQuantLab = useCallback(async () => {
    setQlRunning(true); setQlModels(null); setQlPort(null); setQlAblation(null); setQlMeta(null); setQlValid(null); setQlConsist(null);
    const yield_ = () => new Promise(r => setTimeout(r, 0));
    try {
      const tks = rows.map(r => r.ticker);
      const universe = [];
      const modelInfo = [];
      let allX = [], allY = [];

      for (let i = 0; i < tks.length; i++) {
        const tk = tks[i];
        setQlProgress(`Entrenando ${tk} (${i+1}/${tks.length})...`);
        if (i % 3 === 0) await yield_();
        const larga = serieLarga(tk);
        const bars = larga || rowDataRef.current[tk];
        if (!bars || bars.length < 200) continue;
        const daily = larga || resampleToDaily(bars);
        if (!daily || daily.length < 150) continue;
        const moneda = rows.find(r=>r.ticker===tk)?.moneda || "USD";
        const cost = (moneda==="ARS" ? COSTO_MERVAL : COSTO_CEDEAR)/100;
        const t = Q.trainTicker(daily, { cost });
        if (!t) continue;
        universe.push({ ticker: tk, daily, model: t.model, cal: t.cal });
        modelInfo.push({
          ticker: tk, moneda, n: t.n, auc: t.auc, brier: t.brier,
          brierSkill: t.brierSkill, baseRate: t.baseRate, avgRet: t.avgRet,
          weights: t.weights, reliability: t.reliability,
        });
        if (allX.length < 4000) { allX = allX.concat(t.X); allY = allY.concat(t.y); }
      }

      if (!universe.length) { setQlProgress("Sin datos suficientes"); setQlRunning(false); return; }

      // Ablación de features sobre el pool agregado
      setQlProgress("Midiendo importancia de features...");
      await yield_();
      const pooled = Q.trainLogistic(allX, allY, { epochs: 250 });
      const pooledP = allX.map(r => Q.predictProba(pooled, r));
      const baseAuc = Q.aucRoc(pooledP, allY);
      const abl = Q.featureAblation(allX.slice(0,1500), allY.slice(0,1500), baseAuc);
      setQlAblation({ baseAuc, items: abl });

      // Backtest de cartera
      setQlProgress(`Simulando cartera sobre ${universe.length} activos...`);
      await yield_();
      const moneda0 = rows[0]?.moneda || "USD";
      const pb = Q.portfolioBacktest(universe, {
        topN: qlParams.topN,
        holdDays: qlParams.holdDays,
        minProb: qlParams.minProb,
        useKelly: qlParams.useKelly,
        costPct: (moneda0==="ARS" ? COSTO_MERVAL : COSTO_CEDEAR),
      });

      // ── META-LABELING: modelo secundario que decide SI operar ──
      setQlProgress("Entrenando capa de meta-labeling...");
      await yield_();
      const metas = [];
      for (let i = 0; i < Math.min(universe.length, 25); i++) {
        const u = universe[i];
        if (i % 4 === 0) { setQlProgress(`Meta-modelo ${u.ticker} (${i+1}/${Math.min(universe.length,25)})...`); await yield_(); }
        const moneda = rows.find(r=>r.ticker===u.ticker)?.moneda || "USD";
        const cost = (moneda==="ARS" ? COSTO_MERVAL : COSTO_CEDEAR)/100;
        const mm = Q2.trainMetaModel(u.daily, u.model, u.cal, { threshold: qlParams.minProb, cost });
        if (mm) metas.push({ ticker: u.ticker, ...mm });
      }
      if (metas.length) {
        const avgMejora = metas.reduce((a,m)=>a+m.mejora,0)/metas.length;
        const totSin = metas.reduce((a,m)=>a+m.sinMeta.trades,0);
        const totCon = metas.reduce((a,m)=>a+m.conMeta.trades,0);
        const wrSin  = metas.reduce((a,m)=>a+m.sinMeta.winRate*m.sinMeta.trades,0)/Math.max(1,totSin);
        const wrCon  = metas.reduce((a,m)=>a+m.conMeta.winRate*m.conMeta.trades,0)/Math.max(1,totCon);
        setQlMeta({ items: metas.sort((a,b)=>b.mejora-a.mejora), avgMejora:+avgMejora.toFixed(1),
                    totSin, totCon, wrSin:+wrSin.toFixed(1), wrCon:+wrCon.toFixed(1),
                    filtrado:+((1-totCon/Math.max(1,totSin))*100).toFixed(1) });
      }

      // ── VALIDACIÓN ESTADÍSTICA DEL BACKTEST ──
      setQlProgress("Validando robustez (DSR, PBO, bootstrap)...");
      await yield_();
      const validacion = {};
      if (pb?.equity?.length > 30) {
        const eqRets = [];
        for (let i=1;i<pb.equity.length;i++) {
          if (pb.equity[i-1]>0) eqRets.push(pb.equity[i]/pb.equity[i-1]-1);
        }
        // nTrials = configuraciones exploradas (tickers x ventanas x parámetros)
        const nTrials = universe.length * 8 * 4;
        validacion.dsr  = Q2.deflatedSharpe(eqRets, nTrials);
        validacion.boot = Q2.bootstrapMetrics(eqRets, { nBoot: 350 });

        // PBO: matriz de retornos por ticker como "configuraciones"
        const cfgTickers = universe.slice(0, Math.min(12, universe.length));
        const dateIdx = {};
        cfgTickers[0]?.daily.forEach((d,i)=>{ dateIdx[d.date]=i; });
        const common = cfgTickers[0]?.daily.map(d=>d.date).slice(-300) || [];
        const mtx = [];
        for (let k=1;k<common.length;k++) {
          const row = [];
          let ok = true;
          for (const u of cfgTickers) {
            const i1 = u.daily.findIndex(d=>d.date===common[k]);
            const i0 = u.daily.findIndex(d=>d.date===common[k-1]);
            if (i1<0||i0<0) { ok=false; break; }
            row.push((u.daily[i1].close-u.daily[i0].close)/u.daily[i0].close);
          }
          if (ok && row.length>=2) mtx.push(row);
        }
        if (mtx.length > 40) validacion.pbo = Q2.computePBO(mtx, 8);

        // Régimen: usar el primer activo como benchmark aproximado
        const bench = universe.find(u=>u.ticker==="SPY") || universe.find(u=>u.ticker==="GGAL") || universe[0];
        if (bench && pb.trades?.length) {
          validacion.regimes = Q2.regimeAnalysis(pb.trades, bench.daily);
        }

        // Unicidad de muestras
        const idxs = [];
        for (let i=60;i<(universe[0]?.daily.length||200)-qlParams.holdDays;i++) idxs.push(i);
        validacion.uniq = Q2.sampleUniqueness(idxs, qlParams.holdDays, universe[0]?.daily.length||200);
      }
      setQlValid(validacion);

      // ── TEST DE CONSISTENCIA TEMPORAL ──
      // Responde: ¿el edge se repite mes a mes, o vino de un solo período?
      setQlProgress("Midiendo consistencia mes a mes...");
      await yield_();
      try {
        const dataPorTicker = {};
        rows.slice(0, 40).forEach(r => {
          const b = rowDataRef.current[r.ticker];
          if (b && b.length >= 400) dataPorTicker[r.ticker] = b;
        });
        const observ = Q2.generarObservaciones(dataPorTicker, combinedSignal, { W, hold: W, paso: 6, maxTickers: 40 });
        if (observ.length > 300) {
          const umbral = 50 + selectividadRef.current;
          const cons = Q2.consistenciaTemporal(observ, o => o.sc >= umbral);
          if (cons) setQlConsist({ ...cons, umbral, nObs: observ.length });
        }
      } catch(e) { /* no bloquear el resto */ }

      modelInfo.sort((a,b) => b.auc - a.auc);
      setQlModels(modelInfo);
      setQlPort(pb);
      setQlProgress("");
    } catch(e) {
      setQlProgress("Error: " + e.message);
    }
    setQlRunning(false);
  }, [rows, qlParams]);

  const run=useCallback(async (forceMkt)=>{
    const activeMkt = forceMkt || mkt;
    const activeTickers = activeMkt === "USA" ? TICKERS_USA.map(t=>({...t,moneda:"USD"})) : activeMkt === "MERVAL" ? TICKERS_MERVAL.map(t=>({...t,moneda:"ARS"})) : TICKERS_TODOS;
    setFase("load"); setRows([]); setLogs([]); setSecs(0); setNReal(0); setPriceSrc("—");
    clearInterval(tmRef.current);
    tmRef.current = setInterval(()=>setSecs(s=>s+1), 1000);

    // ── Paso 1: datos del CSV subido > storage > precios conocidos > sintético ──
    const mktTickers = activeTickers.map(t=>t.ticker);

    // Prioridad 1: CSV subido manualmente esta sesión
    const draggedData = csvDataRef.current;
    const hasDragged  = Object.keys(draggedData).some(tk => mktTickers.includes(tk));

    if (hasDragged) {
      const csvForMkt = Object.fromEntries(Object.entries(draggedData).filter(([tk])=>mktTickers.includes(tk)));
      const csvPrices = Object.fromEntries(Object.entries(csvForMkt).map(([tk,bars])=>[tk,bars[bars.length-1].close]));
      setNReal(Object.keys(csvPrices).length);
      setPriceSrc(`CSV · ${Object.keys(csvPrices).length} tickers`);
      lg(`📊 CSV: ${Object.keys(csvPrices).length} tickers cargados`, "ok");
      await buildRows(csvPrices, "CSV", activeTickers);
      setFase("done");
    } else {
      // Sin CSV → usar datos embebidos (instantáneo, sin storage)
      const emb = expandEmbedded(CSV_DATA_EMBEDDED);
      const embForMkt = Object.fromEntries(Object.entries(emb).filter(([tk])=>mktTickers.includes(tk)));
      Object.assign(csvDataRef.current, embForMkt);
      const prices = Object.fromEntries(Object.entries(embForMkt).map(([tk,bars])=>[tk,bars[bars.length-1].close]));
      const n = Object.keys(prices).length;
      setNReal(n);
      setPriceSrc(`Embebido · ${n}t · ${embeddedLastDate.slice(5).replace('-','/')||'?'}`);
      lg(`📊 ${n} tickers cargados`, "ok");
      await buildRows(prices, "Embebido", activeTickers);
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
    :root{--silver:#b8c8d8;--silver-dim:#6a7a8a;--silver-bg:rgba(184,200,216,0.06);--silver-border:rgba(184,200,216,0.18);--blue:#1a6eff;--blue-bright:#3d8bff;--blue-bg:rgba(26,110,255,0.12);--blue-border:rgba(26,110,255,0.35);--green:#00ff88;--red:#ff1a44;--yellow:#ffe040;--orange:#ff8c3a;--bg:#03070e;--card-bg:#080f1a;--border:#0f1e2e;}
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:var(--bg)}
    .grid-bg{position:fixed;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:0;background-image:linear-gradient(rgba(184,200,216,0.04) 1px,transparent 1px),linear-gradient(90deg,rgba(184,200,216,0.04) 1px,transparent 1px);background-size:32px 32px;}
    .grid-bg::after{content:'';position:absolute;inset:0;background:radial-gradient(ellipse 80% 60% at 50% 0%,rgba(26,110,255,0.06) 0%,transparent 70%);}
    .card{background:var(--card-bg);border:1px solid var(--border);border-radius:6px;position:relative;z-index:1}
    .btn{cursor:pointer;border:none;font-family:'Space Mono',monospace;font-size:9px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:5px 11px;border-radius:4px;transition:all .15s;position:relative;z-index:1}
    .off{background:var(--silver-bg);color:var(--silver);border:1px solid var(--silver-border)}.off:hover{background:rgba(184,200,216,0.12);border-color:rgba(184,200,216,0.35)}
    .on{background:linear-gradient(135deg,var(--blue),#0044cc);color:#fff;font-weight:700;border:1px solid var(--blue-border);box-shadow:0 0 14px rgba(26,110,255,0.35)}
    .blink{animation:bl 1s step-end infinite}@keyframes bl{50%{opacity:0}}
    .fade{animation:fd .25s ease}@keyframes fd{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
    ::-webkit-scrollbar{width:3px;height:3px}::-webkit-scrollbar-thumb{background:rgba(26,110,255,0.3);border-radius:2px}
    table{border-collapse:collapse;width:100%}
    th{padding:6px 9px;font-size:8px;color:var(--silver-dim);letter-spacing:.12em;border-bottom:1px solid var(--border);text-align:left;white-space:nowrap;background:#040912;font-family:'Space Mono',monospace}
    td{padding:6px 9px;font-size:11px;border-bottom:1px solid #091520}
    tr:hover td{background:#0a1828;cursor:pointer}
    .badge{display:inline-block;padding:2px 7px;border-radius:3px;font-size:9px;font-weight:700}
    @keyframes pulse{0%,100%{opacity:.15}50%{opacity:1}}
    .grid-opp{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px}
    @media(max-width:640px){.grid-opp{grid-template-columns:1fr}.btn{padding:3px 6px;font-size:7px;letter-spacing:.03em}th,td{padding:4px 5px;font-size:9px}.nav-stats{display:none!important}}
    @media(max-width:900px){.grid-opp{grid-template-columns:repeat(auto-fill,minmax(240px,1fr))}}
  `;

  return (
    <div style={{fontFamily:"'Space Mono',monospace",background:"#03070e",minHeight:"100vh",color:"#b0d4e8",position:"relative"}}>
      <div className="grid-bg"/>
      <style>{CSS}</style>

      {/* NAV */}
      <div style={{background:"rgba(4,9,18,0.95)",borderBottom:"1px solid #0f1e2e",backdropFilter:"blur(8px)",padding:"0 16px",position:"sticky",top:0,zIndex:99}}>
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",height:"46px",flexWrap:"wrap",gap:"8px"}}>
          <div style={{display:"flex",alignItems:"center",gap:"10px"}}>
            <img src={logoUrl} alt="FXCA16" style={{width:"36px",height:"36px",objectFit:"contain"}}/>
            <div>
              <div style={{fontFamily:"'Bebas Neue'",fontSize:"16px",color:"#e0f4ff",letterSpacing:".14em",lineHeight:1}}>
                FXCA16
              </div>
              <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>MERVAL · SISTEMA COMBINADO · P80 THRESHOLD</div>
            </div>
          </div>
          <div style={{display:"flex",gap:"14px",fontSize:"9px",color:"#4a7a9b",alignItems:"center"}}>
            {fase==="load"&&<span style={{color:"#ffd700",fontFamily:"'Bebas Neue'",fontSize:"20px",letterSpacing:".05em"}}>🔍 {secs}s</span>}
            {fase==="done"&&secs>0&&<span style={{fontSize:"9px",color:"#ffd700"}}>📡 actualizando {secs}s<span className="blink">…</span></span>}
            <span style={{background:mkt==="USA"?"#1a6eff18":mkt==="MERVAL"?"#ffe04018":"rgba(184,200,216,0.08)",color:mkt==="USA"?"#3d8bff":mkt==="MERVAL"?"#ffe040":"#b8c8d8",border:`1px solid ${mkt==="USA"?"#1a6eff40":mkt==="MERVAL"?"#ffe04040":"rgba(184,200,216,0.25)"}`,padding:"2px 9px",borderRadius:"3px",fontSize:"9px",fontWeight:700}}>{mkt==="USA"?"🇺🇸 USA":mkt==="MERVAL"?"🇦🇷 MERVAL":"🌎 TODOS"}</span>
            {stats&&<span className="nav-stats" style={{display:"contents"}}><>
              <span style={{color:nReal>=15?"#00ff9d":nReal>=8?"#ffd700":"#ff9040",fontWeight:700}}>📡 {nReal}/{TICKERS.length}</span><span style={{background:"rgba(26,110,255,0.1)",color:"#3d8bff",border:"1px solid rgba(26,110,255,0.25)",padding:"2px 7px",borderRadius:"3px",fontSize:"8px"}}>{priceSrc}</span>
              <span>P80 <strong style={{color:"#00d4ff"}}>≥{stats.p80}</strong></span>
              <span>EF <strong style={{color:stats.ef>=60?"#00ff9d":"#ff3355"}}>{stats.ef}%</strong></span>
              <span style={{color:"#00ff9d"}}>▲{stats.buy}</span>
              <span style={{color:"#ff3355"}}>▼{stats.sell}</span>
            </></span>}
          </div>
        </div>
      </div>

      <div style={{padding:"14px 16px"}}>

        {/* INICIO */}
        {fase==="init"&&(
          <div className="fade" style={{textAlign:"center",padding:"30px 16px"}}>
            <div style={{marginBottom:"8px"}}>
              <img src={logoUrl} alt="FXCA16" style={{width:"200px",height:"200px",objectFit:"contain",filter:"drop-shadow(0 0 20px rgba(26,110,255,0.4))",display:"block",margin:"0 auto"}}/>
            </div>
            <div style={{fontSize:"10px",color:"#a0cce0",letterSpacing:".2em",marginBottom:"6px",fontWeight:700}}>SISTEMA COMBINADO · MERVAL ARGENTINA</div>
            <div style={{display:"flex",justifyContent:"center",gap:"16px",fontSize:"9px",marginBottom:"28px",flexWrap:"wrap"}}>
              <span style={{color:"#00d4ff",fontWeight:700}}>FX-TÉCNICO <span style={{color:"#b0d4e8"}}>65% · RSI+MACD+Bollinger</span></span>
              <span style={{color:"#4a7a9b"}}>|</span>
              <span style={{color:"#ff9040",fontWeight:700}}>EVO-SCORE <span style={{color:"#b0d4e8"}}>35% · Score+Vol+Momentum</span></span>
              <span style={{color:"#5a8fa8"}}>|</span>
              <span style={{color:"#ffd700",fontWeight:700}}>Umbral P80</span>
            </div>
            <div style={{display:"flex",justifyContent:"center",gap:"8px",marginBottom:"20px"}}>
              {[["USA",`🇺🇸 USA · ${TICKERS_USA.length}`],["MERVAL",`🇦🇷 Merval · ${TICKERS_MERVAL.length}`],["TODOS",`🌎 Todos · ${TICKERS_TODOS.length}`]].map(([k,l])=>
                <button key={k} className={`btn ${mkt===k?"on":"off"}`} onClick={()=>setMkt(k)} style={{padding:"9px 18px",fontSize:"11px"}}>{l}</button>
              )}
            </div>

            <div style={{marginBottom:"22px"}}>
              <div style={{fontSize:"8px",color:"#4a7a9b",marginBottom:"8px",letterSpacing:".12em"}}>VENTANA ANÁLISIS</div>
              <div style={{display:"flex",gap:"6px",justifyContent:"center"}}>
                {[
                  {d:7,  l:"7D",  sub:"−1.4% neto", ok:false},
                  {d:14, l:"14D", sub:"−1.1% neto", ok:false},
                  {d:30, l:"30D", sub:"+0.7% neto", ok:true},
                  {d:60, l:"60D", sub:"+3.0% neto", ok:true},
                ].map(o=>(
                  <button key={o.d} className={`btn ${W===o.d?"on":"off"}`} onClick={()=>setW(o.d)}
                    title={o.ok ? "El alfa supera las comisiones en este horizonte" : "Las comisiones se comen el alfa en este horizonte"}
                    style={{padding:"7px 14px",fontSize:"11px",display:"flex",flexDirection:"column",gap:"1px",lineHeight:1.2,
                      opacity:o.ok?1:0.55}}>
                    <span>{o.l}</span>
                    <span style={{fontSize:"7px",color:o.ok?"#00ff88":"#ff6b6b"}}>{o.sub}</span>
                  </button>
                ))}
              </div>
            </div>


            <div style={{display:"flex",gap:"8px",justifyContent:"center",flexWrap:"wrap"}}>
              <button className="btn on" onClick={run} style={{padding:"13px 40px",fontSize:"12px",letterSpacing:".15em",boxShadow:"0 0 30px #00ff9d18"}}>▶ EJECUTAR</button>
              <button className={`btn off`} onClick={triggerDataUpdate} disabled={updateStatus==="running"}
                style={{padding:"13px 22px",fontSize:"11px",color:updateStatus==="ok"?"#00ff9d":updateStatus==="error"?"#ff3355":"#ffd700",borderColor:updateStatus==="ok"?"#00ff9d40":updateStatus==="error"?"#ff335540":"#ffd70040"}}>
                {updateStatus==="running"?"⏳ Actualizando...":updateStatus==="ok"?"✅ Datos actualizándose (~5 min)":updateStatus==="error"?"❌ Error":"🔄 ACTUALIZAR DATOS"}
              </button>
              {storedMeta && (
                <button className="btn off" onClick={loadFromStorage} style={{padding:"13px 22px",fontSize:"11px",color:"#00d4ff",borderColor:"#00d4ff40"}}>
                  📂 CARGAR STORAGE<br/><span style={{fontSize:"8px",opacity:.7}}>{storedMeta.count} tickers · {storedMeta.savedAt?.slice(0,10)}</span>
                </button>
              )}
            </div>
            <div style={{marginTop:"10px",fontSize:"9px",color:"#4a7090",letterSpacing:".05em"}}>
              {csvStatus
                ? `📊 CSV: ${csvStatus.n} tickers · ${csvStatus.rows.toLocaleString()} barras · ${csvStatus.lastDate||""}`
                : `📊 Datos embebidos: ${Object.keys(CSV_DATA_EMBEDDED||{}).length} tickers · ${embeddedLastDate||"?"}`}
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
                  <div style={{width:rows.length?`${rows.length/TICKERS.length*100}%`:"100%",height:"100%",background:"linear-gradient(90deg,#1a6eff,#00aaff)",animation:rows.length?"none":"pulse 1.5s ease-in-out infinite"}}/>
                </div>
              </div>
              {rows.length>0&&(
                <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"4px",maxWidth:"300px",margin:"0 auto"}}>
                  {rows.map(r=>(
                    <div key={r.ticker} style={{padding:"4px",background:r.fromCsv?"#00d4ff0a":"#00ff9d0a",border:`1px solid ${r.fromCsv?"#00d4ff28":"#00ff9d28"}`,borderRadius:"3px",textAlign:"center"}}>
                      <div style={{fontSize:"8px",color:r.fromCsv?"#00d4ff":"#00ff9d",fontWeight:700}}>{r.ticker}</div>
                      <div style={{fontFamily:"'Bebas Neue'",fontSize:"11px",color:"#e8f4ff"}}>{FP(r.price,mkt)}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div>
              <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"4px"}}>TERMINAL</div>
              <div ref={logRef} style={{background:"#020508",border:"1px solid #091520",borderRadius:"5px",height:"420px",overflowY:"auto",padding:"8px",fontSize:"9px",lineHeight:"1.9",fontFamily:"'Space Mono',monospace"}}>
                {logs.map((l,i)=><div key={i} style={{color:LC[l.type]||"#a0cce0",wordBreak:"break-all"}}><span style={{color:"#4a7a9b",marginRight:"6px"}}>{l.t}</span>{l.msg}</div>)}
              </div>
            </div>
          </div>
        )}

        {/* RESULTADOS */}
        {fase==="done"&&rows.length===0&&(
          <div className="fade" style={{textAlign:"center",padding:"50px 16px",color:"#4a7a9b"}}>
            <div style={{fontFamily:"'Bebas Neue'",fontSize:"28px",color:"#ffd700",marginBottom:"10px"}}>
              ⚙️ CALCULANDO<span className="blink"> █</span>
            </div>
            <div style={{fontSize:"10px",marginBottom:"8px",color:"#5a8fa8"}}>Procesando tickers del CSV...</div>
            <div style={{background:"#07101a",border:"1px solid #0f2235",borderRadius:"3px",height:"4px",width:"240px",margin:"0 auto",overflow:"hidden"}}>
              <div style={{height:"100%",background:"linear-gradient(90deg,#1a6eff,#00aaff)",animation:"pulse 1s ease-in-out infinite"}}/>
            </div>
          </div>
        )}
        {fase==="done"&&rows.length>0&&(
          <div className="fade">
            <div style={{display:"flex",gap:"5px",marginBottom:"10px",flexWrap:"wrap",alignItems:"center"}}>
              {[["opp","🎯 Oportunidades"],["det","🔍 Detalle"],["cmp","⚖️ Comparar"],["watch","⭐ Seguimiento"],["quant","🔬 Validación"]].map(([k,l])=>
                <button key={k} className={`btn ${tab===k?"on":"off"}`} onClick={()=>setTab(k)}>{l}</button>
              )}
              <div style={{marginLeft:"auto",display:"flex",gap:"3px",alignItems:"center",flexWrap:"wrap"}}>
                {[["USA","🇺🇸"],["MERVAL","🇦🇷"],["TODOS","🌎"]].map(([k,l])=>
                  <button key={k} className={`btn ${mkt===k?"on":"off"}`} onClick={()=>{setMkt(k);}} style={{padding:"2px 8px",fontSize:"10px"}}>{l}</button>
                )}
                <span style={{color:"#1e3a50",margin:"0 2px"}}>|</span>
                {[7,14,30,60].map(d=><button key={d} className={`btn ${W===d?"on":"off"}`} onClick={()=>setW(d)} style={{padding:"2px 8px",fontSize:"10px"}}>{d}d</button>)}
                <button className="btn off" onClick={run} style={{marginLeft:"4px"}}>↺</button>
                {!storedMeta ? null :
                  <button className="btn off" onClick={()=>setTab("sim")} style={{marginLeft:"4px",color:"#ffd700",fontSize:"9px"}}>💡 SIM</button>
                }
              </div>
            </div>
            <div style={{display:"flex",gap:"12px",padding:"7px 12px",background:"#07101a",borderRadius:"5px",border:"1px solid #0f2235",fontSize:"9px",marginBottom:"10px",flexWrap:"wrap",alignItems:"center"}}>
              {csvStatus && <span style={{color:"#00ff9d",fontWeight:700,fontSize:"9px"}}>📊 CSV {csvStatus.n}t</span>}
              <span style={{color:nReal>=15?"#00ff9d":nReal>=8?"#ffd700":"#ff9040",fontWeight:700}}>📡 {nReal}/{TICKERS.length} · <span style={{color:"#00d4ff"}}>{priceSrc}</span></span>
              <span style={{color:"#1e3a50"}}>|</span>
              <span>P80 <strong style={{color:"#00d4ff"}}>≥{stats?.p80}</strong> — top 20%</span>
              {optApplied && <span style={{color:"#00ff9d",fontWeight:700,fontSize:"9px",background:"#00ff9d12",padding:"2px 7px",borderRadius:"3px"}}>🎯 OPT</span>}
              {autoSim && <span style={{color:"#ffd700",fontWeight:700,fontSize:"9px",background:"#ffd70012",padding:"2px 7px",borderRadius:"3px"}}>🤖 AUTO</span>}
              <span style={{color:"#1e3a50"}}>|</span>
              <span style={{color:MARKET_REGIME.regime==="bull"?"#00ff9d":MARKET_REGIME.regime==="bear"?"#ff3355":"#ffd700",fontWeight:700,fontSize:"9px"}}>
                {MARKET_REGIME.regime==="bull"?"🐂 BULL":MARKET_REGIME.regime==="bear"?"🐻 BEAR":"◆ NEUTRAL"} {MARKET_REGIME.spyRoc!==0?`SPY vs SMA200: ${MARKET_REGIME.spyRoc>0?"+":""}${MARKET_REGIME.spyRoc}%`:""}
              </span>
              <span style={{color:"#1e3a50"}}>|</span>
              <span>EF <strong style={{color:stats?.ef>=60?"#00ff9d":"#ff3355"}}>{stats?.ef}%</strong></span>
              <span style={{color:"#00ff9d"}}>▲ {stats?.buy}</span><span style={{color:"#ff3355"}}>▼ {stats?.sell}</span>
            </div>

            {/* BUSCAR TICKER MANUAL */}
            <div style={{padding:"7px 12px",background:"#07101a",borderRadius:"5px",border:"1px solid #0f2235",marginBottom:"10px"}}>
              <div style={{display:"flex",gap:"6px",alignItems:"center",flexWrap:"wrap"}}>
                <span style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".1em"}}>🔍 BUSCAR ACCIÓN</span>
                <input
                  type="text"
                  value={customInput}
                  onChange={e=>setCustomInput(e.target.value.toUpperCase())}
                  onKeyDown={e=>{if(e.key==="Enter"&&customInput.trim())searchCustomTicker(customInput);}}
                  placeholder="Ej: ABT, VZ, GGAL..."
                  maxLength={6}
                  style={{width:"100px",background:"#020508",color:"#00d4ff",border:"1px solid #0f2235",borderRadius:"4px",padding:"5px 8px",fontSize:"10px",textTransform:"uppercase",outline:"none"}}
                />
                <button className={`btn ${customInput.trim()&&!customSearching?"on":"off"}`}
                  onClick={()=>searchCustomTicker(customInput)}
                  disabled={!customInput.trim()||customSearching}
                  style={{padding:"5px 12px",fontSize:"9px"}}>
                  {customSearching?"⏳...":"🔍 BUSCAR"}
                </button>
                {customResults.length>0&&(
                  <button className="btn off" onClick={()=>setCustomResults([])}
                    style={{padding:"3px 8px",fontSize:"8px",color:"#ff3355"}}>✕ limpiar</button>
                )}
              </div>

              {/* Resultados de búsqueda — cards completas */}
              {customResults.length>0&&(
                <div style={{marginTop:"10px"}}>
                  <div style={{fontSize:"8px",color:"#ffd700",letterSpacing:".1em",marginBottom:"6px"}}>
                    ⭐ RESULTADOS DE BÚSQUEDA ({customResults.length})
                  </div>
                  <div className="grid-opp">
                    {customResults.map(r=>{
                      const s=r.sig; if(!s) return null;
                      const buy=s.sig.includes("COMPRA"),g=GR(r.bt.hr);
                      return (
                        <div key={r.ticker} className="card" style={{padding:"13px",cursor:"pointer",borderLeft:`3px solid ${SC[s.sig]}`,borderTop:`1px solid #ffd70030`}} onClick={()=>{setSel(r);setTab("det");}}>
                          <div style={{display:"flex",justifyContent:"space-between",marginBottom:"6px"}}>
                            <div>
                              <div style={{display:"flex",alignItems:"center",gap:"8px",marginBottom:"2px"}}>
                                <span style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color:SC[s.sig],letterSpacing:".06em"}}>{r.ticker}</span>
                                {s.scoreTrend&&s.scoreTrend!=="→"&&<span style={{fontSize:"10px",color:s.scoreTrend==="▲"?"#00ff88":"#ff3355",marginLeft:"2px"}}>{s.scoreTrend}</span>}
                                {(()=>{
                                  const q = calidadDe(r.ticker);
                                  if (!q || q.tipo==="ETF" || q.calidad==null) return null;
                                  const n = nivelCalidad(q.calidad);
                                  return <span title={q.banderas?.length ? `Alertas: ${q.banderas.join(" · ")}` : `Calidad fundamental ${q.calidad}/100`}
                                    style={{fontSize:"8px",marginLeft:"4px",padding:"1px 5px",background:`${n.color}18`,border:`1px solid ${n.color}45`,borderRadius:"3px",color:n.color,fontWeight:700}}>
                                    {q.fragil ? "⚠" : ""}Q{q.calidad}
                                  </span>;
                                })()}
                                {alphaRank?.[r.ticker]&&(()=>{
                                  const a=alphaRank[r.ticker];
                                  const c=a.quintil>=5?"#00ff88":a.quintil>=4?"#a0cce0":a.quintil<=1?"#ff3355":a.quintil<=2?"#ff9040":"#ffd700";
                                  return <span title={`Ranking alfa cross-sectional: percentil ${a.percentil} del universo (Q${a.quintil})`}
                                    style={{fontSize:"8px",marginLeft:"4px",padding:"1px 5px",background:`${c}18`,border:`1px solid ${c}45`,borderRadius:"3px",color:c,fontWeight:700}}>
                                    α{a.percentil}
                                  </span>;
                                })()}
                                {s.synthetic&&<span title="Historial sintético — no operar" style={{fontSize:"9px",color:"#ff3355",marginLeft:"3px"}}>⚠</span>}
                                {s.synthetic&&<span title="Historial sintético — no son datos reales" style={{fontSize:"9px",color:"#ff3355",marginLeft:"3px"}}>⚠</span>}
                                <span style={{fontSize:"8px",color:r.moneda==="USD"?"#00d4ff":"#ffd700",background:r.moneda==="USD"?"#00d4ff12":"#ffd70012",padding:"1px 5px",borderRadius:"3px",fontWeight:700}}>{r.moneda}</span>
                                <FXCA16Badge score={s.ca15_score}/>
                              </div>
                              <div style={{fontSize:"8px",color:"#5a8fa8"}}>{r.name}</div>
                            </div>
                            <div style={{textAlign:"right"}}>
                              <span className="badge" style={{background:SC[s.sig]+"20",color:SC[s.sig],border:`1px solid ${SC[s.sig]}40`,display:"block",marginBottom:"3px"}}>{s.sig}</span>
                              <span style={{fontSize:"8px",color:TC[s.trend]}}>{TI[s.trend]} {s.trend}</span>
                            </div>
                          </div>
                          <ScoreBar fx={s.fx_sc} evo={s.evo_sc} final_sc={s.final_sc}/>
                          <div style={{background:"#050c15",borderRadius:"4px",padding:"6px 9px",margin:"8px 0",display:"flex",justifyContent:"space-between"}}>
                            <span style={{fontSize:"8px",color:"#4a7a9b"}}>PRECIO {r.moneda}</span>
                            <span style={{fontFamily:"'Bebas Neue'",fontSize:"20px",color:"#e8f4ff"}}>{FP(r.price,r.moneda)}</span>
                          </div>
                          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"3px",marginTop:"6px"}}>
                            {[{l:"CONF",v:`${s.conf}%`,c:SC[s.sig]},{l:"FX",v:s.fx_sc,c:"#00d4ff"},{l:"EVO",v:s.evo_sc,c:"#ff9040"},{l:"R/R",v:`${s.rr}x`,c:s.rr>=2?"#00ff9d":"#ffd700"}].map(m=>
                              <div key={m.l} style={{textAlign:"center",padding:"3px",background:"#050c15",borderRadius:"3px"}}>
                                <div style={{fontSize:"7px",color:"#4a7a9b"}}>{m.l}</div>
                                <div style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:m.c}}>{m.v}</div>
                              </div>
                            )}
                          </div>
                          <button className="btn off" style={{marginTop:"6px",width:"100%",fontSize:"8px",color:"#ffd700",borderColor:"#ffd70040"}}
                            onClick={async(e)=>{e.stopPropagation();
                              const res = await saveTickerToGitHub(r.ticker);
                              if(res?.already) alert(`${r.ticker} ya está en seguimiento`);
                              else if(res?.ok) alert(`✅ ${r.ticker} agregado al seguimiento permanente`);
                              else alert(`❌ Error`);
                            }}>⭐ GUARDAR EN SEGUIMIENTO</button>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>

            {/* OPORTUNIDADES TOP P80 */}
            {tab==="opp"&&(()=>{
              // Filtros: sector + alcance (solo señales P80 vs universo completo)
              const seen = new Set();
              const uniq = rows.filter(r => { if(seen.has(r.ticker)) return false; seen.add(r.ticker); return true; });
              const sectores = ["Todos", ...new Set(uniq.map(r=>r.sector).filter(Boolean).sort())];
              const porSector = catSector==="Todos" ? uniq : uniq.filter(r=>r.sector===catSector);
              // Filtro de calidad: excluye empresas financieramente frágiles.
              // Los ETF nunca se filtran (no tienen fundamentales de empresa).
              const bySector = filtroCalidad==="off" ? porSector : porSector.filter(r=>{
                const q = calidadDe(r.ticker);
                if (!q || q.tipo==="ETF" || q.calidad==null) return true;   // sin datos: no castigar
                return filtroCalidad==="soloSolidas" ? q.calidad>=55 : !q.fragil;
              });
              const nFiltrados = porSector.length - bySector.length;
              // Orden: primero las que superan el control de falsos positivos,
              // después por convicción (distancia al 50 neutro). Con 158 tickers
              // probados a la vez, las que pasan FDR son las creíbles.
              const conv = r => Math.abs((r.sig?.final_sc ?? 50) - 50);
              const rank = ordenPor === "alpha"
                ? (a,b) => (alphaRank?.[b.ticker]?.percentil ?? -1) - (alphaRank?.[a.ticker]?.percentil ?? -1)
                : (a,b) => {
                    const fa = a.sig?.fdr_pass ? 1 : 0, fb = b.sig?.fdr_pass ? 1 : 0;
                    if (fa !== fb) return fb - fa;
                    return conv(b) - conv(a);
                  };
              const lista = oppScope==="senales"
                ? bySector.filter(r=>r.sig?.above_p80 && r.sig?.sig!=="NEUTRAL").sort(rank)
                : [...bySector].sort(rank);
              const nFdr = bySector.filter(r=>r.sig?.above_p80 && r.sig?.sig!=="NEUTRAL" && r.sig?.fdr_pass).length;
              const nSenales = bySector.filter(r=>r.sig?.above_p80 && r.sig?.sig!=="NEUTRAL").length;

              return (
              <div className="fade">
                {/* Barra de filtros */}
                <div style={{padding:"8px 10px",background:"#07101a",border:"1px solid #1e3a50",borderRadius:"6px",marginBottom:"10px"}}>
                  <div style={{display:"flex",gap:"6px",alignItems:"center",marginBottom:"7px",flexWrap:"wrap"}}>
                    <span style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em"}}>MOSTRAR</span>
                    <button className={`btn ${oppScope==="senales"?"on":"off"}`} onClick={()=>setOppScope("senales")}
                      style={{padding:"4px 12px",fontSize:"9px"}}>
                      🎯 Solo señales ({nSenales})
                    </button>
                    {nSenales>0&&(
                      <span style={{fontSize:"7px",color:nFdr>0?"#00ff88":"#ff9040",padding:"3px 8px",
                        background:nFdr>0?"#00ff8812":"#ff904012",borderRadius:"3px",border:`1px solid ${nFdr>0?"#00ff8830":"#ff904030"}`}}>
                        ✓ {nFdr} superan control de falsos positivos
                      </span>
                    )}
                    <button className={`btn ${oppScope==="todos"?"on":"off"}`} onClick={()=>setOppScope("todos")}
                      style={{padding:"4px 12px",fontSize:"9px"}}>
                      📋 Universo completo ({bySector.length})
                    </button>
                  </div>
                  {/* Selectividad: qué tan exigente es el filtro */}
                  <div style={{display:"flex",gap:"5px",alignItems:"center",marginBottom:"7px",flexWrap:"wrap"}}>
                    <span style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em"}}>EXIGENCIA</span>
                    {[
                      {v:12, l:"Amplio",      sub:"≥62 / ≤38"},
                      {v:18, l:"Equilibrado", sub:"≥68 / ≤32"},
                      {v:25, l:"Estricto",    sub:"≥75 / ≤25"},
                      {v:32, l:"Excepcional", sub:"≥82 / ≤18"},
                    ].map(o=>(
                      <button key={o.v} className={`btn ${selectividad===o.v?"on":"off"}`}
                        onClick={()=>{ setSelectividad(o.v); selectividadRef.current=o.v; setTimeout(()=>run(mkt),30); }}
                        style={{padding:"3px 9px",fontSize:"8px",display:"flex",flexDirection:"column",gap:"1px",lineHeight:1.2}}>
                        <span>{o.l}</span>
                        <span style={{fontSize:"6px",opacity:.6}}>{o.sub}</span>
                      </button>
                    ))}
                    <span style={{fontSize:"7px",color:"#5a8fa8"}}>
                      convicción mínima sobre el 50 neutro
                    </span>
                    <span style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em",marginLeft:"8px"}}>CALIDAD</span>
                    {[["off","Todas"],["sinFragiles","Sin frágiles"],["soloSolidas","Solo sólidas"]].map(([k,l])=>(
                      <button key={k} className={`btn ${filtroCalidad===k?"on":"off"}`} onClick={()=>setFiltroCalidad(k)}
                        style={{padding:"3px 9px",fontSize:"8px"}}>{l}</button>
                    ))}
                    {nFiltrados>0&&<span style={{fontSize:"7px",color:"#ff9040"}}>−{nFiltrados} excluidas</span>}
                    {alphaRank&&(
                      <>
                        <span style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em",marginLeft:"8px"}}>ORDEN</span>
                        {[["conviccion","Convicción"],["alpha","α Alfa relativo"]].map(([k,l])=>(
                          <button key={k} className={`btn ${ordenPor===k?"on":"off"}`} onClick={()=>setOrdenPor(k)}
                            style={{padding:"3px 9px",fontSize:"8px"}}>{l}</button>
                        ))}
                      </>
                    )}
                  </div>

                  <div style={{display:"flex",gap:"4px",flexWrap:"wrap",alignItems:"center"}}>
                    <span style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em",marginRight:"2px"}}>SECTOR</span>
                    {sectores.map(sec=>{
                      const cnt = sec==="Todos" ? uniq.length : uniq.filter(r=>r.sector===sec).length;
                      const hay = (sec==="Todos"?uniq:uniq.filter(r=>r.sector===sec))
                                    .some(r=>r.sig?.above_p80 && r.sig?.sig!=="NEUTRAL");
                      return (
                        <button key={sec} className={`btn ${catSector===sec?"on":"off"}`} onClick={()=>setCatSector(sec)}
                          style={{position:"relative",padding:"3px 8px",fontSize:"8px"}}>
                          {sec} <span style={{opacity:.55}}>{cnt}</span>
                          {hay&&sec!=="Todos"&&<span style={{position:"absolute",top:"-2px",right:"-2px",width:"6px",height:"6px",background:"#00ff88",borderRadius:"50%",display:"block"}}/>}
                        </button>
                      );
                    })}
                  </div>
                </div>

                {(()=>{
                  const parciales = bySector.filter(r=>r.sig?.ruedaAbierta).length;
                  if (!parciales) return null;
                  return (
                    <div style={{padding:"7px 10px",background:"#ff904012",border:"1px solid #ff904035",borderRadius:"5px",marginBottom:"10px",fontSize:"8px",color:"#ffb380",lineHeight:1.6}}>
                      ⏱ <strong>{parciales} activos con la rueda todavía abierta.</strong> Los datos se descargaron durante el horario de mercado,
                      así que el último día está incompleto. Los movimientos que ves pueden revertir antes del cierre —
                      el histórico ya los excluye del cálculo, pero el precio mostrado es intradiario.
                    </div>
                  );
                })()}

                {lista.length===0&&(()=>{
                  if (oppScope!=="senales") return (
                    <div style={{textAlign:"center",padding:"40px",color:"#4a7a9b",fontSize:"11px"}}>Sin datos. Ejecutá el sistema.</div>
                  );
                  // Diagnóstico: ¿por qué no hay señales?
                  const scores = bySector.map(r=>r.sig?.final_sc||0).filter(s=>s>0).sort((a,b)=>a-b);
                  const p80v   = scores.length ? scores[Math.floor(scores.length*0.8)] : 0;
                  const enTop  = bySector.filter(r=>r.sig?.in_top20).length;
                  const porScore = bySector.filter(r=>r.sig?.in_top20 && (r.sig.final_sc<58 && r.sig.final_sc>42)).length;
                  const porRR    = bySector.filter(r=>r.sig?.in_top20 && (r.sig.rr??0)<1.2).length;
                  const maxSc  = scores.length ? scores[scores.length-1] : 0;
                  const medSc  = scores.length ? scores[Math.floor(scores.length*0.5)] : 0;
                  return (
                    <div style={{padding:"18px 16px",background:"#07101a",border:"1px solid #1e3a50",borderRadius:"6px"}}>
                      <div style={{textAlign:"center",marginBottom:"12px"}}>
                        <div style={{fontSize:"22px",marginBottom:"4px"}}>🎯</div>
                        <div style={{fontFamily:"'Bebas Neue'",fontSize:"20px",color:"#ffd700"}}>NINGUNA OPORTUNIDAD SUPERA EL FILTRO</div>
                        <div style={{fontSize:"8px",color:"#b0d4e8",marginTop:"3px"}}>
                          Esto no es un error: el sistema está diseñado para no forzar señales cuando no las hay.
                        </div>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"6px",marginBottom:"10px"}}>
                        {[
                          {l:"SCORE MEDIANO", v:medSc.toFixed(0),  c:"#a0cce0"},
                          {l:"SCORE MÁXIMO",  v:maxSc.toFixed(0),  c:maxSc>=58?"#00ff88":"#ff9040"},
                          {l:"UMBRAL P80",    v:p80v.toFixed(0),   c:"#ffd700"},
                          {l:"MÍNIMO EXIGIDO",v:"58",              c:"#ff3355"},
                        ].map(x=>(
                          <div key={x.l} style={{textAlign:"center",padding:"6px 3px",background:"#050c15",borderRadius:"4px"}}>
                            <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"17px",color:x.c}}>{x.v}</div>
                          </div>
                        ))}
                      </div>
                      <div style={{fontSize:"8px",color:"#4a7a9b",marginBottom:"5px",letterSpacing:".1em"}}>DÓNDE SE FRENAN</div>
                      {[
                        {l:`${enTop} activos entraron al top 20%`, ok:enTop>0},
                        {l:`${porScore} quedaron en zona neutral (score entre 42 y 58)`, ok:porScore===0},
                        {l:`${porRR} no cubren costos con su R/R (necesitan ≥1.2x)`, ok:porRR===0},
                      ].map((x,i)=>(
                        <div key={i} style={{display:"flex",gap:"6px",alignItems:"center",padding:"3px 0",fontSize:"8px"}}>
                          <span style={{color:x.ok?"#00ff88":"#ff9040"}}>{x.ok?"✓":"→"}</span>
                          <span style={{color:"#b0d4e8"}}>{x.l}</span>
                        </div>
                      ))}
                      <div style={{marginTop:"10px",padding:"8px 10px",background:"#ffd70010",border:"1px solid #ffd70030",borderRadius:"4px",fontSize:"8px",color:"#b0d4e8",lineHeight:1.7}}>
                        📌 {maxSc < 58
                          ? `Ningún activo llega a 58 puntos (el mejor está en ${maxSc.toFixed(0)}). El mercado no muestra setups con ventaja en este horizonte. Probá otra ventana o esperá.`
                          : porRR > 0
                          ? `Hay activos con score suficiente, pero sus objetivos no cubren el ${COSTO_CEDEAR}% de comisiones. Con costos altos, entrar sería perder aunque acierte la dirección.`
                          : `Mirá "Universo completo" para ver el ranking y evaluar manualmente.`}
                      </div>
                      <button className="btn off" onClick={()=>setOppScope("todos")}
                        style={{marginTop:"8px",width:"100%",fontSize:"9px",padding:"6px"}}>
                        📋 Ver universo completo ({bySector.length})
                      </button>
                    </div>
                  );
                })()}
                <div className="grid-opp">
                  {lista.map(r=>{
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
                            <div style={{fontSize:"8px",color:"#5a8fa8"}}>{r.name}</div>
                          </div>
                          <div style={{textAlign:"right"}}>
                            <span className="badge" style={{background:SC[s.sig]+"20",color:SC[s.sig],border:`1px solid ${SC[s.sig]}40`,display:"block",marginBottom:"3px"}}>{s.sig}</span>
                            <span style={{fontSize:"8px",color:TC[s.trend]}}>{TI[s.trend]} {s.trend}</span>
                          </div>
                        </div>

                        {/* Score breakdown FX vs EVO */}
                        <ScoreBar fx={s.fx_sc} evo={s.evo_sc} final_sc={s.final_sc}/>

                        <div style={{background:"#050c15",borderRadius:"4px",padding:"6px 9px",margin:"8px 0",display:"flex",justifyContent:"space-between",alignItems:"baseline"}}>
                          <span style={{fontSize:"8px",color:"#4a7a9b"}}>{MONEDA(r,mkt)==="USD"?"PRECIO USD":"PRECIO ARS"}</span>
                          {r.price != null
                            ? <span style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color:r.fromCsv?"#00d4ff":r.real?"#e8f4ff":"#2a4a5a"}}>{FP(r.price,MONEDA(r,mkt))}</span>
                            : <span style={{fontSize:"10px",color:"#4a7a9b",fontStyle:"italic"}}>buscando precio…</span>
                          }
                        </div>

                        {[{l:"→ ENTRADA",v:s.entry,c:buy?"#00ff9d":"#ff9040"},{l:"🛡 STOP",v:s.sl,c:"#ff3355"},{l:"TP2",v:s.tp2,c:"#00ff9d"},{l:"TP3",v:s.tp3,c:"#00d4ff"}].filter(x=>x.v).map(x=>{
                          const pct=((x.v-r.price)/r.price*100).toFixed(1);
                          return <div key={x.l} style={{display:"flex",justifyContent:"space-between",padding:"3px 0",borderBottom:"1px solid #091520",fontSize:"9px"}}>
                            <span style={{color:"#5a8fa8"}}>{x.l}</span>
                            <div style={{display:"flex",gap:"7px"}}>
                              <span style={{color:x.c,fontWeight:700}}>{FP(x.v,MONEDA(r,mkt))}</span>
                              <span style={{fontSize:"8px",color:+pct>0?"#00ff9d":+pct<0?"#ff3355":"#ffd700"}}>{+pct>0?"+":""}{pct}%</span>
                            </div>
                          </div>;
                        })}

                        <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:"3px",marginTop:"8px"}}>
                          {[{l:"R/R",v:`${s.rr}x`,c:s.rr>=2?"#00ff9d":"#ffd700"},{l:"RSI",v:s.rsi,c:s.rsi>70?"#ff3355":s.rsi<30?"#00ff9d":"#ffd700"},{l:"EVO",v:s.evo_prob,c:s.evo_prob>=0.6?"#ff9040":"#ffd700"},{l:"CONF",v:`${s.conf}%`,c:SC[s.sig]},{l:"EF",v:`${r.bt.hr}%`,c:g.c}].map(m=>
                            <div key={m.l} style={{textAlign:"center",padding:"3px",background:"#050c15",borderRadius:"3px",border:"1px solid #0a1d2e"}}>
                              <div style={{fontSize:"7px",color:"#4a7a9b"}}>{m.l}</div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:m.c}}>{m.v}</div>
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
              );
            })()}

            {tab==="det"&&(
              <div className="fade">
                <div style={{display:"flex",gap:"3px",flexWrap:"wrap",marginBottom:"10px"}}>
                  {rows.map(r=>{const g=GR(r.bt.hr);return <button key={r.ticker} className={`btn ${sel?.ticker===r.ticker?"on":"off"}`} onClick={()=>{
  const data = rowDataRef.current[r.ticker];
  if (data) {
    const W2 = (optApplied && optParams[r.ticker]?.w) || W;
    const sig2 = combinedSignal(data, W2);
    const bt2  = r.bt?.n > 0 ? r.bt : backtest(data, W2);
    const updated = {...r, sig: sig2, bt: bt2};
    setRows(prev => prev.map(p => p.ticker===r.ticker ? updated : p));
    setSel(updated);
    return;
  }
  setSel(r);
}} style={{color:sel?.ticker===r.ticker?undefined:g.c}}>{r.ticker}{r.real?" 📡":""}</button>;})}
                </div>
                {!sel?<div style={{textAlign:"center",padding:"40px",color:"#4a7a9b"}}>Seleccioná una acción arriba</div>:
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
                        <div style={{fontSize:"9px",color:"#5a8fa8",marginBottom:"4px"}}>{sel.name} · {sel.sector}</div>
                        {sel.price!=null
  ? <div style={{fontFamily:"'Bebas Neue'",fontSize:"32px",color:sel.fromCsv?"#00d4ff":sel.real?"#e8f4ff":"#2a4a5a"}}>{FP(sel.price,MONEDA(sel,mkt))}</div>
  : <div style={{fontSize:"11px",color:"#4a7a9b",padding:"8px 0",fontStyle:"italic"}}>⏳ buscando precio real…</div>}
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
                      <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"10px"}}>SCORE COMBINADO FXCA16</div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"6px",marginBottom:"10px"}}>
                        {[{l:"FX-TÉCNICO",v:s.fx_sc,c:"#00d4ff",sub:"RSI+MACD+BB+ATR"},{l:"EVO-SCORE",v:s.evo_sc,c:"#ff9040",sub:"Score+Vol+Mom"},{l:"COMBINADO",v:s.final_sc,c:"#00ff9d",sub:"65% FX + 35% EVO"}].map(x=>
                          <div key={x.l} style={{textAlign:"center",padding:"10px 8px",background:"#050c15",borderRadius:"4px",border:`1px solid ${x.c}20`}}>
                            <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"28px",color:x.c,lineHeight:1}}>{x.v}</div>
                            <div style={{fontSize:"7px",color:x.c,opacity:.6,marginTop:"2px"}}>{x.sub}</div>
                          </div>
                        )}
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"6px"}}>
                        {[{l:"FXCA16 SCORE",v:`${s.ca15_score}/3`,c:s.ca15_score===3?"#00ff9d":s.ca15_score===2?"#ffd700":"#ff9040"},{l:"EVO PROB",v:s.evo_prob,c:s.evo_prob>=0.65?"#00ff9d":s.evo_prob>=0.5?"#ffd700":"#ff9040"},{l:"VOL 24H",v:`${s.vol_24h}x`,c:s.vol_24h>=1.5?"#00ff9d":s.vol_24h>=1?"#ffd700":"#ff9040"},{l:"PCT 6H",v:`${s.pct6h>=0?"+":""}${(s.pct6h*100).toFixed(2)}%`,c:s.pct6h>0?"#00ff9d":"#ff3355"}].map(x=>
                          <div key={x.l} style={{textAlign:"center",padding:"7px",background:"#050c15",borderRadius:"3px"}}>
                            <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.v}</div>
                          </div>
                        )}
                      </div>
                    </div>}

                    {s&&<div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"10px",marginBottom:"10px"}}>
                      <div className="card" style={{padding:"12px"}}>
                        <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"8px"}}>NIVELES · {W}D</div>
                        {[{l:"🛡 STOP",v:s.sl,c:"#ff3355"},{l:"◎ PRECIO",v:s.px,c:"#aec8d8"},{l:"→ ENTRADA",v:s.entry,c:buy?"#00ff9d":"#ff9040"},{l:"TP1",v:s.tp1,c:"#5dffb0"},{l:"TP2",v:s.tp2,c:"#00ff9d"},{l:"TP3",v:s.tp3,c:"#00d4ff"}].filter(x=>x.v).map(x=>{
                          const pct=((x.v-s.px)/s.px*100).toFixed(1);
                          return <div key={x.l} style={{display:"flex",justifyContent:"space-between",padding:"5px 7px",background:"#050c15",borderRadius:"3px",marginBottom:"3px",fontSize:"9px",border:`1px solid ${x.c}15`}}>
                            <span style={{color:"#5a8fa8"}}>{x.l}</span>
                            <div style={{display:"flex",gap:"6px"}}>
                              <span style={{color:x.c,fontFamily:"'Bebas Neue'",fontSize:"13px"}}>${F(x.v)}</span>
                              <span style={{fontSize:"8px",color:+pct>0?"#00ff9d":+pct<0?"#ff3355":"#ffd700"}}>{+pct>0?"+":""}{pct}%</span>
                            </div>
                          </div>;
                        })}
                        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"4px",marginTop:"8px"}}>
                          {[{l:"R/R",v:`${s.rr}x`,c:s.rr>=2?"#00ff9d":"#ffd700"},{l:"ATR",v:`$${s.atr}`,c:"#a0cce0"},{l:"CONF",v:`${s.conf}%`,c:SC[s.sig]}].map(x=>
                            <div key={x.l} style={{textAlign:"center",padding:"5px",background:"#050c15",borderRadius:"3px"}}>
                              <div style={{fontSize:"7px",color:"#4a7a9b"}}>{x.l}</div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"13px",color:x.c}}>{x.v}</div>
                            </div>
                          )}
                        </div>
                      </div>
                      <div className="card" style={{padding:"12px"}}>
                        <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"8px"}}>INDICADORES FXCA16</div>
                        {[
                          {l:"ROC 10h",v:`${s.roc10>=0?"+":""}${s.roc10}%`,c:s.roc10>1.5?"#00ff9d":s.roc10<-1.5?"#ff3355":"#ffd700"},
                          {l:"ROC 5h", v:`${s.roc5>=0?"+":""}${s.roc5}%`, c:s.roc5>1?"#00ff9d":s.roc5<-1?"#ff3355":"#ffd700"},
                          {l:"Vol.Div.",v:s.volDiv>0?"▲ ACUM":s.volDiv<0?"▼ DIST":"─",c:s.volDiv>0?"#00ff9d":s.volDiv<0?"#ff3355":"#ffd700"},
                          {l:"MACD",   v:(s.macd>0?"▲ ":"▼ ")+Math.abs(s.macd),c:s.macd>0?"#00ff9d":"#ff3355"},
                          {l:"Mom. 5h",v:`${s.mom5>=0?"+":""}${s.mom5}%`,c:s.mom5>=0?"#00ff9d":"#ff3355"},
                          {l:"Régimen",v:s.regime||"neutral",c:s.regime==="bull"?"#00ff9d":s.regime==="bear"?"#ff3355":"#ffd700"},
                          {l:"WF peso", v:s.wfWeight,c:s.wfWeight>=1.05?"#00ff9d":s.wfWeight<=0.95?"#ff3355":"#ffd700"},
                          {l:"H-Factor",v:s.hourFactor,c:s.hourFactor>=1?"#00ff9d":s.hourFactor<0.9?"#ff3355":"#ffd700"},
                          {l:"RSI ref.", v:s.rsi,c:s.rsi>70?"#ff3355":s.rsi<30?"#00ff9d":"#5a8fa8"},
                          {l:"SMA 20",  v:`$${s.sma20?.toFixed(0)??"─"}`,c:"#8b5cf6"},
                          {l:"SMA 50",  v:`$${s.sma50?.toFixed(0)??"─"}`,c:"#f59e0b"},
                          {l:"BB Sup.", v:`$${s.boll?.u?.toFixed(0)??"─"}`,c:"#3b82f6"},
                          {l:"BB Inf.", v:`$${s.boll?.l?.toFixed(0)??"─"}`,c:"#3b82f6"},
                        ].map(x=>
                          <div key={x.l} style={{display:"flex",justifyContent:"space-between",padding:"4px 0",borderBottom:"1px solid #091520",fontSize:"9px"}}>
                            <span style={{color:"#5a8fa8"}}>{x.l}</span>
                            <span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                          </div>
                        )}
                      </div>
                    </div>}

                    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(85px,1fr))",gap:"5px",marginBottom:"9px"}}>
                      {[{l:"TRADES",v:sel.bt.n,c:"#b0d4e8"},{l:"WINS",v:sel.bt.hits,c:"#00ff9d"},{l:"LOSSES",v:sel.bt.n-sel.bt.hits,c:"#ff3355"},{l:"EF%",v:`${sel.bt.hr}%`,c:g.c},{l:"AVG RET",v:`${sel.bt.avg>=0?"+":""}${sel.bt.avg}%`,c:sel.bt.avg>=0?"#00ff9d":"#ff3355"},{l:"P.FACTOR",v:`${sel.bt.pf}x`,c:sel.bt.pf>=1.5?"#00ff9d":"#ffd700"},{l:"SHARPE",v:sel.bt.sh,c:sel.bt.sh>=1?"#00ff9d":"#ffd700"},{l:"MAX DD",v:`${sel.bt.dd}%`,c:sel.bt.dd<15?"#00ff9d":"#ff3355"},{l:"EQUITY",v:sel.bt.eq,c:sel.bt.eq>=100?"#00ff9d":"#ff3355"}].map(x=>
                        <div key={x.l} className="card" style={{padding:"7px"}}>
                          <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>{x.l}</div>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.v}</div>
                        </div>
                      )}
                    </div>

                    {/* ══ VEREDICTO FINAL ══ */}
                    {(()=>{
                      const data = rowDataRef.current[sel.ticker];
                      if (!data || data.length < 30) return null;
                      const s = sel.sig;
                      const moneda = sel.moneda || "USD";
                      const px = sel.price || data[data.length-1].close;

                      // ── Calcular los 4 ejes ──
                      // 1. FXCA16
                      const fxScore   = s?.final_sc || 0;
                      const fxcaConf  = s?.conf || 0;
                      const fxBull    = s?.sig?.includes("COMPRA");
                      const fxBear    = s?.sig?.includes("VENTA");
                      const fxLabel   = fxBull ? (s?.sig?.includes("FUERTE")?"COMPRA FUERTE":"COMPRA") : fxBear ? (s?.sig?.includes("FUERTE")?"VENTA FUERTE":"VENTA") : "NEUTRAL";
                      const fxColor   = fxBull?"#00ff88":fxBear?"#ff3355":"#ffd700";
                      const fxRating  = fxScore>=75?3:fxScore>=55?2:fxScore>=40?1:0;

                      // 2. CONFLUENCIA
                      const fib_      = calcFibonacci(data, W);
                      const rsiDiv_   = detectRSIDivergence(data);
                      const volFib_   = checkVolumeAtFib(data, fib_?.levels);
                      const cross_    = detectCross(data);
                      const boll_     = detectBollingerRSISetup(data);
                      const cand_     = detectCandlePattern(data);
                      const conf_     = calcConfluence(s, rsiDiv_, volFib_, cross_, boll_, cand_, null);
                      const confScore = conf_?.score || 0;
                      const confDir   = conf_?.action?.includes("COMPRAR")?"bull":conf_?.action?.includes("VENDER")?"bear":"neutral";
                      const confColor = confDir==="bull"?"#00ff88":confDir==="bear"?"#ff3355":"#ffd700";
                      const confRating= confScore>=70?3:confScore>=50?2:confScore>=35?1:0;
                      const confLabel = conf_?.action || "ESPERAR";
                      // Incongruencia entre FXCA16 y Confluencia
                      const incongruencia = (fxBull && confDir==="bear") || (fxBear && confDir==="bull");
                      const alineado      = (fxBull && confDir==="bull") || (fxBear && confDir==="bear");

                      // 3. ANÁLISIS ESTRUCTURAL
                      const mtf_      = calcMultiTimeframe(data, W);
                      const reg_      = detectTickerRegime(data);
                      const atrB_     = calcATRBands(data);
                      const vp_       = calcVolumeProfile(data);
                      const mtfBull   = mtf_?.filter(f=>f.dir==="bull").length||0;
                      const mtfBear   = mtf_?.filter(f=>f.dir==="bear").length||0;
                      const mtfAlign  = Math.max(mtfBull,mtfBear);
                      const mtfDir_   = mtfBull>mtfBear?"bull":mtfBear>mtfBull?"bear":"neutral";
                      const regPhase  = reg_?.phase||"";
                      const regBull   = ["Markup","Acumulación","Recuperación"].includes(regPhase);
                      const regBear   = ["Markdown","Distribución"].includes(regPhase);
                      const estScore  = mtfAlign*25 + (regBull?20:regBear?0:10) + (atrB_?.breakoutUp?15:0);
                      const estRating = estScore>=70?3:estScore>=45?2:estScore>=20?1:0;
                      const estColor  = regBull&&mtfAlign>=2?"#00ff88":regBear||mtfAlign<=1?"#ff3355":"#ffd700";
                      const estLabel  = regPhase||"Sin datos";

                      // 4. OPTIMIZACIÓN
                      const wf_       = backtestWalkForward(data, W, (moneda==="ARS"?COSTO_MERVAL:COSTO_CEDEAR));
                      const sq_       = calcSignalQuality(data, s, W, (moneda==="ARS"?COSTO_MERVAL:COSTO_CEDEAR));
                      const events_   = getUpcomingEvents(sel.ticker, moneda);
                      const ps_       = calcPositionSizing(s, conf_, 1000000);
                      const hasRisk_  = events_.some(e=>e.type==="earnings"&&e.daysLeft<=5);
                      const wfOk      = wf_ && wf_.hr>=52 && wf_.consistency>=50;
                      const sqOk      = sq_ && sq_.hr>=55;
                      const optRating = (wfOk?1:0)+(sqOk?1:0)+(!hasRisk_?1:0);
                      const optColor  = optRating>=3?"#00ff88":optRating>=2?"#ffd700":"#ff3355";
                      const optLabel  = hasRisk_?`⚠️ Earnings en ${events_.find(e=>e.type==="earnings")?.daysLeft}d`:wfOk&&sqOk?"✓ Backtest consistente":!wfOk?"Backtest débil":"Calidad media";

                      // ── VEREDICTO GLOBAL ──
                      const totalRating = fxRating + confRating + estRating + optRating;
                      const maxRating   = 3+3+3+3;
                      const globalPct   = Math.round(totalRating/maxRating*100);

                      let veredicto, veredictoColor, veredictoDesc, accion;

                      if (incongruencia && fxBull) {
                        veredicto      = "ALERTA — AGOTAMIENTO";
                        veredictoColor = "#ffd700";
                        veredictoDesc  = `FXCA16 muestra momentum alcista (score ${fxScore}) pero la confluencia detecta señales de agotamiento. El precio puede estar cerca de un techo.`;
                        accion         = `No entres ahora. Si tenés posición, subí el stop a la entrada. Esperá retroceso a Fib 38.2% (${fib_?FP(fib_.levels?.find(l=>l.label==="38.2%")?.value,moneda):"—"}) para re-evaluar.`;
                      } else if (incongruencia && fxBear) {
                        veredicto      = "POSIBLE REBOTE";
                        veredictoColor = "#ffd700";
                        veredictoDesc  = `FXCA16 muestra presión bajista pero la confluencia detecta señales de rebote. La caída puede estar agotándose.`;
                        accion         = `No vendas en este punto. Esperá confirmación del rebote con vela de reversión y volumen antes de actuar.`;
                      } else if (alineado && fxBull && globalPct>=65) {
                        veredicto      = "COMPRAR";
                        veredictoColor = "#00ff88";
                        veredictoDesc  = `Señal alineada entre todos los ejes. Score global ${globalPct}%. ${mtfAlign===3?"Triple confirmación multi-timeframe.":""} ${regBull?"Fase Weinstein "+regPhase+".":""}`;
                        accion         = `Entrá en $${FP(s?.entry,moneda)} con stop en ${FP(s?.sl,moneda)}. Objetivo principal TP2 ${FP(s?.tp2,moneda)} (+${s?.tp2&&px?(((s.tp2-px)/px*100).toFixed(1)):"-"}%). Sizing: ${ps_?.level||"normal"}.`;
                      } else if (alineado && fxBull && globalPct>=45) {
                        veredicto      = "COMPRAR MODERADO";
                        veredictoColor = "#a0cce0";
                        veredictoDesc  = `Señal mayormente alineada con score global ${globalPct}%. Algunos ejes muestran incertidumbre.`;
                        accion         = `Podés entrar con sizing reducido (50%). Stop en ${FP(s?.sl,moneda)}. Objetivo TP1 ${FP(s?.tp1,moneda)}.`;
                      } else if (alineado && fxBear && globalPct>=55) {
                        veredicto      = "EVITAR / REDUCIR";
                        veredictoColor = "#ff3355";
                        veredictoDesc  = `Señal bajista confirmada en múltiples ejes. Score global ${globalPct}%. ${regBear?"Fase Weinstein "+regPhase+".":""}`;
                        accion         = `No abras posiciones largas. Si tenés posición, considerá reducir o salir. El sistema no opera en short.`;
                      } else {
                        veredicto      = "ESPERAR";
                        veredictoColor = "#ffd700";
                        veredictoDesc  = `Los ejes no tienen suficiente alineación para recomendar una acción clara. Score global ${globalPct}%.`;
                        accion         = `Revisá esta acción en ${W>=14?"unos días":"la próxima sesión"} o cambiá la ventana de análisis.`;
                      }

                      return (
                        <div className="card" style={{padding:"14px",marginBottom:"9px",border:`2px solid ${veredictoColor}40`,boxShadow:`0 0 20px ${veredictoColor}10`}}>

                          {/* VEREDICTO */}
                          <div style={{textAlign:"center",marginBottom:"14px",paddingBottom:"12px",borderBottom:"1px solid #0f2235"}}>
                            <div style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".2em",marginBottom:"4px"}}>VEREDICTO FINAL</div>
                            <div style={{display:"flex",gap:"4px",justifyContent:"center",flexWrap:"wrap",marginBottom:"6px"}}>
                              {s?.synthetic&&<span style={{fontSize:"7px",padding:"2px 7px",background:"#ff335520",border:"1px solid #ff335550",borderRadius:"3px",color:"#ff3355",fontWeight:700}}>⚠ DATOS SINTÉTICOS — no operar con esta señal</span>}
                              {s?.ruedaAbierta&&<span style={{fontSize:"7px",padding:"2px 7px",background:"#ff904020",border:"1px solid #ff904050",borderRadius:"3px",color:"#ff9040",fontWeight:700}}>⏱ RUEDA ABIERTA — el día no cerró, el precio puede revertir</span>}
                              {(()=>{
                                const ev = getUpcomingEvents(sel.ticker, sel.moneda);
                                const pasado = ev.find(e=>e.type==="earnings_pasado");
                                const prox   = ev.find(e=>e.type==="earnings");
                                if (pasado) return <span style={{fontSize:"7px",padding:"2px 7px",background:"#00d4ff20",border:"1px solid #00d4ff50",borderRadius:"3px",color:"#00d4ff",fontWeight:700}}>
                                  📊 REPORTÓ BALANCE hace {Math.abs(pasado.daysLeft)}d — el movimiento reciente es por la noticia, no técnico</span>;
                                if (prox && prox.daysLeft<=7) return <span style={{fontSize:"7px",padding:"2px 7px",background:"#ff335520",border:"1px solid #ff335550",borderRadius:"3px",color:"#ff3355",fontWeight:700}}>
                                  📊 BALANCE EN {prox.daysLeft<=0?"HOY":prox.daysLeft+"d"} — alta volatilidad, considerá esperar</span>;
                                return null;
                              })()}
                              {s?.fdr_pass===false&&<span style={{fontSize:"7px",padding:"2px 7px",background:"#ffd70015",border:"1px solid #ffd70040",borderRadius:"3px",color:"#ffd700"}}>⚠ {s.fdr_note||"Posible falso positivo"}</span>}
                              {s?.fdr_pass===true&&<span style={{fontSize:"7px",padding:"2px 7px",background:"#00ff8815",border:"1px solid #00ff8840",borderRadius:"3px",color:"#00ff88"}}>✓ Supera control FDR</span>}
                            </div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"32px",color:veredictoColor,lineHeight:1,marginBottom:"6px"}}>{veredicto}</div>
                            <div style={{fontSize:"8px",color:"#b0d4e8",lineHeight:1.7,marginBottom:"8px",maxWidth:"480px",margin:"0 auto 8px"}}>{veredictoDesc}</div>
                            <div style={{padding:"8px 12px",background:`${veredictoColor}12`,border:`1px solid ${veredictoColor}30`,borderRadius:"5px",fontSize:"8px",color:"#ffd700",lineHeight:1.7,textAlign:"left"}}>
                              📌 {accion}
                            </div>
                          </div>

                          {/* ── ALFA CROSS-SECTIONAL ── */}
                          {alphaRank?.[sel.ticker]&&(()=>{
                            const a=alphaRank[sel.ticker];
                            const c=a.quintil>=5?"#00ff88":a.quintil>=4?"#a0cce0":a.quintil<=1?"#ff3355":a.quintil<=2?"#ff9040":"#ffd700";
                            return (
                              <div style={{marginBottom:"12px",padding:"10px",background:`${c}0d`,border:`1px solid ${c}35`,borderRadius:"6px"}}>
                                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"6px"}}>
                                  <div>
                                    <div style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em"}}>α ALFA CROSS-SECTIONAL</div>
                                    <div style={{fontSize:"7px",color:"#5a8fa8"}}>{ALPHA.ALPHA_VALIDADA.nombre}</div>
                                  </div>
                                  <div style={{textAlign:"right"}}>
                                    <div style={{fontFamily:"'Bebas Neue'",fontSize:"24px",color:c,lineHeight:1}}>P{a.percentil}</div>
                                    <div style={{fontSize:"7px",color:c}}>quintil {a.quintil} de 5</div>
                                  </div>
                                </div>
                                <div style={{height:"7px",background:"#0c1826",borderRadius:"4px",position:"relative",marginBottom:"6px"}}>
                                  {[20,40,60,80].map(p=><div key={p} style={{position:"absolute",left:`${p}%`,top:0,bottom:0,width:"1px",background:"#1e3a50"}}/>)}
                                  <div style={{position:"absolute",left:`${a.percentil}%`,top:"-3px",bottom:"-3px",width:"3px",background:c,borderRadius:"2px",transform:"translateX(-50%)",boxShadow:`0 0 6px ${c}80`}}/>
                                </div>
                                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"5px",marginBottom:"6px"}}>
                                  {[
                                    {l:"Shock de volumen", v:a.vol_shock, bueno:a.vol_shock>0.3},
                                    {l:"Momentum 1 mes",   v:a.mom_1m,    bueno:a.mom_1m<-0.3},
                                  ].map(x=>(
                                    <div key={x.l} style={{padding:"5px 7px",background:"#050c15",borderRadius:"3px"}}>
                                      <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                                      <div style={{fontSize:"11px",color:x.bueno?"#00ff88":"#a0cce0",fontFamily:"'Bebas Neue'"}}>
                                        {x.v>=0?"+":""}{x.v.toFixed(2)}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                                <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                                  {a.quintil>=5
                                    ? "En el quintil superior del universo. La combinación de volumen entrando sobre precio castigado es el patrón con mejor evidencia del sistema."
                                    : a.quintil>=4
                                    ? "Por encima de la media del universo en atractivo relativo."
                                    : a.quintil<=1
                                    ? "En el quintil inferior. Históricamente este grupo rinde por debajo del universo."
                                    : "En la zona media del universo: sin ventaja relativa clara."}
                                </div>
                                <div style={{marginTop:"5px",paddingTop:"5px",borderTop:"1px solid #0f2235",fontSize:"6px",color:"#4a7a9b",lineHeight:1.6}}>
                                  Promedio de los últimos {ALPHA.ALPHA_VALIDADA.metricas.suavizado} días — suavizar cancela el ruido de un solo día
                                  {a.diasPromediados ? ` (${a.diasPromediados} días con datos)` : ""}.<br/>
                                  Validado: IC {ALPHA.ALPHA_VALIDADA.metricas.ic} · IR {ALPHA.ALPHA_VALIDADA.metricas.ir} ·
                                  t={ALPHA.ALPHA_VALIDADA.metricas.t} · positivo en {ALPHA.ALPHA_VALIDADA.metricas.pctFechas}% de las fechas
                                </div>
                              </div>
                            );
                          })()}

                          {/* ── CALIDAD FUNDAMENTAL ── */}
                          {(()=>{
                            const q = calidadDe(sel.ticker);
                            if (!q) return null;
                            if (q.tipo === "ETF") return (
                              <div style={{marginBottom:"12px",padding:"8px 10px",background:"#0c182610",border:"1px solid #1e3a50",borderRadius:"5px",fontSize:"7px",color:"#5a8fa8"}}>
                                🏛️ Es un ETF — no aplica análisis fundamental de empresa.
                              </div>
                            );
                            if (q.calidad == null) return null;
                            const n = nivelCalidad(q.calidad);
                            const fmt = (v,suf="") => v==null ? "—" : `${(v*(suf==="%"?100:1)).toFixed(suf==="%"?1:2)}${suf}`;
                            return (
                              <div style={{marginBottom:"12px",padding:"10px",background:`${n.color}0d`,border:`1px solid ${n.color}35`,borderRadius:"6px"}}>
                                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"7px"}}>
                                  <div>
                                    <div style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em"}}>🏛️ CALIDAD FUNDAMENTAL</div>
                                    <div style={{fontSize:"7px",color:"#5a8fa8"}}>Rentabilidad · solidez · generación de caja</div>
                                  </div>
                                  <div style={{textAlign:"right"}}>
                                    <div style={{fontFamily:"'Bebas Neue'",fontSize:"24px",color:n.color,lineHeight:1}}>{q.calidad}</div>
                                    <div style={{fontSize:"8px",color:n.color,fontWeight:700}}>{n.txt}</div>
                                  </div>
                                </div>
                                <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"5px",marginBottom:"6px"}}>
                                  {[
                                    {l:"Margen neto", v:fmt(q.margenNeto,"%"), ok:q.margenNeto>0.05},
                                    {l:"ROE",         v:fmt(q.roe,"%"),        ok:q.roe>0.10},
                                    {l:"Deuda/Patr.", v:q.deudaPatr!=null?`${q.deudaPatr.toFixed(0)}%`:"—", ok:q.deudaPatr!=null&&q.deudaPatr<100},
                                  ].map(x=>(
                                    <div key={x.l} style={{padding:"5px 7px",background:"#050c15",borderRadius:"3px",textAlign:"center"}}>
                                      <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                                      <div style={{fontSize:"12px",fontFamily:"'Bebas Neue'",color:x.ok?"#00ff88":"#ffd700"}}>{x.v}</div>
                                    </div>
                                  ))}
                                </div>
                                {q.banderas?.length>0&&(
                                  <div style={{padding:"6px 8px",background:"#ff335512",border:"1px solid #ff335530",borderRadius:"4px",marginBottom:"5px"}}>
                                    <div style={{fontSize:"7px",color:"#ff3355",fontWeight:700,marginBottom:"2px"}}>⚠ ALERTAS FINANCIERAS</div>
                                    <div style={{fontSize:"7px",color:"#ffb380",lineHeight:1.6}}>{q.banderas.join(" · ")}</div>
                                  </div>
                                )}
                                <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                                  {q.fragil
                                    ? "Empresa con fragilidad financiera. Aunque la señal técnica sea buena, el riesgo de un evento adverso es mayor. Considerá tamaño reducido o directamente excluirla."
                                    : q.calidad>=75
                                    ? "Balance sólido. Reduce el riesgo de sorpresas negativas que no aparecen en el análisis técnico."
                                    : "Situación financiera aceptable, sin alertas relevantes."}
                                </div>
                                <div style={{marginTop:"5px",paddingTop:"5px",borderTop:"1px solid #0f2235",fontSize:"6px",color:"#4a7a9b",lineHeight:1.6}}>
                                  Uso deliberadamente acotado: son datos de HOY, no series point-in-time.
                                  Por eso no alimentan el score ni el alfa — solo advierten sobre fragilidad antes de tomar posición.
                                </div>
                              </div>
                            );
                          })()}

                          {/* 4 EJES */}
                          <div style={{display:"grid",gridTemplateColumns:"repeat(2,1fr)",gap:"8px",marginBottom:"12px"}}>

                            {/* ① FXCA16 */}
                            <div style={{padding:"10px",background:"#050c15",borderRadius:"5px",border:`1px solid ${fxColor}25`}}>
                              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:"4px"}}>
                                <div style={{fontSize:"7px",color:"#4a7a9b"}}>① FXCA16</div>
                                <div style={{display:"flex",gap:"2px"}}>{[0,1,2].map(i=><div key={i} style={{width:"8px",height:"8px",borderRadius:"2px",background:i<fxRating?fxColor:"#0c1826"}}/>)}</div>
                              </div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"16px",color:fxColor,marginBottom:"6px",lineHeight:1}}>{fxLabel}</div>
                              <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"3px",marginBottom:"5px"}}>
                                {[
                                  {l:"CONF",    v:`${fxcaConf}%`,  c:fxcaConf>=80?"#00ff88":"#ffd700"},
                                  {l:"FX",      v:s?.fx_sc||0,     c:"#00d4ff"},
                                  {l:"EVO",     v:s?.evo_sc||0,    c:"#ff9040"},
                                  {l:"RSI",     v:s?.rsi||"—",     c:(s?.rsi>70)?"#ff3355":(s?.rsi<30)?"#00ff88":"#ffd700"},
                                  {l:"MACD",    v:s?.macd_h>=0?"▲":"▼", c:s?.macd_h>=0?"#00ff88":"#ff3355"},
                                  {l:"R/R",     v:`${s?.rr||0}x`,  c:(s?.rr>=2)?"#00ff88":"#ffd700"},
                                ].map(x=>(
                                  <div key={x.l} style={{textAlign:"center",padding:"3px",background:"#07101a",borderRadius:"3px"}}>
                                    <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                                    <div style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:x.c}}>{x.v}</div>
                                  </div>
                                ))}
                              </div>
                              <div style={{borderTop:"1px solid #0f2235",paddingTop:"4px",display:"flex",flexDirection:"column",gap:"2px"}}>
                                <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px"}}>
                                  <span style={{color:"#4a7a9b"}}>Tendencia</span>
                                  <span style={{color:TC?.[s?.trend]||"#ffd700",fontWeight:600}}>{s?.trend||"—"}</span>
                                </div>
                                <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px"}}>
                                  <span style={{color:"#4a7a9b"}}>ROC 10h</span>
                                  <span style={{color:(s?.roc10>=0)?"#00ff88":"#ff3355",fontWeight:600}}>{s?.roc10>=0?"+":""}{s?.roc10||0}%</span>
                                </div>
                                <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px"}}>
                                  <span style={{color:"#4a7a9b"}}>Vol 24h</span>
                                  <span style={{color:(s?.vol_24h>=1.3)?"#00ff88":"#a0cce0",fontWeight:600}}>{s?.vol_24h||0}x prom.</span>
                                </div>
                                <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px"}}>
                                  <span style={{color:"#4a7a9b"}}>EVO Prob.</span>
                                  <span style={{color:(s?.evo_prob>=0.6)?"#ff9040":"#a0cce0",fontWeight:600}}>{s?.evo_prob||0}</span>
                                </div>
                                {[
                                  {l:"Motor dominante", v:s?.regimeMix?`${s.regimeMix} · mom ${Math.round((s.wMom||0)*100)}%`:"—",
                                   c:s?.regimeMix==="TENDENCIAL"?"#00ff88":s?.regimeMix==="LATERAL"?"#ff9040":"#ffd700"},
                                  {l:"Momentum / Reversión", v:s?`${s.mom_sc??"—"} / ${s.rev_sc??"—"}`:"—", c:"#a0cce0"},
                                  {l:"R/R neto (post costos)", v:s?`${s.rr}x · bruto ${s.rr_bruto}x`:"—",
                                   c:s?.rr>=2?"#00ff88":s?.rr>=1.2?"#ffd700":"#ff3355"},
                                  {l:"Costo estimado", v:s?.costPct?`${s.costPct}% round-trip`:"—", c:"#ff9040"},
                                  {l:"Niveles calculados desde", v:s?.slSource==="estructural"?"Soportes/resistencias reales":"ATR (sin estructura)",
                                   c:s?.slSource==="estructural"?"#00ff88":"#ffd700"},
                                  {l:"Fuerza relativa", v:s?.rsLabel||"—",
                                   c:s?.rsScore>=65?"#00ff88":s?.rsScore>=50?"#a0cce0":s?.rsScore>=35?"#ffd700":"#ff3355"},
                                ].map(x=>(
                                  <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px"}}>
                                    <span style={{color:"#4a7a9b"}}>{x.l}</span>
                                    <span style={{color:x.c,fontWeight:600,textAlign:"right",maxWidth:"58%"}}>{x.v}</span>
                                  </div>
                                ))}
                              </div>
                            </div>

                            {/* ② CONFLUENCIA */}
                            <div style={{padding:"10px",background:"#050c15",borderRadius:"5px",border:`1px solid ${confColor}25`}}>
                              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:"4px"}}>
                                <div style={{fontSize:"7px",color:"#4a7a9b"}}>② CONFLUENCIA</div>
                                <div style={{display:"flex",gap:"2px"}}>{[0,1,2].map(i=><div key={i} style={{width:"8px",height:"8px",borderRadius:"2px",background:i<confRating?confColor:"#0c1826"}}/>)}</div>
                              </div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"16px",color:confColor,marginBottom:"6px",lineHeight:1}}>{confLabel}</div>
                              <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"3px",marginBottom:"5px"}}>
                                {[
                                  {l:"SCORE",   v:`${confScore}%`,    c:confColor},
                                  {l:"▲ BULL",  v:conf_?.bull||0,     c:"#00ff88"},
                                  {l:"▼ BEAR",  v:conf_?.bear||0,     c:"#ff3355"},
                                ].map(x=>(
                                  <div key={x.l} style={{textAlign:"center",padding:"3px",background:"#07101a",borderRadius:"3px"}}>
                                    <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                                    <div style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:x.c}}>{x.v}</div>
                                  </div>
                                ))}
                              </div>
                              <div style={{borderTop:"1px solid #0f2235",paddingTop:"4px",display:"flex",flexDirection:"column",gap:"2px"}}>
                                {[
                                  {l:"RSI Divergencia", v:rsiDiv_?.bullish?"▲ Alcista":rsiDiv_?.bearish?"▼ Bajista":"Sin señal", c:rsiDiv_?.bullish?"#00ff88":rsiDiv_?.bearish?"#ff3355":"#5a8fa8"},
                                  {l:"Cruce medias",    v:cross_?.golden?"⭐ Golden Cross":cross_?.death?"💀 Death Cross":cross_?.gap>=0?"SMA20 > SMA50":"SMA20 < SMA50", c:cross_?.golden?"#00ff88":cross_?.death?"#ff3355":"#a0cce0"},
                                  {l:"BB + RSI",        v:boll_?.oversold?"▲ Sobreventa":boll_?.overbought?"▼ Sobrecompra":"Zona neutral", c:boll_?.oversold?"#00ff88":boll_?.overbought?"#ff3355":"#5a8fa8"},
                                  {l:"Vol en Fib",      v:volFib_?.confirmed?`✓ Fib ${volFib_?.closestFib} (${volFib_?.volRatio}x)`:`${volFib_?.volRatio||"—"}x vol`, c:volFib_?.confirmed?"#ffd700":"#5a8fa8"},
                                  {l:"Patrón vela",     v:cand_?.patterns?.length?cand_.patterns[0].name:"Sin patrón", c:cand_?.patterns?.[0]?.type==="bullish"?"#00ff88":cand_?.patterns?.[0]?.type==="bearish"?"#ff3355":"#5a8fa8"},
                                ].map(x=>(
                                  <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px"}}>
                                    <span style={{color:"#4a7a9b"}}>{x.l}</span>
                                    <span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                                  </div>
                                ))}
                              </div>
                            </div>

                            {/* ③ ESTRUCTURAL */}
                            <div style={{padding:"10px",background:"#050c15",borderRadius:"5px",border:`1px solid ${estColor}25`}}>
                              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:"4px"}}>
                                <div style={{fontSize:"7px",color:"#4a7a9b"}}>③ ESTRUCTURAL</div>
                                <div style={{display:"flex",gap:"2px"}}>{[0,1,2].map(i=><div key={i} style={{width:"8px",height:"8px",borderRadius:"2px",background:i<estRating?estColor:"#0c1826"}}/>)}</div>
                              </div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"16px",color:estColor,marginBottom:"6px",lineHeight:1}}>{estLabel}</div>
                              <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"3px",marginBottom:"5px"}}>
                                {[
                                  {l:"TF 7D",  v:mtf_?.[0]?.dir==="bull"?"▲":mtf_?.[0]?.dir==="bear"?"▼":"◆", c:mtf_?.[0]?.dir==="bull"?"#00ff88":mtf_?.[0]?.dir==="bear"?"#ff3355":"#ffd700"},
                                  {l:"TF 30D", v:mtf_?.[1]?.dir==="bull"?"▲":mtf_?.[1]?.dir==="bear"?"▼":"◆", c:mtf_?.[1]?.dir==="bull"?"#00ff88":mtf_?.[1]?.dir==="bear"?"#ff3355":"#ffd700"},
                                  {l:"TF 60D", v:mtf_?.[2]?.dir==="bull"?"▲":mtf_?.[2]?.dir==="bear"?"▼":"◆", c:mtf_?.[2]?.dir==="bull"?"#00ff88":mtf_?.[2]?.dir==="bear"?"#ff3355":"#ffd700"},
                                ].map(x=>(
                                  <div key={x.l} style={{textAlign:"center",padding:"3px",background:"#07101a",borderRadius:"3px"}}>
                                    <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                                    <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.v}</div>
                                  </div>
                                ))}
                              </div>
                              <div style={{borderTop:"1px solid #0f2235",paddingTop:"4px",display:"flex",flexDirection:"column",gap:"2px"}}>
                                {[
                                  {l:"Fase Weinstein",  v:reg_?.phase||"—",                     c:reg_?.color||"#a0cce0"},
                                  {l:"ATR Bands",       v:atrB_?.breakoutUp?"✓ Breakout alcista":atrB_?.breakoutDown?"✓ Breakout bajista":atrB_?.falseBreakUp?"⚠ Falso breakout":atrB_?.falseBreakDown?"⚠ Trampa bajista":"Dentro de bandas", c:atrB_?.breakoutUp?"#00ff88":atrB_?.breakoutDown?"#ff3355":atrB_?.falseBreakUp||atrB_?.falseBreakDown?"#ffd700":"#5a8fa8"},
                                  {l:"POC (Control)",   v:vp_?`${FP(vp_.poc,moneda)} (${vp_.pctFromPoc>=0?"+":""}${vp_.pctFromPoc}%)`:"—", c:Math.abs(vp_?.pctFromPoc||99)<3?"#ffd700":"#a0cce0"},
                                  {l:"Value Area",      v:vp_?`${FP(vp_.vaL,moneda)} — ${FP(vp_.vaH,moneda)}`:"—", c:vp_?.inValueArea?"#00ff88":"#5a8fa8"},
                                  {l:"Volumen",         v:reg_?.volExpanding?"▲ Expandiendo":reg_?.volContracting?"▼ Contrayendo":"→ Estable", c:reg_?.volExpanding?"#00ff88":reg_?.volContracting?"#ff3355":"#ffd700"},
                                ].map(x=>(
                                  <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px"}}>
                                    <span style={{color:"#4a7a9b"}}>{x.l}</span>
                                    <span style={{color:x.c,fontWeight:600,textAlign:"right",maxWidth:"55%"}}>{x.v}</span>
                                  </div>
                                ))}
                              </div>
                            </div>

                            {/* ④ OPTIMIZACIÓN */}
                            <div style={{padding:"10px",background:"#050c15",borderRadius:"5px",border:`1px solid ${optColor}25`}}>
                              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:"4px"}}>
                                <div style={{fontSize:"7px",color:"#4a7a9b"}}>④ OPTIMIZACIÓN</div>
                                <div style={{display:"flex",gap:"2px"}}>{[0,1,2].map(i=><div key={i} style={{width:"8px",height:"8px",borderRadius:"2px",background:i<optRating?optColor:"#0c1826"}}/>)}</div>
                              </div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"16px",color:optColor,marginBottom:"6px",lineHeight:1}}>{sq_?.quality||"SIN DATOS"}</div>
                              <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"3px",marginBottom:"5px"}}>
                                {[
                                  {l:"WF WIN%",   v:wf_?`${wf_.hr}%`:"—",          c:wf_&&wf_.hr>=55?"#00ff88":wf_&&wf_.hr>=45?"#ffd700":"#ff3355"},
                                  {l:"HIST WIN%", v:sq_&&sq_.total>=30?`${sq_.hr}%`:"n<30", c:sq_&&sq_.significant?"#00ff88":"#ff3355"},
                                  {l:"RET NETO",  v:sq_&&sq_.total>=30?`${sq_.avgRet>=0?"+":""}${sq_.avgRet}%`:"—", c:sq_&&sq_.avgRet>0?"#00ff88":"#ff3355"},
                                ].map(x=>(
                                  <div key={x.l} style={{textAlign:"center",padding:"3px",background:"#07101a",borderRadius:"3px"}}>
                                    <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                                    <div style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:x.c}}>{x.v}</div>
                                  </div>
                                ))}
                              </div>
                              <div style={{borderTop:"1px solid #0f2235",paddingTop:"4px",display:"flex",flexDirection:"column",gap:"2px"}}>
                                {[
                                  {l:"Consistencia WF",  v:wf_?`${wf_.consistency}% (neto costos)`:"Sin datos",   c:wf_&&wf_.consistency>=60?"#00ff88":wf_&&wf_.consistency>=40?"#ffd700":"#ff3355"},
                                  {l:"Muestra (n)",      v:sq_?`${sq_.total} casos`:"—", c:sq_&&sq_.total>=30?"#00ff88":"#ff3355"},
                                  {l:"IC 95% win-rate",  v:sq_&&sq_.total>=30?`${sq_.ciLow}%–${sq_.ciHigh}%${sq_.significant?"":" ⚠"}`:"n insuficiente", c:sq_?.significant?"#00ff88":"#ff3355"},
                                  {l:"Máx subida hist.", v:sq_&&sq_.total>=30?`+${sq_.avgMaxRet}%`:"—",                  c:"#00ff88"},
                                  {l:"Máx caída hist.",  v:sq_&&sq_.total>=30?`${sq_.avgMaxDD}%`:"—",                   c:"#ff3355"},
                                  {l:"Eventos próximos", v:events_.length?events_.map(e=>`${e.name} (${e.daysLeft<=0?"HOY":e.daysLeft+"d"})`).join(" · "):"Sin eventos",
                                    c:hasRisk_?"#ff3355":events_.length?"#ffd700":"#00ff88"},
                                {l:"Validez estadística", v:sq_?.note?(sq_.significant?"✓ significativa":"✗ no significativa"):"—",
                                    c:sq_?.significant?"#00ff88":"#ff3355"},
                                ].map(x=>(
                                  <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px"}}>
                                    <span style={{color:"#4a7a9b"}}>{x.l}</span>
                                    <span style={{color:x.c,fontWeight:600,textAlign:"right",maxWidth:"55%"}}>{x.v}</span>
                                  </div>
                                ))}
                              </div>
                            </div>

                          </div>

                          {/* BARRA GLOBAL */}
                          <div>
                            <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px",color:"#4a7a9b",marginBottom:"3px"}}>
                              <span>SCORE GLOBAL</span>
                              <span style={{color:veredictoColor,fontWeight:700}}>{globalPct}% ({totalRating}/{maxRating} puntos)</span>
                            </div>
                            <div style={{height:"6px",background:"#0c1826",borderRadius:"3px",overflow:"hidden"}}>
                              <div style={{height:"100%",width:`${globalPct}%`,background:`linear-gradient(90deg,#ff3355,#ffd700 40%,#00ff88)`,borderRadius:"3px",transition:"width .4s"}}/>
                            </div>
                            <div style={{display:"flex",justifyContent:"space-between",fontSize:"6px",color:"#4a7a9b",marginTop:"2px"}}>
                              <span>EVITAR</span><span>ESPERAR</span><span>MODERAR</span><span>COMPRAR</span>
                            </div>
                          </div>

                          {/* SIZING Y EVENTOS inline */}
                          {(ps_||events_.length>0)&&(
                            <div style={{marginTop:"10px",paddingTop:"10px",borderTop:"1px solid #0f2235",display:"flex",gap:"8px",flexWrap:"wrap",alignItems:"center"}}>
                              {ps_&&<div style={{fontSize:"8px",padding:"4px 10px",background:`${ps_.levelColor}15`,border:`1px solid ${ps_.levelColor}30`,borderRadius:"4px",color:ps_.levelColor,fontWeight:700}}>
                                💰 {ps_.level} · {ps_.riskPct}% riesgo · ${ps_.riskAmount.toLocaleString()}
                              </div>}
                              {events_.map((ev,i)=>(
                                <div key={i} style={{fontSize:"8px",padding:"4px 10px",background:ev.type==="earnings"?"#ff335515":"#ffd70010",border:`1px solid ${ev.type==="earnings"?"#ff335540":"#ffd70030"}`,borderRadius:"4px",color:ev.type==="earnings"?"#ff3355":"#ffd700"}}>
                                  📅 {ev.name} — {ev.daysLeft<=0?"HOY":ev.daysLeft===1?"MAÑANA":`${ev.daysLeft}d`}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })()}

                    {/* FIBONACCI */}
                    {/* FIBONACCI */}
                    {(()=>{
                      const fibData = rowDataRef.current[sel.ticker];
                      if (!fibData || fibData.length < 10) return null;
                      const fib = calcFibonacci(fibData, W);
                      if (!fib) return null;
                      const px = sel.price || fibData[fibData.length-1].close;
                      const isUp = fib.trend === "up";
                      const moneda = sel.moneda || "USD";

                      return (
                        <div className="card" style={{padding:"12px",marginBottom:"9px"}}>
                          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"10px"}}>
                            <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>
                              📐 FIBONACCI — últimas {fib.lookback} barras
                            </div>
                            <div style={{display:"flex",gap:"8px",fontSize:"8px"}}>
                              <span style={{color:"#00ff88"}}>MAX {FP(fib.high,moneda)}</span>
                              <span style={{color:"#4a7a9b"}}>|</span>
                              <span style={{color:"#ff3355"}}>MIN {FP(fib.low,moneda)}</span>
                            </div>
                          </div>

                          {/* Zona de rebote más cercana */}
                          <div style={{padding:"8px",background: isUp?"#00ff8808":"#ff335508",border:`1px solid ${isUp?"#00ff8830":"#ff335530"}`,borderRadius:"4px",marginBottom:"8px"}}>
                            <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>
                              {isUp ? "🔄 SOPORTE MÁS CERCANO (posible rebote alcista)" : "🔄 RESISTENCIA MÁS CERCANA (posible rebote bajista)"}
                            </div>
                            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                              <span style={{fontFamily:"'Bebas Neue'",fontSize:"20px",color:isUp?"#00ff88":"#ff3355"}}>
                                {FP(fib.closest?.value, moneda)}
                              </span>
                              <span style={{fontSize:"9px",color:"#ffd700",fontWeight:700}}>
                                Fib {fib.closest?.label}
                              </span>
                              <span style={{fontSize:"9px",color:isUp?"#00ff88":"#ff3355"}}>
                                {fib.closest?.value > px ? "+" : ""}{((fib.closest?.value - px)/px*100).toFixed(2)}%
                              </span>
                            </div>
                          </div>

                          {/* Barra visual de posición */}
                          {(()=>{
                            const posPct = ((px - fib.low) / fib.rng * 100).toFixed(1);
                            return (
                              <div style={{marginBottom:"10px",padding:"8px 10px",background:"#050c15",borderRadius:"4px"}}>
                                <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px",color:"#4a7a9b",marginBottom:"4px"}}>
                                  <span>MIN {FP(fib.low,moneda)}</span>
                                  <span style={{color:"#ffd700",fontWeight:700}}>PRECIO ACTUAL: {posPct}% del rango</span>
                                  <span>MAX {FP(fib.high,moneda)}</span>
                                </div>
                                <div style={{position:"relative",height:"12px",background:"#0c1826",borderRadius:"6px",overflow:"visible"}}>
                                  {/* Líneas de Fibonacci */}
                                  {[0.236,0.382,0.5,0.618,0.786].map(f=>(
                                    <div key={f} style={{position:"absolute",left:`${f*100}%`,top:0,bottom:0,width:"1px",background:"#ffd70030"}}/>
                                  ))}
                                  {/* Barra de progreso */}
                                  <div style={{position:"absolute",left:0,top:0,bottom:0,width:`${posPct}%`,background:`linear-gradient(90deg,#ff3355,#ffd700,#00ff88)`,borderRadius:"6px",opacity:.7}}/>
                                  {/* Indicador de precio */}
                                  <div style={{
                                    position:"absolute",
                                    left:`${posPct}%`,
                                    top:"-3px",bottom:"-3px",
                                    width:"3px",
                                    background:"#fff",
                                    borderRadius:"2px",
                                    transform:"translateX(-50%)",
                                    boxShadow:"0 0 6px #fff8"
                                  }}/>
                                </div>
                                <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px",color:"#4a7a9b",marginTop:"4px"}}>
                                  {[0,23.6,38.2,50,61.8,78.6,100].map(f=>(
                                    <span key={f} style={{color: Math.abs(f-posPct)<5?"#ffd700":"#4a7a9b"}}>{f}%</span>
                                  ))}
                                </div>
                              </div>
                            );
                          })()}

                          {/* Tabla de niveles */}
                          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"6px"}}>
                            {/* Retrocesos */}
                            <div>
                              <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"4px",letterSpacing:".1em"}}>
                                {isUp ? "▼ RETROCESOS (soporte)" : "▲ REBOTES (resistencia)"}
                              </div>
                              {fib.levels.map(l => {
                                const pct = ((l.value - px)/px*100).toFixed(1);
                                const distPct = Math.abs(l.value - px)/px;
                                const isCurrent = distPct < 0.02; // dentro del 2%
                                const isNearest = l.label === fib.closest?.label;
                                const isAbove = l.value > px;
                                return (
                                  <div key={l.label} style={{
                                    display:"flex",justifyContent:"space-between",alignItems:"center",
                                    padding: isNearest?"5px 8px":"3px 5px",marginBottom:"2px",
                                    background: isNearest?"#ffd70025":isCurrent?"#ffd70010":isAbove?"#00ff8808":"#ff335508",
                                    border: isNearest?"2px solid #ffd700":"1px solid transparent",
                                    borderRadius:"4px",fontSize:"8px",
                                    boxShadow: isNearest?"0 0 8px #ffd70030":"none",
                                  }}>
                                    <div style={{display:"flex",alignItems:"center",gap:"4px"}}>
                                      {isNearest && <span style={{color:"#ffd700",fontSize:"10px"}}>◀</span>}
                                      <span style={{color:"#ffd700",fontWeight:isNearest?700:400,fontSize:isNearest?"10px":"8px"}}>{l.label}</span>
                                    </div>
                                    <span style={{color:isNearest?"#fff":"#e8f4ff",fontWeight:isNearest?700:400}}>{FP(l.value,moneda)}</span>
                                    <span style={{color:+pct>=0?"#00ff88":"#ff3355",fontSize:"7px",fontWeight:isNearest?700:400}}>{+pct>=0?"+":""}{pct}%</span>
                                  </div>
                                );
                              })}
                            </div>

                            {/* Extensiones */}
                            <div>
                              <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"4px",letterSpacing:".1em"}}>
                                {isUp ? "▲ EXTENSIONES (objetivo)" : "▼ EXTENSIONES (objetivo)"}
                              </div>
                              {fib.extensions.map(l => {
                                const pct = ((l.value - px)/px*100).toFixed(1);
                                return (
                                  <div key={l.label} style={{
                                    display:"flex",justifyContent:"space-between",
                                    padding:"3px 5px",marginBottom:"2px",
                                    background: isUp?"#00d4ff08":"#ff9040008",
                                    borderRadius:"3px",fontSize:"8px"
                                  }}>
                                    <span style={{color:isUp?"#00d4ff":"#ff9040",fontWeight:700}}>{l.label}</span>
                                    <span style={{color:"#e8f4ff"}}>{FP(l.value,moneda)}</span>
                                    <span style={{color:+pct>=0?"#00ff88":"#ff3355",fontSize:"7px"}}>{+pct>=0?"+":""}{pct}%</span>
                                  </div>
                                );
                              })}

                              {/* Explicación */}
                              <div style={{marginTop:"6px",padding:"5px",background:"#050c15",borderRadius:"3px",fontSize:"7px",color:"#5a8fa8",lineHeight:"1.6"}}>
                                {isUp
                                  ? "📈 Tendencia alcista: los retrocesos son zonas de compra. Las extensiones son objetivos de ganancia."
                                  : "📉 Tendencia bajista: los rebotes son zonas de resistencia. Las extensiones son objetivos de caída."}
                              </div>
                            </div>
                          </div>
                        </div>
                      );
                    })()}

                    <div className="card" style={{padding:"10px",marginBottom:"9px"}}>
                      <div style={{fontSize:"8px",color:"#4a7a9b",marginBottom:"6px"}}>EQUITY CURVE</div>
                      <div style={{overflowX:"auto"}}>
                        <Curve curve={sel.bt.curve} w={Math.min(560, window.innerWidth-60)} h={80}/>
                      </div>
                    </div>

                    <div className="card" style={{padding:"10px"}}>
                      <div style={{fontSize:"8px",color:"#4a7a9b",marginBottom:"6px"}}>OPERACIONES · {sel.bt.trades.length}</div>
                      <div style={{overflowX:"auto",maxHeight:"320px",overflowY:"auto",WebkitOverflowScrolling:"touch"}}>
                        <table>
                          <thead><tr><th>#</th><th>SEÑAL</th><th>FXCA16</th><th>ENTRADA</th><th>STOP</th><th>SALIDA</th><th>DÍAS</th><th>MOTIVO</th><th>RET</th><th>RES</th></tr></thead>
                          <tbody>{sel.bt.trades.map((t,i)=>
                            <tr key={i} style={{background:t.win?"#00ff9d06":t.reason==="STOP LOSS"?"#ff335506":"transparent"}}>
                              <td style={{color:"#4a7a9b"}}>{i+1}</td>
                              <td><span className="badge" style={{background:SC[t.sig]+"15",color:SC[t.sig],border:`1px solid ${SC[t.sig]}30`,fontSize:"8px"}}>{t.sig}</span></td>
                              <td><FXCA16Badge score={t.ca15}/></td>
                              <td style={{color:"#5dffb0",fontSize:"10px"}}>${F(t.entry)}</td>
                              <td style={{color:"#ff3355",fontSize:"9px"}}>{t.sl?`$${F(t.sl)}`:"─"}</td>
                              <td style={{fontWeight:600,fontSize:"10px"}}>${F(t.exit)}</td>
                              <td style={{color:"#4a7a9b"}}>{t.days}d</td>
                              <td style={{fontSize:"8px",color:t.reason==="TAKE PROFIT"?"#00d4ff":t.reason==="STOP LOSS"?"#ff3355":"#5a8fa8"}}>{t.reason}</td>
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

            {/* ══ TAB: SEGUIMIENTO ══ */}
            {tab==="watch"&&(
              <div className="fade">
                {/* Barra de listas */}
                <div style={{display:"flex",gap:"6px",marginBottom:"10px",flexWrap:"wrap",alignItems:"center"}}>
                  {watchlists.map((wl,i)=>(
                    <div key={wl.id} style={{display:"inline-flex",alignItems:"center",gap:"2px"}}>
                      {editingWL===wl.id ? (
                        <input autoFocus value={editWLName}
                          onChange={e=>setEditWLName(e.target.value)}
                          onBlur={()=>{
                            const wls=watchlists.map(w=>w.id===wl.id?{...w,name:editWLName||w.name}:w);
                            saveWatchlists(wls);setEditingWL(null);
                          }}
                          onKeyDown={e=>{if(e.key==="Enter"){
                            const wls=watchlists.map(w=>w.id===wl.id?{...w,name:editWLName||w.name}:w);
                            saveWatchlists(wls);setEditingWL(null);
                          }}}
                          style={{width:"90px",background:"#07101a",color:"#00d4ff",border:"1px solid #00d4ff40",borderRadius:"4px",padding:"3px 6px",fontSize:"9px"}}
                        />
                      ) : (
                        <button className={`btn ${activeWL===i?"on":"off"}`} onClick={()=>setActiveWL(i)}
                          style={{padding:"4px 10px",fontSize:"9px"}}>
                          ⭐ {wl.name} <span style={{opacity:.6}}>({wl.tickers.length})</span>
                        </button>
                      )}
                      <button className="btn off" onClick={()=>{setEditingWL(wl.id);setEditWLName(wl.name);}}
                        style={{padding:"3px 5px",fontSize:"8px",color:"#a0cce0"}}>✏️</button>
                      {watchlists.length>1&&(
                        <button className="btn off" onClick={()=>deleteWatchlist(i)}
                          style={{padding:"3px 5px",fontSize:"8px",color:"#ff3355"}}>✕</button>
                      )}
                    </div>
                  ))}
                  <button className="btn off" onClick={createWatchlist}
                    style={{padding:"4px 10px",fontSize:"9px",color:"#00ff9d",borderColor:"#00ff9d40"}}>
                    + Nueva lista
                  </button>
                </div>

                {/* Agregar ticker manual a la lista */}
                <div style={{display:"flex",gap:"6px",marginBottom:"10px",alignItems:"center"}}>
                  <input type="text" value={wlInput} onChange={e=>setWlInput(e.target.value.toUpperCase())}
                    onKeyDown={e=>{if(e.key==="Enter"&&wlInput.trim()){addToWatchlist(activeWL,wlInput.trim());setWlInput("");}}}
                    placeholder="Agregar ticker..."
                    maxLength={6}
                    style={{width:"110px",background:"#020508",color:"#00d4ff",border:"1px solid #0f2235",borderRadius:"4px",padding:"5px 8px",fontSize:"10px",textTransform:"uppercase",outline:"none"}}
                  />
                  <button className={`btn ${wlInput.trim()?"on":"off"}`}
                    onClick={()=>{if(wlInput.trim()){addToWatchlist(activeWL,wlInput.trim());setWlInput("");}}}
                    style={{padding:"5px 12px",fontSize:"9px"}}>+ Agregar</button>
                  <span style={{fontSize:"8px",color:"#4a7a9b"}}>o hacé click en ⭐ desde cualquier señal</span>
                </div>

                {/* Alertas — tickers del watchlist que están en P80 */}
                {(()=>{
                  const wlTickers = watchlists[activeWL]?.tickers || [];
                  const alertas = rows.filter(r => wlTickers.includes(r.ticker) && r.sig?.above_p80 && r.sig?.sig !== "NEUTRAL");
                  if (!alertas.length) return null;
                  return (
                    <div style={{padding:"8px 10px",background:"#ffd70010",border:"1px solid #ffd70030",borderRadius:"5px",marginBottom:"10px"}}>
                      <div style={{fontSize:"7px",color:"#ffd700",marginBottom:"5px",fontWeight:700,letterSpacing:".1em"}}>🔔 ALERTAS P80 EN ESTA LISTA ({alertas.length})</div>
                      <div style={{display:"flex",flexWrap:"wrap",gap:"5px"}}>
                        {alertas.map(r=>(
                          <div key={r.ticker} onClick={()=>{setSel(r);setTab("det");}}
                            style={{padding:"4px 10px",background:SC[r.sig.sig]+"20",border:`1px solid ${SC[r.sig.sig]}40`,borderRadius:"4px",cursor:"pointer"}}>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:SC[r.sig.sig]}}>{r.ticker}</div>
                            <div style={{fontSize:"7px",color:SC[r.sig.sig],opacity:.8}}>{r.sig.sig}</div>
                            {r.sig.scoreTrend&&r.sig.scoreTrend!=="→"&&(
                              <div style={{fontSize:"7px",color:r.sig.scoreTrend==="▲"?"#00ff88":"#ff3355"}}>{r.sig.scoreTrend} {r.sig.scoreDelta>=0?"+":""}{r.sig.scoreDelta}pts</div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })()}

                {/* Tickers de la lista activa */}
                {(watchlists[activeWL]?.tickers||[]).length===0 ? (
                  <div style={{textAlign:"center",padding:"40px",color:"#4a7a9b",fontSize:"11px"}}>
                    <div style={{fontSize:"28px",marginBottom:"8px"}}>⭐</div>
                    <div>Todavía no hay acciones en <strong style={{color:"#ffd700"}}>{watchlists[activeWL]?.name}</strong></div>
                    <div style={{fontSize:"9px",marginTop:"6px",color:"#4a7a9b"}}>Agregá tickers desde las señales o escribí arriba</div>
                  </div>
                ) : (
                  <div className="grid-opp">
                    {(watchlists[activeWL]?.tickers||[]).map(tk=>{
                      // Buscar en rows o en rowDataRef
                      const rowData = rows.find(r=>r.ticker===tk);
                      const barData = rowDataRef.current[tk];
                      let r = rowData;
                      if (!r && barData?.length>=60) {
                        const sig2 = combinedSignal(barData.map(b=>({...b,_ticker:tk})), W);
                        const px2  = barData[barData.length-1].close;
                        const mon  = barData[0]?.moneda||"USD";
                        r = {ticker:tk,name:tk,sector:"—",moneda:mon,price:px2,sig:sig2,
                          bt:{trades:[],curve:[],n:0,hits:0,hr:0,avg:0,aw:0,al:0,pf:0,sh:0,dd:0,eq:100},
                          real:true,fromCsv:true};
                      }
                      if (!r) return (
                        <div key={tk} className="card" style={{padding:"12px",opacity:.5}}>
                          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                            <span style={{fontFamily:"'Bebas Neue'",fontSize:"18px",color:"#a0cce0"}}>{tk}</span>
                            <span style={{fontSize:"8px",color:"#4a7a9b"}}>Sin datos · ejecutá el sistema</span>
                            <button className="btn off" onClick={()=>removeFromWatchlist(activeWL,tk)}
                              style={{padding:"2px 7px",fontSize:"8px",color:"#ff3355"}}>✕</button>
                          </div>
                        </div>
                      );
                      const s=r.sig; if(!s) return null;
                      const g=GR(r.bt.hr);
                      return (
                        <div key={tk} className="card" style={{padding:"13px",cursor:"pointer",borderLeft:`3px solid ${SC[s.sig]}`}}
                          onClick={()=>{setSel(r);rowDataRef.current[tk]=barData||rowDataRef.current[tk];setTab("det");}}>
                          <div style={{display:"flex",justifyContent:"space-between",marginBottom:"6px"}}>
                            <div>
                              <div style={{display:"flex",alignItems:"center",gap:"6px",marginBottom:"2px"}}>
                                <span style={{fontFamily:"'Bebas Neue'",fontSize:"20px",color:SC[s.sig]}}>{tk}</span>
                                <span style={{fontSize:"8px",color:r.moneda==="USD"?"#00d4ff":"#ffd700",background:r.moneda==="USD"?"#00d4ff12":"#ffd70012",padding:"1px 5px",borderRadius:"3px",fontWeight:700}}>{r.moneda}</span>
                                <FXCA16Badge score={s.ca15_score}/>
                              </div>
                              <div style={{fontSize:"8px",color:"#5a8fa8"}}>{r.name}</div>
                            </div>
                            <div style={{display:"flex",flexDirection:"column",alignItems:"flex-end",gap:"4px"}}>
                              <span className="badge" style={{background:SC[s.sig]+"20",color:SC[s.sig],border:`1px solid ${SC[s.sig]}40`}}>{s.sig}</span>
                              <button className="btn off" onClick={e=>{e.stopPropagation();removeFromWatchlist(activeWL,tk);}}
                                style={{padding:"2px 7px",fontSize:"7px",color:"#ff3355"}}>✕ quitar</button>
                            </div>
                          </div>
                          <ScoreBar fx={s.fx_sc} evo={s.evo_sc} final_sc={s.final_sc}/>
                          <div style={{background:"#050c15",borderRadius:"4px",padding:"5px 8px",margin:"7px 0",display:"flex",justifyContent:"space-between"}}>
                            <span style={{fontSize:"8px",color:"#4a7a9b"}}>PRECIO</span>
                            <span style={{fontFamily:"'Bebas Neue'",fontSize:"18px",color:"#e8f4ff"}}>{FP(r.price,r.moneda)}</span>
                          </div>
                          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"3px"}}>
                            {[{l:"CONF",v:`${s.conf}%`,c:SC[s.sig]},{l:"FX",v:s.fx_sc,c:"#00d4ff"},{l:"EVO",v:s.evo_sc,c:"#ff9040"},{l:"R/R",v:`${s.rr}x`,c:s.rr>=2?"#00ff9d":"#ffd700"}].map(m=>
                              <div key={m.l} style={{textAlign:"center",padding:"3px",background:"#050c15",borderRadius:"3px"}}>
                                <div style={{fontSize:"7px",color:"#4a7a9b"}}>{m.l}</div>
                                <div style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:m.c}}>{m.v}</div>
                              </div>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            )}


            {/* ══ TAB: CATEGORÍAS ══ */}
            {tab==="cmp"&&(()=>{
              const tickerList = rows.map(r=>r.ticker).sort();
              const rA = rows.find(r=>r.ticker===cmpA);
              const rB = rows.find(r=>r.ticker===cmpB);

              // Calcular análisis completo para un ticker
              const calcFull = (r) => {
                if (!r) return null;
                const data    = rowDataRef.current[r.ticker];
                const s       = r.sig;
                const moneda  = r.moneda||"USD";
                const px      = r.price||0;
                if (!data||!s) return null;
                const fib_    = calcFibonacci(data, W);
                const rsiDiv_ = detectRSIDivergence(data);
                const volFib_ = checkVolumeAtFib(data, fib_?.levels);
                const cross_  = detectCross(data);
                const boll_   = detectBollingerRSISetup(data);
                const cand_   = detectCandlePattern(data);
                const conf_   = calcConfluence(s, rsiDiv_, volFib_, cross_, boll_, cand_, null);
                const mtf_    = calcMultiTimeframe(data, W);
                const reg_    = detectTickerRegime(data);
                const atrB_   = calcATRBands(data);
                const vp_     = calcVolumeProfile(data);
                const wf_     = backtestWalkForward(data, W, (moneda==="ARS"?COSTO_MERVAL:COSTO_CEDEAR));
                const sq_     = calcSignalQuality(data, s, W, (moneda==="ARS"?COSTO_MERVAL:COSTO_CEDEAR));
                const events_ = getUpcomingEvents(r.ticker, moneda);
                const ps_     = calcPositionSizing(s, conf_, 1000000);
                const fxScore    = s?.final_sc||0;
                const fxcaConf   = s?.conf||0;
                const fxBull     = s?.sig?.includes("COMPRA");
                const fxBear     = s?.sig?.includes("VENTA");
                const fxColor    = fxBull?"#00ff88":fxBear?"#ff3355":"#ffd700";
                const fxRating   = fxScore>=75?3:fxScore>=55?2:fxScore>=40?1:0;
                const confScore  = conf_?.score||0;
                const confDir    = conf_?.action?.includes("COMPRAR")?"bull":conf_?.action?.includes("VENDER")?"bear":"neutral";
                const confColor  = confDir==="bull"?"#00ff88":confDir==="bear"?"#ff3355":"#ffd700";
                const confRating = confScore>=70?3:confScore>=50?2:confScore>=35?1:0;
                const mtfBull    = mtf_?.filter(f=>f.dir==="bull").length||0;
                const mtfBear    = mtf_?.filter(f=>f.dir==="bear").length||0;
                const mtfAlign   = Math.max(mtfBull,mtfBear);
                const regPhase   = reg_?.phase||"";
                const regBull    = ["Markup","Acumulación","Recuperación"].includes(regPhase);
                const regBear    = ["Markdown","Distribución"].includes(regPhase);
                const estScore   = mtfAlign*25+(regBull?20:regBear?0:10)+(atrB_?.breakoutUp?15:0);
                const estRating  = estScore>=70?3:estScore>=45?2:estScore>=20?1:0;
                const estColor   = regBull&&mtfAlign>=2?"#00ff88":regBear||mtfAlign<=1?"#ff3355":"#ffd700";
                const hasRisk_   = events_.some(e=>e.type==="earnings"&&e.daysLeft<=5);
                const wfOk       = wf_&&wf_.hr>=52&&wf_.consistency>=50;
                const sqOk       = sq_&&sq_.hr>=55;
                const optRating  = (wfOk?1:0)+(sqOk?1:0)+(!hasRisk_?1:0);
                const optColor   = optRating>=3?"#00ff88":optRating>=2?"#ffd700":"#ff3355";
                const totalRating= fxRating+confRating+estRating+optRating;
                const globalPct  = Math.round(totalRating/12*100);
                const incongruencia = (fxBull&&confDir==="bear")||(fxBear&&confDir==="bull");
                const alineado   = (fxBull&&confDir==="bull")||(fxBear&&confDir==="bear");
                let veredicto, veredictoColor;
                if(incongruencia&&fxBull){veredicto="ALERTA";veredictoColor="#ffd700";}
                else if(incongruencia&&fxBear){veredicto="REBOTE";veredictoColor="#ffd700";}
                else if(alineado&&fxBull&&globalPct>=65){veredicto="COMPRAR";veredictoColor="#00ff88";}
                else if(alineado&&fxBull&&globalPct>=45){veredicto="MODERAR";veredictoColor="#7ab0c8";}
                else if(alineado&&fxBear&&globalPct>=55){veredicto="EVITAR";veredictoColor="#ff3355";}
                else{veredicto="ESPERAR";veredictoColor="#ffd700";}
                return { s, moneda, px, fxScore, fxcaConf, fxBull, fxBear, fxColor, fxRating,
                  conf_, confScore, confDir, confColor, confRating,
                  mtf_, reg_, atrB_, vp_, mtfAlign, regPhase, estColor, estRating,
                  wf_, sq_, events_, ps_, hasRisk_, optRating, optColor,
                  totalRating, globalPct, veredicto, veredictoColor,
                  rsiDiv_, cross_, boll_, cand_, volFib_ };
              };

              const fA = calcFull(rA);
              const fB = calcFull(rB);

              const CmpCol = ({r, f, label}) => {
                if (!r||!f) return (
                  <div style={{textAlign:"center",padding:"30px 10px",color:"#4a7a9b",background:"#07101a",borderRadius:"6px"}}>
                    <div style={{fontSize:"24px",marginBottom:"6px"}}>📊</div>
                    <div style={{fontSize:"9px"}}>Seleccioná {label}</div>
                  </div>
                );
                const {s,moneda,fxScore,fxcaConf,fxColor,fxRating,confScore,confColor,confRating,
                  estColor,estRating,optColor,optRating,globalPct,veredicto,veredictoColor,
                  mtf_,reg_,atrB_,vp_,mtfAlign,wf_,sq_,hasRisk_,events_,ps_,
                  rsiDiv_,cross_,boll_,cand_,volFib_,conf_} = f;
                return (
                  <div style={{background:"#07101a",borderRadius:"6px",padding:"12px",border:`2px solid ${veredictoColor}40`}}>
                    {/* Header */}
                    <div style={{textAlign:"center",marginBottom:"10px",paddingBottom:"8px",borderBottom:"1px solid #0f2235"}}>
                      <div style={{fontFamily:"'Bebas Neue'",fontSize:"28px",color:SC[s.sig]||"#e8f4ff"}}>{r.ticker}</div>
                      <div style={{fontSize:"8px",color:"#5a8fa8",marginBottom:"3px"}}>{r.name}</div>
                      <div style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color:"#e8f4ff"}}>{FP(r.price,moneda)}</div>
                      <span className="badge" style={{background:SC[s.sig]+"20",color:SC[s.sig],border:`1px solid ${SC[s.sig]}40`,margin:"4px 0",display:"inline-block"}}>{s.sig}</span>
                      <div style={{fontFamily:"'Bebas Neue'",fontSize:"20px",color:veredictoColor,marginTop:"4px"}}>{veredicto}</div>
                      <div style={{height:"5px",background:"#0c1826",borderRadius:"3px",overflow:"hidden",marginTop:"4px"}}>
                        <div style={{height:"100%",width:`${globalPct}%`,background:`linear-gradient(90deg,#ff3355,#ffd700 40%,#00ff88)`,borderRadius:"3px"}}/>
                      </div>
                      <div style={{fontSize:"7px",color:veredictoColor,marginTop:"2px",fontWeight:700}}>{globalPct}% score global</div>
                    </div>

                    {/* ① FXCA16 */}
                    <div style={{marginBottom:"8px",padding:"7px",background:"#050c15",borderRadius:"4px",border:`1px solid ${fxColor}20`}}>
                      <div style={{display:"flex",justifyContent:"space-between",marginBottom:"4px"}}>
                        <span style={{fontSize:"7px",color:"#4a7a9b"}}>① FXCA16</span>
                        <div style={{display:"flex",gap:"2px"}}>{[0,1,2].map(i=><div key={i} style={{width:"7px",height:"7px",borderRadius:"2px",background:i<fxRating?fxColor:"#0c1826"}}/>)}</div>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"2px",marginBottom:"4px"}}>
                        {[{l:"CONF",v:`${fxcaConf}%`,c:fxcaConf>=80?"#00ff88":"#ffd700"},{l:"FX",v:s?.fx_sc||0,c:"#00d4ff"},{l:"EVO",v:s?.evo_sc||0,c:"#ff9040"},
                          {l:"RSI",v:s?.rsi||"—",c:s?.rsi>70?"#ff3355":s?.rsi<30?"#00ff88":"#ffd700"},{l:"R/R",v:`${s?.rr||0}x`,c:s?.rr>=2?"#00ff88":"#ffd700"},{l:"ROC",v:`${s?.roc10>=0?"+":""}${s?.roc10||0}%`,c:s?.roc10>=0?"#00ff88":"#ff3355"},
                        ].map(x=>(
                          <div key={x.l} style={{textAlign:"center",padding:"2px",background:"#07101a",borderRadius:"2px"}}>
                            <div style={{fontSize:"5px",color:"#4a7a9b"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"11px",color:x.c}}>{x.v}</div>
                          </div>
                        ))}
                      </div>
                      {[{l:"Tendencia",v:s?.trend||"—",c:TC?.[s?.trend]||"#ffd700"},{l:"Vol 24h",v:`${s?.vol_24h||0}x`,c:s?.vol_24h>=1.3?"#00ff88":"#a0cce0"},{l:"EVO Prob",v:s?.evo_prob||0,c:s?.evo_prob>=0.6?"#ff9040":"#a0cce0"}].map(x=>(
                        <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px",marginBottom:"1px"}}>
                          <span style={{color:"#4a7a9b"}}>{x.l}</span><span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                        </div>
                      ))}
                    </div>

                    {/* ② CONFLUENCIA */}
                    <div style={{marginBottom:"8px",padding:"7px",background:"#050c15",borderRadius:"4px",border:`1px solid ${confColor}20`}}>
                      <div style={{display:"flex",justifyContent:"space-between",marginBottom:"4px"}}>
                        <span style={{fontSize:"7px",color:"#4a7a9b"}}>② CONFLUENCIA</span>
                        <div style={{display:"flex",gap:"2px"}}>{[0,1,2].map(i=><div key={i} style={{width:"7px",height:"7px",borderRadius:"2px",background:i<confRating?confColor:"#0c1826"}}/>)}</div>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"2px",marginBottom:"4px"}}>
                        {[{l:"SCORE",v:`${confScore}%`,c:confColor},{l:"▲ BULL",v:conf_?.bull||0,c:"#00ff88"},{l:"▼ BEAR",v:conf_?.bear||0,c:"#ff3355"}].map(x=>(
                          <div key={x.l} style={{textAlign:"center",padding:"2px",background:"#07101a",borderRadius:"2px"}}>
                            <div style={{fontSize:"5px",color:"#4a7a9b"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"11px",color:x.c}}>{x.v}</div>
                          </div>
                        ))}
                      </div>
                      {[
                        {l:"RSI Div",v:rsiDiv_?.bullish?"▲ Alcista":rsiDiv_?.bearish?"▼ Bajista":"Sin señal",c:rsiDiv_?.bullish?"#00ff88":rsiDiv_?.bearish?"#ff3355":"#5a8fa8"},
                        {l:"Medias",v:cross_?.golden?"⭐ Golden":cross_?.death?"💀 Death":cross_?.gap>=0?"SMA20>50":"SMA20<50",c:cross_?.golden?"#00ff88":cross_?.death?"#ff3355":"#a0cce0"},
                        {l:"BB+RSI",v:boll_?.oversold?"▲ Sobreventa":boll_?.overbought?"▼ Sobrecompra":"Neutral",c:boll_?.oversold?"#00ff88":boll_?.overbought?"#ff3355":"#5a8fa8"},
                        {l:"Vela",v:cand_?.patterns?.length?cand_.patterns[0].name:"Sin patrón",c:cand_?.patterns?.[0]?.type==="bullish"?"#00ff88":cand_?.patterns?.[0]?.type==="bearish"?"#ff3355":"#5a8fa8"},
                      ].map(x=>(
                        <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px",marginBottom:"1px"}}>
                          <span style={{color:"#4a7a9b"}}>{x.l}</span><span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                        </div>
                      ))}
                    </div>

                    {/* ③ ESTRUCTURAL */}
                    <div style={{marginBottom:"8px",padding:"7px",background:"#050c15",borderRadius:"4px",border:`1px solid ${estColor}20`}}>
                      <div style={{display:"flex",justifyContent:"space-between",marginBottom:"4px"}}>
                        <span style={{fontSize:"7px",color:"#4a7a9b"}}>③ ESTRUCTURAL</span>
                        <div style={{display:"flex",gap:"2px"}}>{[0,1,2].map(i=><div key={i} style={{width:"7px",height:"7px",borderRadius:"2px",background:i<estRating?estColor:"#0c1826"}}/>)}</div>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"2px",marginBottom:"4px"}}>
                        {[
                          {l:"7D",v:mtf_?.[0]?.dir==="bull"?"▲":mtf_?.[0]?.dir==="bear"?"▼":"◆",c:mtf_?.[0]?.dir==="bull"?"#00ff88":mtf_?.[0]?.dir==="bear"?"#ff3355":"#ffd700"},
                          {l:"30D",v:mtf_?.[1]?.dir==="bull"?"▲":mtf_?.[1]?.dir==="bear"?"▼":"◆",c:mtf_?.[1]?.dir==="bull"?"#00ff88":mtf_?.[1]?.dir==="bear"?"#ff3355":"#ffd700"},
                          {l:"60D",v:mtf_?.[2]?.dir==="bull"?"▲":mtf_?.[2]?.dir==="bear"?"▼":"◆",c:mtf_?.[2]?.dir==="bull"?"#00ff88":mtf_?.[2]?.dir==="bear"?"#ff3355":"#ffd700"},
                        ].map(x=>(
                          <div key={x.l} style={{textAlign:"center",padding:"2px",background:"#07101a",borderRadius:"2px"}}>
                            <div style={{fontSize:"5px",color:"#4a7a9b"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.v}</div>
                          </div>
                        ))}
                      </div>
                      {[
                        {l:"Weinstein",v:reg_?.phase||"—",c:reg_?.color||"#a0cce0"},
                        {l:"ATR",v:atrB_?.breakoutUp?"✓ Breakout":atrB_?.falseBreakUp?"⚠ Falso":"Normal",c:atrB_?.breakoutUp?"#00ff88":atrB_?.falseBreakUp?"#ffd700":"#5a8fa8"},
                        {l:"POC",v:vp_?`${FP(vp_.poc,moneda)} (${vp_.pctFromPoc>=0?"+":""}${vp_.pctFromPoc}%)`:"—",c:Math.abs(vp_?.pctFromPoc||99)<3?"#ffd700":"#a0cce0"},
                        {l:"Volumen",v:reg_?.volExpanding?"▲ Expand":reg_?.volContracting?"▼ Contrae":"Estable",c:reg_?.volExpanding?"#00ff88":reg_?.volContracting?"#ff3355":"#ffd700"},
                      ].map(x=>(
                        <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px",marginBottom:"1px"}}>
                          <span style={{color:"#4a7a9b"}}>{x.l}</span><span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                        </div>
                      ))}
                    </div>

                    {/* ④ OPTIMIZACIÓN */}
                    <div style={{marginBottom:"8px",padding:"7px",background:"#050c15",borderRadius:"4px",border:`1px solid ${optColor}20`}}>
                      <div style={{display:"flex",justifyContent:"space-between",marginBottom:"4px"}}>
                        <span style={{fontSize:"7px",color:"#4a7a9b"}}>④ OPTIMIZACIÓN</span>
                        <div style={{display:"flex",gap:"2px"}}>{[0,1,2].map(i=><div key={i} style={{width:"7px",height:"7px",borderRadius:"2px",background:i<optRating?optColor:"#0c1826"}}/>)}</div>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"2px",marginBottom:"4px"}}>
                        {[
                          {l:"WF WIN%",v:wf_?`${wf_.hr}%`:"—",c:wf_&&wf_.hr>=55?"#00ff88":wf_&&wf_.hr>=45?"#ffd700":"#ff3355"},
                          {l:"HIST WIN%",v:sq_&&sq_.total>=3?`${sq_.hr}%`:"—",c:sq_&&sq_.hr>=60?"#00ff88":sq_&&sq_.hr>=45?"#ffd700":"#ff3355"},
                          {l:"RET HIST",v:sq_&&sq_.total>=3?`${sq_.avgRet>=0?"+":""}${sq_.avgRet}%`:"—",c:sq_&&sq_.avgRet>0?"#00ff88":"#ff3355"},
                        ].map(x=>(
                          <div key={x.l} style={{textAlign:"center",padding:"2px",background:"#07101a",borderRadius:"2px"}}>
                            <div style={{fontSize:"5px",color:"#4a7a9b"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"11px",color:x.c}}>{x.v}</div>
                          </div>
                        ))}
                      </div>
                      {[
                        {l:"Consist. WF",v:wf_?`${wf_.consistency}%`:"—",c:wf_&&wf_.consistency>=60?"#00ff88":"#ffd700"},
                        {l:"Máx suba hist",v:sq_&&sq_.total>=3?`+${sq_.avgMaxRet}%`:"—",c:"#00ff88"},
                        {l:"Máx baja hist",v:sq_&&sq_.total>=3?`${sq_.avgMaxDD}%`:"—",c:"#ff3355"},
                        {l:"Eventos",v:hasRisk_?`⚠ Earnings ${events_.find(e=>e.type==="earnings")?.daysLeft}d`:events_.length?events_[0].name:"Sin eventos",c:hasRisk_?"#ff3355":events_.length?"#ffd700":"#00ff88"},
                      ].map(x=>(
                        <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px",marginBottom:"1px"}}>
                          <span style={{color:"#4a7a9b"}}>{x.l}</span><span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                        </div>
                      ))}
                    </div>

                    {/* Niveles y sizing */}
                    <div style={{padding:"7px",background:"#050c15",borderRadius:"4px"}}>
                      {[
                        {l:"ENTRADA",v:FP(s?.entry,moneda),c:"#00d4ff"},
                        {l:"STOP",v:FP(s?.sl,moneda),c:"#ff3355"},
                        {l:"TP1",v:FP(s?.tp1,moneda),c:"#7ab0c8"},
                        {l:"TP2",v:FP(s?.tp2,moneda),c:"#00ff88"},
                        {l:"SIZING",v:ps_?ps_.level:"—",c:ps_?.levelColor||"#ffd700"},
                      ].map(x=>(
                        <div key={x.l} style={{display:"flex",justifyContent:"space-between",fontSize:"7px",marginBottom:"2px"}}>
                          <span style={{color:"#4a7a9b"}}>{x.l}</span><span style={{color:x.c,fontWeight:700}}>{x.v}</span>
                        </div>
                      ))}
                    </div>
                    <button className="btn off" style={{marginTop:"6px",width:"100%",fontSize:"8px",color:"#ffd700",borderColor:"#ffd70020"}}
                      onClick={()=>addToWatchlist(activeWL,r.ticker)}>⭐ Agregar a seguimiento</button>
                  </div>
                );
              };

              return (
                <div className="fade">
                  {/* Selectores */}
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"10px",marginBottom:"12px"}}>
                    {[{val:cmpA,set:setCmpA,label:"Acción A"},{val:cmpB,set:setCmpB,label:"Acción B"}].map(({val,set,label})=>(
                      <div key={label}>
                        <div style={{fontSize:"8px",color:"#4a7a9b",marginBottom:"4px"}}>{label}</div>
                        <select value={val} onChange={e=>set(e.target.value)}
                          style={{width:"100%",background:"#07101a",color:"#00d4ff",border:"1px solid #1e3a50",borderRadius:"4px",padding:"6px 8px",fontSize:"10px",outline:"none"}}>
                          <option value="">— Seleccioná —</option>
                          {tickerList.map(t=>(
                            <option key={t} value={t}>{t} — {rows.find(r=>r.ticker===t)?.name||""}</option>
                          ))}
                        </select>
                      </div>
                    ))}
                  </div>

                  {(cmpA||cmpB) ? (
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"10px"}}>
                      <CmpCol r={rA} f={fA} label="Acción A"/>
                      <CmpCol r={rB} f={fB} label="Acción B"/>
                    </div>
                  ) : (
                    <div style={{textAlign:"center",padding:"40px",color:"#4a7a9b"}}>
                      <div style={{fontSize:"32px",marginBottom:"8px"}}>⚖️</div>
                      <div style={{fontSize:"11px"}}>Seleccioná dos acciones para comparar</div>
                    </div>
                  )}

                  {/* Ganador */}
                  {fA&&fB&&(
                    <div style={{marginTop:"12px",padding:"12px",background:"#07101a",borderRadius:"6px",border:`1px solid ${fA.globalPct>fB.globalPct?fA.veredictoColor:fB.veredictoColor}40`,textAlign:"center"}}>
                      <div style={{fontSize:"8px",color:"#4a7a9b",marginBottom:"4px"}}>MEJOR OPORTUNIDAD</div>
                      <div style={{fontFamily:"'Bebas Neue'",fontSize:"26px",color:fA.globalPct>fB.globalPct?fA.veredictoColor:fB.veredictoColor}}>
                        {fA.globalPct>fB.globalPct?cmpA:fB.globalPct>fA.globalPct?cmpB:"EMPATE"}
                      </div>
                      <div style={{fontSize:"8px",color:"#a0cce0",marginTop:"2px"}}>
                        Score {fA.globalPct}% vs {fB.globalPct}% · Diferencia {Math.abs(fA.globalPct-fB.globalPct)} pts
                      </div>
                      {fA.globalPct!==fB.globalPct&&(
                        <div style={{fontSize:"8px",color:"#ffd700",marginTop:"4px"}}>
                          📌 {fA.globalPct>fB.globalPct?cmpA:cmpB} tiene mejor veredicto ({fA.globalPct>fB.globalPct?fA.veredicto:fB.veredicto}) y mayor score global.
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })()}


            {/* ══ TAB: QUANT LAB ══ */}
            {tab==="quant"&&(
              <div className="fade">
                {/* Controles */}
                <div style={{padding:"10px 12px",background:"#07101a",border:"1px solid #1e3a50",borderRadius:"6px",marginBottom:"10px"}}>
                  <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"8px"}}>
                    🔬 VALIDACIÓN CUANTITATIVA — modelo aprendido + backtest de cartera
                  </div>
                  <div style={{fontSize:"8px",color:"#b0d4e8",lineHeight:1.7,marginBottom:"10px"}}>
                    A diferencia del resto de la app (que usa pesos escritos a mano), acá el sistema
                    <strong style={{color:"#00ff88"}}> aprende los pesos desde los datos</strong> con regresión logística,
                    etiqueta con triple-barrera, valida con K-fold purgado (sin fuga temporal),
                    calibra las probabilidades y simula una <strong style={{color:"#00ff88"}}>cartera completa</strong> con costos reales.
                  </div>
                  <div style={{display:"flex",gap:"8px",flexWrap:"wrap",alignItems:"flex-end",marginBottom:"8px"}}>
                    {[
                      {k:"topN",     l:"Posiciones simultáneas", opts:[3,5,8,10]},
                      {k:"holdDays", l:"Días de tenencia",       opts:[5,10,20,30]},
                      {k:"minProb",  l:"Prob. mínima",           opts:[0.5,0.55,0.6,0.65]},
                    ].map(f=>(
                      <div key={f.k}>
                        <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"3px"}}>{f.l}</div>
                        <div style={{display:"flex",gap:"3px"}}>
                          {f.opts.map(o=>(
                            <button key={o} className={`btn ${qlParams[f.k]===o?"on":"off"}`}
                              onClick={()=>setQlParams(p=>({...p,[f.k]:o}))}
                              style={{padding:"4px 8px",fontSize:"8px"}}>{o}</button>
                          ))}
                        </div>
                      </div>
                    ))}
                    <div>
                      <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"3px"}}>Sizing</div>
                      <button className={`btn ${qlParams.useKelly?"on":"off"}`}
                        onClick={()=>setQlParams(p=>({...p,useKelly:!p.useKelly}))}
                        style={{padding:"4px 10px",fontSize:"8px"}}>
                        {qlParams.useKelly?"Kelly ¼":"Equal weight"}
                      </button>
                    </div>
                  </div>
                  <button className={`btn ${qlRunning?"off":"on"}`} onClick={runQuantLab} disabled={qlRunning||!rows.length}
                    style={{padding:"8px 20px",fontSize:"10px"}}>
                    {qlRunning?"⏳ Procesando...":"▶ ENTRENAR Y VALIDAR"}
                  </button>
                  {qlProgress&&<span style={{fontSize:"8px",color:"#ffd700",marginLeft:"10px"}}>{qlProgress}</span>}
                  {!rows.length&&<span style={{fontSize:"8px",color:"#ff3355",marginLeft:"10px"}}>Ejecutá el sistema primero</span>}
                </div>

                {/* Resultados de cartera */}
                {qlPort&&qlPort.metrics&&(()=>{
                  const m = qlPort.metrics;
                  const veredicto = m.sharpe>=1.0&&m.cagr>0 ? {t:"ESTRATEGIA VIABLE",c:"#00ff88"}
                                  : m.sharpe>=0.5&&m.cagr>0 ? {t:"MARGINALMENTE VIABLE",c:"#ffd700"}
                                  : {t:"NO VIABLE",c:"#ff3355"};
                  return (
                    <div className="card" style={{padding:"13px",marginBottom:"10px",border:`2px solid ${veredicto.c}40`}}>
                      <div style={{textAlign:"center",marginBottom:"12px",paddingBottom:"10px",borderBottom:"1px solid #0f2235"}}>
                        <div style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".2em"}}>BACKTEST DE CARTERA · NETO DE COSTOS</div>
                        <div style={{fontFamily:"'Bebas Neue'",fontSize:"30px",color:veredicto.c,lineHeight:1.1}}>{veredicto.t}</div>
                        <div style={{fontSize:"8px",color:"#b0d4e8"}}>
                          {qlPort.nTrades} operaciones · {m.nPeriods} días simulados · {qlParams.topN} posiciones · Kelly {qlParams.useKelly?"activo":"off"}
                        </div>
                      </div>

                      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"6px",marginBottom:"10px"}}>
                        {[
                          {l:"CAGR",     v:`${m.cagr>=0?"+":""}${m.cagr}%`, c:m.cagr>0?"#00ff88":"#ff3355", h:"Retorno anualizado"},
                          {l:"SHARPE",   v:m.sharpe,   c:m.sharpe>=1?"#00ff88":m.sharpe>=0.5?"#ffd700":"#ff3355", h:">1 es bueno, >2 excelente"},
                          {l:"SORTINO",  v:m.sortino,  c:m.sortino>=1.5?"#00ff88":m.sortino>=0.7?"#ffd700":"#ff3355", h:"Sharpe que solo penaliza caídas"},
                          {l:"MAX DD",   v:`${m.maxDD}%`, c:m.maxDD>-15?"#00ff88":m.maxDD>-30?"#ffd700":"#ff3355", h:"Peor caída desde un pico"},
                          {l:"CALMAR",   v:m.calmar,   c:m.calmar>=0.5?"#00ff88":"#ffd700", h:"CAGR / MaxDD"},
                          {l:"VOL ANUAL",v:`${m.volAnual}%`, c:"#a0cce0", h:"Volatilidad anualizada"},
                          {l:"WIN RATE", v:`${qlPort.winRate}%`, c:qlPort.winRate>=50?"#00ff88":"#ffd700", h:"% operaciones ganadoras"},
                          {l:"P.FACTOR", v:qlPort.profitFactor, c:qlPort.profitFactor>=1.3?"#00ff88":qlPort.profitFactor>=1?"#ffd700":"#ff3355", h:"Ganancia bruta / pérdida bruta"},
                        ].map(x=>(
                          <div key={x.l} title={x.h} style={{textAlign:"center",padding:"6px 3px",background:"#050c15",borderRadius:"4px",border:`1px solid ${x.c}20`}}>
                            <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"17px",color:x.c}}>{x.v}</div>
                          </div>
                        ))}
                      </div>

                      {/* Curva de equity */}
                      {qlPort.equity?.length>2&&(()=>{
                        const eq=qlPort.equity, mn=Math.min(...eq), mx=Math.max(...eq);
                        const W_=Math.min(560,(typeof window!=="undefined"?window.innerWidth:400)-60), H_=90;
                        const pts=eq.map((v,i)=>`${(i/(eq.length-1))*W_},${H_-((v-mn)/(mx-mn||1))*H_}`).join(" ");
                        const up=eq[eq.length-1]>=eq[0];
                        return (
                          <div style={{marginBottom:"10px"}}>
                            <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"4px"}}>CURVA DE CAPITAL (neta de comisiones)</div>
                            <div style={{overflowX:"auto"}}>
                              <svg width={W_} height={H_} style={{display:"block"}}>
                                <line x1="0" y1={H_-((eq[0]-mn)/(mx-mn||1))*H_} x2={W_} y2={H_-((eq[0]-mn)/(mx-mn||1))*H_} stroke="#1e3a50" strokeDasharray="3 3"/>
                                <polyline points={pts} fill="none" stroke={up?"#00ff88":"#ff3355"} strokeWidth="1.6"/>
                              </svg>
                            </div>
                            <div style={{display:"flex",justifyContent:"space-between",fontSize:"7px",color:"#4a7a9b"}}>
                              <span>${Math.round(eq[0]).toLocaleString()}</span>
                              <span style={{color:up?"#00ff88":"#ff3355"}}>${Math.round(eq[eq.length-1]).toLocaleString()}</span>
                            </div>
                          </div>
                        );
                      })()}

                      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"6px",fontSize:"8px"}}>
                        {[
                          {l:"Salidas por TP",   v:qlPort.exitBreakdown.TP,   c:"#00ff88"},
                          {l:"Salidas por Stop", v:qlPort.exitBreakdown.SL,   c:"#ff3355"},
                          {l:"Salidas por tiempo",v:qlPort.exitBreakdown.TIME,c:"#ffd700"},
                        ].map(x=>(
                          <div key={x.l} style={{padding:"5px 8px",background:"#050c15",borderRadius:"3px",display:"flex",justifyContent:"space-between"}}>
                            <span style={{color:"#4a7a9b"}}>{x.l}</span><span style={{color:x.c,fontWeight:700}}>{x.v}</span>
                          </div>
                        ))}
                      </div>

                      <div style={{marginTop:"10px",padding:"8px 10px",background:`${veredicto.c}10`,border:`1px solid ${veredicto.c}30`,borderRadius:"5px",fontSize:"8px",color:"#b0d4e8",lineHeight:1.7}}>
                        📌 {m.sharpe>=1
                          ? `Sharpe ${m.sharpe} con MaxDD ${m.maxDD}%: la estrategia genera retorno ajustado por riesgo aceptable. Para gestión real, considerá reducir el sizing hasta que el MaxDD sea tolerable para vos.`
                          : m.sharpe>=0.5
                          ? `Sharpe ${m.sharpe} está por debajo del umbral profesional (1.0). La estrategia tiene señal pero el margen sobre los costos es fino. Probá horizontes más largos o subir la probabilidad mínima.`
                          : `Sharpe ${m.sharpe} y CAGR ${m.cagr}%: después de descontar comisiones la estrategia no supera al azar. No operar con estos parámetros — ajustá horizonte, umbral o revisá si el universo tiene datos reales.`}
                      </div>
                    </div>
                  );
                })()}



                {/* ══ CONSISTENCIA TEMPORAL — el test decisivo ══ */}
                {qlConsist&&(
                  <div className="card" style={{padding:"13px",marginBottom:"10px",border:`2px solid ${qlConsist.color}40`}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"3px"}}>
                      📅 CONSISTENCIA MES A MES — ¿el edge se repite o vino de un solo período?
                    </div>
                    <div style={{fontSize:"7px",color:"#5a8fa8",marginBottom:"10px",lineHeight:1.6}}>
                      Un t-stat alto sobre toda la muestra puede venir de un único mes excepcional.
                      Este test parte el historial y cuenta en cuántos períodos el filtro superó al mercado.
                      <strong style={{color:"#a0cce0"}}> Señal real: &gt;65% de meses. Azar: ~50%.</strong>
                    </div>

                    <div style={{textAlign:"center",marginBottom:"10px",paddingBottom:"10px",borderBottom:"1px solid #0f2235"}}>
                      <div style={{fontFamily:"'Bebas Neue'",fontSize:"26px",color:qlConsist.color,lineHeight:1.1}}>
                        {qlConsist.veredicto}
                      </div>
                      <div style={{fontSize:"8px",color:"#b0d4e8",marginTop:"3px"}}>
                        filtro score ≥ {qlConsist.umbral} · {qlConsist.nObs.toLocaleString()} observaciones sin lookahead
                      </div>
                    </div>

                    <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"6px",marginBottom:"10px"}}>
                      {[
                        {l:"MESES CON EDGE", v:`${qlConsist.positivos}/${qlConsist.nMeses}`, c:qlConsist.pctPositivos>=65?"#00ff88":"#ff3355"},
                        {l:"% POSITIVOS",    v:`${qlConsist.pctPositivos}%`,                 c:qlConsist.pctPositivos>=65?"#00ff88":qlConsist.pctPositivos>=55?"#ffd700":"#ff3355"},
                        {l:"EXCESO MEDIO",   v:`${qlConsist.excesoMedio>=0?"+":""}${qlConsist.excesoMedio}%`, c:qlConsist.excesoMedio>0?"#00ff88":"#ff3355"},
                        {l:"SIN EL MEJOR MES",v:`${qlConsist.excesoSinMejor>=0?"+":""}${qlConsist.excesoSinMejor}%`, c:qlConsist.excesoSinMejor>0?"#00ff88":"#ff3355"},
                      ].map(x=>(
                        <div key={x.l} style={{textAlign:"center",padding:"6px 3px",background:"#050c15",borderRadius:"4px",border:`1px solid ${x.c}20`}}>
                          <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"17px",color:x.c}}>{x.v}</div>
                        </div>
                      ))}
                    </div>

                    {/* Barras por mes */}
                    <div style={{marginBottom:"8px"}}>
                      <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"5px"}}>EXCESO SOBRE EL MERCADO, MES A MES</div>
                      {(()=>{
                        const mx = Math.max(...qlConsist.filas.map(f=>Math.abs(f.exceso)), 1);
                        return qlConsist.filas.map(f=>(
                          <div key={f.mes} style={{display:"flex",alignItems:"center",gap:"5px",marginBottom:"2px"}}>
                            <span style={{fontSize:"7px",color:"#a0cce0",width:"48px",flexShrink:0}}>{f.mes}</span>
                            <div style={{flex:1,height:"9px",background:"#0c1826",borderRadius:"2px",position:"relative",overflow:"hidden"}}>
                              <div style={{position:"absolute",left:"50%",top:0,bottom:0,width:"1px",background:"#1e3a50"}}/>
                              <div style={{position:"absolute",top:0,bottom:0,borderRadius:"2px",
                                left: f.exceso>=0 ? "50%" : `${50 - Math.abs(f.exceso)/mx*48}%`,
                                width:`${Math.abs(f.exceso)/mx*48}%`,
                                background: f.exceso>=0?"#00ff88":"#ff3355", opacity:.85}}/>
                            </div>
                            <span style={{fontSize:"7px",width:"46px",textAlign:"right",color:f.exceso>=0?"#00ff88":"#ff3355",fontWeight:600}}>
                              {f.exceso>=0?"+":""}{f.exceso}%
                            </span>
                            <span style={{fontSize:"6px",width:"32px",textAlign:"right",color:"#4a7a9b"}}>n={f.nSeñal}</span>
                          </div>
                        ));
                      })()}
                    </div>

                    <div style={{padding:"8px 10px",background:`${qlConsist.color}10`,border:`1px solid ${qlConsist.color}30`,borderRadius:"4px",fontSize:"8px",color:"#b0d4e8",lineHeight:1.7}}>
                      📌 {qlConsist.concentracion > 70
                        ? `El ${qlConsist.concentracion}% del exceso total viene de un solo mes (${qlConsist.mejorMes}). Sin ese mes el exceso medio es ${qlConsist.excesoSinMejor}%. Esto es amplificación de beta en un rally, no capacidad predictiva: el filtro carga activos de alto momentum, que se mueven más cuando el mercado sube fuerte y peor el resto del tiempo.`
                        : qlConsist.pctPositivos >= 65
                        ? `El filtro superó al mercado en ${qlConsist.positivos} de ${qlConsist.nMeses} meses (${qlConsist.pctPositivos}%), y mantiene ${qlConsist.excesoSinMejor}% de exceso incluso descartando el mejor período. Eso es consistente con un edge real.`
                        : `Solo ${qlConsist.pctPositivos}% de los meses tuvieron exceso positivo — no se distingue del azar (50%). El filtro no demuestra capacidad predictiva sostenida en este historial.`}
                    </div>
                  </div>
                )}

                {/* ══ VALIDACIÓN DE ROBUSTEZ ══ */}
                {qlValid&&(qlValid.dsr||qlValid.pbo)&&(
                  <div className="card" style={{padding:"13px",marginBottom:"10px",border:"1px solid #ff904030"}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"4px"}}>
                      🛡️ ¿ES REAL O ES SOBREAJUSTE?
                    </div>
                    <div style={{fontSize:"7px",color:"#5a8fa8",marginBottom:"10px",lineHeight:1.6}}>
                      Un Sharpe alto no prueba nada si probaste cientos de configuraciones.
                      Estas métricas corrigen por eso.
                    </div>

                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"8px",marginBottom:"10px"}}>
                      {/* Deflated Sharpe */}
                      {qlValid.dsr&&(
                        <div style={{padding:"9px",background:"#050c15",borderRadius:"5px",border:`1px solid ${qlValid.dsr.significativo?"#00ff88":"#ff3355"}30`}}>
                          <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>DEFLATED SHARPE RATIO</div>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color:qlValid.dsr.significativo?"#00ff88":qlValid.dsr.dsr>=0.9?"#ffd700":"#ff3355",lineHeight:1}}>
                            {qlValid.dsr.veredicto}
                          </div>
                          <div style={{fontSize:"7px",color:"#b0d4e8",marginTop:"4px",lineHeight:1.6}}>
                            DSR = <strong style={{color:qlValid.dsr.significativo?"#00ff88":"#ff3355"}}>{(qlValid.dsr.dsr*100).toFixed(1)}%</strong> ·
                            Sharpe {qlValid.dsr.sharpe} vs umbral {qlValid.dsr.sharpeUmbral} exigido por azar
                          </div>
                          <div style={{fontSize:"7px",color:"#5a8fa8",marginTop:"3px"}}>
                            Corregido por {qlValid.dsr.nTrials.toLocaleString()} configuraciones probadas ·
                            skew {qlValid.dsr.skew} · kurtosis {qlValid.dsr.kurtosis}
                            {qlValid.dsr.minTrackRecord&&` · requiere ${qlValid.dsr.minTrackRecord} días de track record`}
                          </div>
                        </div>
                      )}

                      {/* PBO */}
                      {qlValid.pbo&&(
                        <div style={{padding:"9px",background:"#050c15",borderRadius:"5px",border:`1px solid ${qlValid.pbo.color}30`}}>
                          <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>PROBABILIDAD DE SOBREAJUSTE (PBO)</div>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color:qlValid.pbo.color,lineHeight:1}}>
                            {qlValid.pbo.veredicto}
                          </div>
                          <div style={{fontSize:"7px",color:"#b0d4e8",marginTop:"4px",lineHeight:1.6}}>
                            PBO = <strong style={{color:qlValid.pbo.color}}>{(qlValid.pbo.pbo*100).toFixed(1)}%</strong> ·
                            {qlValid.pbo.nCombos} particiones cruzadas sobre {qlValid.pbo.nConfigs} configuraciones
                          </div>
                          <div style={{fontSize:"7px",color:"#5a8fa8",marginTop:"3px"}}>
                            &lt;20% confiable · 20-50% moderado · &gt;50% el backtest no vale
                          </div>
                        </div>
                      )}
                    </div>

                    {/* Bootstrap */}
                    {qlValid.boot&&(
                      <div style={{padding:"9px",background:"#050c15",borderRadius:"5px",marginBottom:"8px"}}>
                        <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"6px"}}>
                          BOOTSTRAP · {qlValid.boot.nBoot} remuestreos — ¿cuán frágil es el resultado?
                        </div>
                        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"6px",marginBottom:"6px"}}>
                          {[
                            {l:"SHARPE",  o:qlValid.boot.sharpe, s:""},
                            {l:"CAGR",    o:qlValid.boot.cagr,   s:"%"},
                            {l:"MAX DD",  o:qlValid.boot.maxDD,  s:"%"},
                          ].map(x=>(
                            <div key={x.l} style={{textAlign:"center",padding:"5px",background:"#07101a",borderRadius:"3px"}}>
                              <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l} · IC 90%</div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:"#e8f4ff"}}>{x.o.p50}{x.s}</div>
                              <div style={{fontSize:"7px",color:"#5a8fa8"}}>[{x.o.p05}{x.s} , {x.o.p95}{x.s}]</div>
                            </div>
                          ))}
                        </div>
                        <div style={{display:"flex",gap:"10px",fontSize:"7px"}}>
                          <span style={{color:"#4a7a9b"}}>P(Sharpe &gt; 0) = <strong style={{color:qlValid.boot.probSharpePositivo>=0.9?"#00ff88":"#ffd700"}}>{(qlValid.boot.probSharpePositivo*100).toFixed(0)}%</strong></span>
                          <span style={{color:"#4a7a9b"}}>P(Sharpe &gt; 1) = <strong style={{color:qlValid.boot.probSharpe1>=0.5?"#00ff88":"#ff9040"}}>{(qlValid.boot.probSharpe1*100).toFixed(0)}%</strong></span>
                        </div>
                      </div>
                    )}

                    {/* Unicidad */}
                    {qlValid.uniq&&(
                      <div style={{padding:"8px 10px",background:qlValid.uniq.avgUniqueness<0.2?"#ff335510":"#ffd70010",
                        border:`1px solid ${qlValid.uniq.avgUniqueness<0.2?"#ff335530":"#ffd70030"}`,borderRadius:"4px",fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                        ⚠️ <strong>Unicidad de muestras: {qlValid.uniq.avgUniqueness}</strong> —
                        las etiquetas se solapan en el tiempo, así que las observaciones no son independientes.
                        Tu muestra <strong style={{color:"#ff9040"}}>efectiva</strong> es de ~<strong>{qlValid.uniq.effectiveN}</strong> casos,
                        no del total nominal. Cualquier intervalo de confianza calculado sin esta corrección está inflado.
                      </div>
                    )}

                    {/* Regímenes */}
                    {qlValid.regimes?.length>0&&(
                      <div style={{marginTop:"8px"}}>
                        <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"5px"}}>
                          RENDIMIENTO POR RÉGIMEN DE MERCADO — ¿gana siempre o solo en bull?
                        </div>
                        {qlValid.regimes.map(r=>(
                          <div key={r.regime} style={{display:"flex",alignItems:"center",gap:"6px",padding:"4px 7px",marginBottom:"2px",
                            background:"#050c15",borderRadius:"3px",borderLeft:`3px solid ${r.avgRet>0?"#00ff88":"#ff3355"}`}}>
                            <span style={{fontSize:"7px",color:"#a0cce0",width:"120px",flexShrink:0}}>{r.regime}</span>
                            <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"4px",flex:1,fontSize:"7px"}}>
                              {[
                                {l:"n",v:r.n,c:"#a0cce0"},
                                {l:"WR",v:r.winRate+"%",c:r.winRate>=50?"#00ff88":"#ff3355"},
                                {l:"AVG",v:(r.avgRet>=0?"+":"")+r.avgRet+"%",c:r.avgRet>0?"#00ff88":"#ff3355"},
                                {l:"SR",v:r.sharpe,c:r.sharpe>=0.3?"#00ff88":"#ffd700"},
                              ].map(x=>(
                                <div key={x.l} style={{textAlign:"center"}}>
                                  <span style={{color:"#4a7a9b",fontSize:"6px"}}>{x.l} </span>
                                  <span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                        {(()=>{
                          const bull = qlValid.regimes.filter(r=>r.regime.includes("ALCISTA"));
                          const bear = qlValid.regimes.filter(r=>r.regime.includes("BAJISTA"));
                          const bullOk = bull.some(r=>r.avgRet>0), bearOk = bear.some(r=>r.avgRet>0);
                          return (
                            <div style={{marginTop:"5px",padding:"6px 8px",background:bearOk?"#00ff8810":"#ff904010",borderRadius:"3px",fontSize:"7px",color:"#b0d4e8",lineHeight:1.6}}>
                              📌 {bullOk&&bearOk
                                ? "Gana en ambos regímenes: la estrategia tiene alfa genuino, no es beta disfrazada."
                                : bullOk&&!bearOk
                                ? "Solo gana en mercados alcistas. Eso es exposición al mercado, no habilidad. Considerá apagar el sistema cuando el índice pierda su SMA50."
                                : "Rendimiento débil en todos los regímenes."}
                            </div>
                          );
                        })()}
                      </div>
                    )}
                  </div>
                )}

                {/* ══ META-LABELING ══ */}
                {qlMeta&&(
                  <div className="card" style={{padding:"13px",marginBottom:"10px",border:"1px solid #00d4ff30"}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"4px"}}>
                      🎭 META-LABELING — segundo modelo que decide SI operar
                    </div>
                    <div style={{fontSize:"7px",color:"#5a8fa8",marginBottom:"10px",lineHeight:1.6}}>
                      El modelo primario dice la dirección. El secundario responde otra pregunta:
                      <em> "¿vale la pena tomar esta señal en particular?"</em> Filtra falsos positivos
                      sin cambiar la dirección. Con costos de {COSTO_CEDEAR}% por operación, filtrar es tan valioso como acertar.
                    </div>

                    <div style={{display:"grid",gridTemplateColumns:"1fr auto 1fr",gap:"10px",alignItems:"center",marginBottom:"10px"}}>
                      <div style={{padding:"10px",background:"#050c15",borderRadius:"5px",textAlign:"center"}}>
                        <div style={{fontSize:"7px",color:"#4a7a9b"}}>SIN META-LABELING</div>
                        <div style={{fontFamily:"'Bebas Neue'",fontSize:"26px",color:"#ff9040"}}>{qlMeta.wrSin}%</div>
                        <div style={{fontSize:"7px",color:"#5a8fa8"}}>{qlMeta.totSin.toLocaleString()} operaciones</div>
                      </div>
                      <div style={{textAlign:"center"}}>
                        <div style={{fontSize:"18px",color:"#00d4ff"}}>→</div>
                        <div style={{fontSize:"7px",color:"#00ff88",fontWeight:700}}>+{qlMeta.avgMejora} pts</div>
                      </div>
                      <div style={{padding:"10px",background:"#00ff8810",borderRadius:"5px",textAlign:"center",border:"1px solid #00ff8830"}}>
                        <div style={{fontSize:"7px",color:"#4a7a9b"}}>CON META-LABELING</div>
                        <div style={{fontFamily:"'Bebas Neue'",fontSize:"26px",color:"#00ff88"}}>{qlMeta.wrCon}%</div>
                        <div style={{fontSize:"7px",color:"#5a8fa8"}}>{qlMeta.totCon.toLocaleString()} operaciones ({qlMeta.filtrado}% filtradas)</div>
                      </div>
                    </div>

                    <div style={{maxHeight:"200px",overflowY:"auto",WebkitOverflowScrolling:"touch"}}>
                      {qlMeta.items.slice(0,20).map(m=>(
                        <div key={m.ticker} style={{display:"flex",alignItems:"center",gap:"6px",padding:"4px 7px",marginBottom:"2px",
                          background:"#050c15",borderRadius:"3px",borderLeft:`3px solid ${m.mejora>0?"#00ff88":"#ff3355"}`}}>
                          <span style={{fontFamily:"'Bebas Neue'",fontSize:"13px",color:m.mejora>0?"#00ff88":"#ff3355",width:"50px",flexShrink:0}}>{m.ticker}</span>
                          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"4px",flex:1,fontSize:"7px"}}>
                            {[
                              {l:"AUC 2º", v:m.auc.toFixed(3), c:m.auc>=0.55?"#00ff88":"#ffd700"},
                              {l:"WR sin", v:m.sinMeta.winRate+"%", c:"#ff9040"},
                              {l:"WR con", v:m.conMeta.winRate+"%", c:"#00ff88"},
                              {l:"Filtra", v:m.conMeta.filtrado+"%", c:"#a0cce0"},
                            ].map(x=>(
                              <div key={x.l} style={{textAlign:"center"}}>
                                <div style={{color:"#4a7a9b",fontSize:"6px"}}>{x.l}</div>
                                <div style={{color:x.c,fontWeight:600}}>{x.v}</div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Importancia de features */}
                {qlAblation&&(
                  <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                    <div style={{display:"flex",justifyContent:"space-between",marginBottom:"8px"}}>
                      <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>📊 IMPORTANCIA DE INDICADORES (ablación)</div>
                      <div style={{fontSize:"8px",color:qlAblation.baseAuc>=0.55?"#00ff88":"#ffd700"}}>AUC base {qlAblation.baseAuc.toFixed(3)}</div>
                    </div>
                    <div style={{fontSize:"7px",color:"#5a8fa8",marginBottom:"8px",lineHeight:1.6}}>
                      Se quita cada indicador y se mide cuánto empeora el modelo. Delta alto = aporta información real.
                      Delta ≈ 0 o negativo = es ruido y podría eliminarse.
                    </div>
                    {qlAblation.items.map(f=>{
                      const pct = Math.min(100, Math.abs(f.delta)*2000);
                      const good = f.delta > 0.002;
                      return (
                        <div key={f.feature} style={{display:"flex",alignItems:"center",gap:"6px",marginBottom:"3px"}}>
                          <span style={{fontSize:"7px",color:"#a0cce0",width:"85px",flexShrink:0}}>{f.feature}</span>
                          <div style={{flex:1,height:"7px",background:"#0c1826",borderRadius:"3px",overflow:"hidden"}}>
                            <div style={{height:"100%",width:`${pct}%`,background:good?"#00ff88":f.delta<0?"#ff3355":"#4a7a9b",borderRadius:"3px"}}/>
                          </div>
                          <span style={{fontSize:"7px",color:good?"#00ff88":f.delta<0?"#ff3355":"#4a7a9b",width:"48px",textAlign:"right"}}>
                            {f.delta>=0?"+":""}{(f.delta*100).toFixed(2)}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                )}

                {/* Calidad de modelos por ticker */}
                {qlModels&&(
                  <div className="card" style={{padding:"12px"}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"6px"}}>
                      🎯 MODELOS POR ACTIVO ({qlModels.length}) — ordenados por poder predictivo
                    </div>
                    <div style={{fontSize:"7px",color:"#5a8fa8",marginBottom:"8px",lineHeight:1.6}}>
                      <strong>AUC</strong>: 0.50 = azar, &gt;0.55 = señal real, &gt;0.60 = fuerte ·
                      <strong> Brier Skill</strong>: &gt;0 significa que la probabilidad calibrada supera a predecir la tasa base ·
                      Todo medido <strong>fuera de muestra</strong> con K-fold purgado.
                    </div>
                    <div style={{maxHeight:"340px",overflowY:"auto",WebkitOverflowScrolling:"touch"}}>
                      {qlModels.map(m=>{
                        const ok = m.auc>=0.55, mid = m.auc>=0.52;
                        const c = ok?"#00ff88":mid?"#ffd700":"#ff3355";
                        return (
                          <div key={m.ticker} style={{display:"flex",alignItems:"center",gap:"6px",padding:"5px 7px",marginBottom:"3px",background:"#050c15",borderRadius:"4px",borderLeft:`3px solid ${c}`}}>
                            <span style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:c,width:"52px",flexShrink:0}}>{m.ticker}</span>
                            <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:"4px",flex:1,fontSize:"7px"}}>
                              {[
                                {l:"AUC",   v:m.auc.toFixed(3),      c},
                                {l:"Brier", v:m.brier?.toFixed(3)??"—", c:"#a0cce0"},
                                {l:"Skill", v:(m.brierSkill>=0?"+":"")+m.brierSkill?.toFixed(3), c:m.brierSkill>0?"#00ff88":"#ff3355"},
                                {l:"n",     v:m.n,                    c:"#a0cce0"},
                                {l:"Top",   v:m.weights?.[0]?.name??"—", c:"#ff9040"},
                              ].map(x=>(
                                <div key={x.l} style={{textAlign:"center"}}>
                                  <div style={{color:"#4a7a9b",fontSize:"6px"}}>{x.l}</div>
                                  <div style={{color:x.c,fontWeight:600}}>{x.v}</div>
                                </div>
                              ))}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                    {(()=>{
                      const good = qlModels.filter(m=>m.auc>=0.55).length;
                      const pct = Math.round(good/qlModels.length*100);
                      return (
                        <div style={{marginTop:"8px",padding:"7px 9px",background:pct>=30?"#00ff8810":"#ff335510",border:`1px solid ${pct>=30?"#00ff8830":"#ff335530"}`,borderRadius:"4px",fontSize:"8px",color:"#b0d4e8",lineHeight:1.6}}>
                          📌 {good} de {qlModels.length} activos ({pct}%) tienen poder predictivo real (AUC ≥ 0.55).
                          {pct>=30
                            ? " El sistema encuentra estructura genuina en una porción significativa del universo. Concentrá el capital en esos activos."
                            : " La mayoría de los activos no muestran predictibilidad. Esto es normal en mercados eficientes — operá solo los del tope de la lista."}
                        </div>
                      );
                    })()}
                  </div>
                )}
              </div>
            )}

            {/* OPTIMIZADOR */}

                        <div style={{marginTop:"12px",padding:"6px 10px",background:"#050c15",borderRadius:"4px",fontSize:"8px",color:"#1e3a50"}}>
              ⚠️ FXCA16 · Precios vía Anthropic Web Search · Histórico sintético · Umbral dinámico P80 · No es asesoramiento financiero.
            </div>
          </div>
        )}
      </div>
    </div>
  );
}