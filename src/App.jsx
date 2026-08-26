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
// Deduplica por (fecha, hora) al expandir. Defensa del lado de la app:
// data.js puede venir con barras repetidas si un ticker quedó en la lista
// estándar y en custom_tickers.json a la vez (ver el fix en update_data.py).
// Las barras repetidas rompen los indicadores — cada una es una barra de
// variación cero intercalada, que aplana el RSI hacia 50 y duplica de hecho
// el período de suavizado del MACD.
function expandEmbedded(raw){const out={};for(const [tk,bars] of Object.entries(raw)){const seen=new Set();out[tk]=bars.filter(b=>{const k=b.d+'|'+b.h;if(seen.has(k))return false;seen.add(k);return true;}).map(b=>({date:b.d,hour:b.h,open:b.o,high:b.hi,low:b.lo,close:b.c,volume:b.v,moneda:b.m,_ticker:tk}));}return out;}


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
// ── HIPÓTESIS PROBADAS Y DESCARTADAS ──
//
// Memoria institucional: patrones investigados manualmente (fuera del
// pipeline normal de quant.js/quant2.js) con metodología y resultado
// explícito. Objetivo: no volver a "descubrir" y operar la misma idea
// unos meses después pensando que es nueva. Ver docs/hallazgos.md para
// el detalle completo de cada test.
const HALLAZGOS_DESCARTADOS = [
  {
    fecha: "2026-08-26",
    hipotesis: "AUDITORÍA DE CONSTANTES: verificar que cada constante precalculada reproduce lo que dice medir",
    origen: "Tras encontrar VOL_MEDIA_ANUAL rota y CORRELATION_GROUPS incorrectos, se auditaron TODAS las constantes que afectan señales contra los datos de 10 años.",
    metodo: "Para cada constante: recalcular la cantidad declarada desde CSV_DATA_DAILY_RAW / serie horaria y comparar. Donde aplica, además split temporal 60/40 para ver si el efecto es estable.",
    n: "RSI: 380.808 obs · DOW: 384.419 obs · horarias: 251.335 obs",
    resultado: "RSI_TASAS_BASE ✅ CORRECTA — reproduce casi exacto (pUp 33.0 declarado vs 32.7 real, 25.9 vs 25.7, 21.6 vs 21.6; promedio general 21.4 vs 21.7). Es la única de la familia que pasa limpia. HOUR_RELIABILITY ✅ DEFENDIBLE — los factores no siguen el orden de los retornos medios, pero sí correlacionan con la magnitud del movimiento (|ret| medio h13=1.386% vs h20=0.279%), que es lo que dice medir (confiabilidad, no retorno). DOW_FACTOR ❌ ROTA E INESTABLE — ver entrada aparte.",
    veredicto: "auditoría completa — 1 rota de 3 revisadas",
    nota: "Estado final de todas las constantes del proyecto: VOL_MEDIA_ANUAL (rota → reemplazada por media móvil), CORRELATION_GROUPS (rota → reemplazada por cálculo), DOW_FACTOR (rota e inestable → neutralizada), adaptiveScoreAdj (sobreajuste → neutralizada), TICKER_CONFIDENCE (sobreajuste → eliminada), dynParams/adaptiveW (perjudicaba → neutralizada), RSI_TASAS_BASE (✅ correcta), HOUR_RELIABILITY (✅ defendible), VELAS_TASAS_BASE (✅ honesta, medida con la norma nueva). El chequeo que destapó las tres rotas es el mismo y toma minutos: recalcular la cantidad declarada desde los datos y comparar.",
  },
  {
    fecha: "2026-08-26",
    hipotesis: "DOW_FACTOR: el día de la semana modula la confiabilidad de la señal (viernes malo, martes bueno)",
    origen: "Constante hardcodeada con comentarios explicativos ('Viernes: cierre de posiciones, evitar señales nuevas'). Auditada junto al resto.",
    metodo: "Retorno medio diario por día de la semana sobre 10 años y 384.419 observaciones. Split temporal 60/40 promediando POR FECHA (no por observación) para chequear estabilidad.",
    n: "384.419 obs, ~77.000 por día de la semana",
    resultado: "Los factores están INVERTIDOS respecto a los datos. Ranking declarado: Martes > Miércoles > Jueves > Lunes > Viernes. Ranking real: Miércoles > Viernes > Martes > Lunes > Jueves. El viernes tenía el factor MÁS BAJO (0.90) y es el SEGUNDO MEJOR día (+0.1807%, t=13.92); el jueves con factor 1.03 es el peor (+0.1102%). Y el efecto no es estable: entre las dos mitades el orden se da vuelta (IS: Vie>Mie>Mar>Jue>Lun · OOS: Lun>Mie>Vie>Jue>Mar) — el lunes pasa de último a primero, el martes de tercero a último.",
    veredicto: "neutralizada — todos los factores en 1.00",
    nota: "Doble problema: además de estar invertida, no hay efecto que capturar. Un ranking que se da vuelta al partir la muestra es ruido con estructura aparente, no señal. Es el mismo error que atrp<0.87 y que el rally de octubre 2025: describir el período en vez de encontrar un patrón. A diferencia de las otras constantes rotas, esta SÍ afectaba el score en producción vía timeFactor (hourFactor × dowFactor).",
  },

  {
    fecha: "2026-08-26",
    hipotesis: "CORRELATION_GROUPS: los 4 grupos escritos a mano cubrían el riesgo de concentración",
    origen: "Revisión del sistema completo. Los grupos estaban hardcodeados con comentarios que declaraban correlaciones específicas.",
    metodo: "Correlación de Pearson sobre retornos logarítmicos diarios, 250 ruedas, todo el universo (158 tickers = 12.403 pares posibles). Se contrastan las correlaciones declaradas contra las medidas.",
    n: "158 tickers, 250 ruedas",
    resultado: "Tres problemas. (1) El ticker 'YPF' NO EXISTE en el universo — es YPFD — así que el grupo de energía Merval estaba parcialmente roto. (2) Los 4 grupos cubrían 16 de los 104 pares con correlación >0.70 del universo: el 85% de las correlaciones altas quedaba sin contemplar (CRM-NOW 0.80, MU-XLK 0.75, AMD-XLK 0.73, QQQ-SPY 0.93, QQQ-XLK 0.957). (3) El grupo tech se declaraba en 0.68-0.70 y su correlación real es 0.465 — sobreestimado.",
    veredicto: "reemplazado por cálculo desde los datos",
    nota: "Otra constante hardcodeada que no reproducía lo que decía medir, igual que VOL_MEDIA_ANUAL. Ahora matrizCorrelacion() calcula sobre 250 ruedas terminando en la rueda anterior, coherente con la norma del proyecto. Resultado: 104 pares sobre 61 tickers, contra los 17 tickers de los grupos viejos. GGAL pasa de correlacionar con 3 papeles a 11 (AUSO, BHIP, BMA, BPAT, CEPU, EDN, LOMA, METR, PAMP, SUPV, TGSU2). Además la deduplicación ahora exige MISMA DIRECCIÓN de señal para degradar: dos papeles correlacionados con señales opuestas no son la misma apuesta y antes se trataban igual. Los grupos viejos se conservan sólo como semilla de respaldo si la serie diaria no estuviera disponible.",
  },
  {
    fecha: "2026-08-26",
    hipotesis: "LIMITACIÓN ESTRUCTURAL: el universo de datos tiene sesgo de supervivencia — afecta TODAS las validaciones del proyecto",
    origen: "Revisión completa del sistema. No es una hipótesis de mercado sino una limitación de los datos que hay que tener presente al leer cualquier resultado de este registro.",
    metodo: "Inspección de CSV_DATA_DAILY_RAW: se contaron los tickers cuya serie termina en la fecha actual (vivos) contra los que dejaron de cotizar antes.",
    n: "158 tickers, 10 años de historia diaria",
    resultado: "158 tickers vivos, 0 dados de baja. El universo son EXCLUSIVAMENTE supervivientes: son las empresas que existen hoy, y se les mide la historia de 10 años hacia atrás. Las que quebraron, se deslistaron o fueron absorbidas no están.",
    veredicto: "limitación conocida — no corregible con los datos actuales",
    nota: "DIRECCIÓN DEL SESGO POR HALLAZGO: (1) Las señales de REVERSIÓN A LA MEDIA están INFLADAS — 'ARS bajo SMA200 rebota' se mide sólo sobre papeles que efectivamente se recuperaron; los que cayeron y nunca volvieron no están en la muestra. Ese hallazgo hay que leerlo con ese descuento. (2) Los VETOS están si acaso SUBESTIMADOS — el veto de Fibonacci dice 'comprar sin soporte cercano rinde peor'; si estuvieran los papeles que desaparecieron, rendiría todavía peor. Es sesgo conservador, juega a favor. (3) Los hallazgos CROSS-SECCIONALES (persistencia direccional, alfa) están parcialmente protegidos porque el exceso se mide contra el universo del mismo día, y el sesgo afecta a todos los papeles del pool de forma similar — pero no elimina el problema. MITIGACIÓN POSIBLE: yfinance no entrega deslistados fácilmente; habría que sumar una fuente de tickers históricos. Mientras tanto, el Tracker es la única evidencia libre de este sesgo, porque marca hacia adelante sobre el universo real del momento.",
  },
  {
    fecha: "2026-08-26",
    hipotesis: "VETO: comprar lejos de cualquier nivel de Fibonacci (sin soporte estructural cerca) rinde peor",
    origen: "Barrido de Fibonacci como variable, cruzado con dirección de señal y trend",
    metodo: "156 tickers × 10 años (63.659 obs) con combinedSignal() y calcFibonacci() reales. Condición: COMPRA + trend=ALCISTA + distancia al nivel Fib más cercano > 3.92% (tercil superior) + ese nivel clasificado como 'soporte' débil. Exceso a 20 días controlado por fecha, moneda y tercil de volatilidad. Split temporal 60/40, drop-one por ticker, consistencia anual.",
    n: "424 obs totales (314 en muestra / 110 fuera de muestra), 93 tickers",
    resultado: "Exceso -2.632% a 20d con t=-3.80. Fuera de muestra -3.066% con t=-2.50, MÁS negativo que en muestra. Win rate 41.5%. Drop-one: sin el ticker más favorable (INTC) sigue en t=-2.99. CONSISTENCIA ANUAL: 8 de 8 años con exceso negativo. Funciona en ambos mercados: ARS -3.461% (t=-3.46), USD -1.930% (t=-2.10).",
    veredicto: "confirmado como veto — pendiente de implementar",
    nota: "El hallazgo más sólido de los preliminares: 8/8 años negativos, aguanta drop-one, más fuerte fuera de muestra que dentro, y funciona en los dos mercados (a diferencia de la persistencia direccional, que es sólo Merval). Mecanismo coherente con el fix de R/R estructural: comprar lejos de todo soporte es comprar sin piso técnico. Cautela: 424 observaciones y 45/93 tickers con exceso negativo (la mitad), así que se apoya en pocos casos por activo. Como VETO no paga comisión — filtrar una compra es gratis — así que el umbral de evidencia para aplicarlo es más bajo que para una señal de entrada.",
  },
  {
    fecha: "2026-08-26",
    hipotesis: "En señales de VENTA, el campo trend discrimina el resultado (ALCISTA FUERTE vs BAJISTA FUERTE se comportan distinto)",
    origen: "Observación del usuario de que no es lo mismo una venta con tendencia bajista que bajista fuerte. Quedó preliminar con 1 año de datos horarios; se cierra acá con 10 años.",
    metodo: "156 tickers × 10 años, señales de VENTA separadas por trend, exceso a 3 días controlado por fecha, moneda y tercil de volatilidad. Split 60/40. Test explícito del gradiente entre extremos.",
    n: "34.617 señales de venta repartidas en 5 niveles de trend",
    resultado: "Sólo ALCISTA FUERTE cruza: -0.287% (t=-1.99) y fuera de muestra -0.557% con t=-3.65, MÁS fuerte fuera que dentro. Los otros cuatro niveles no cruzan en ningún caso (|t| ≤ 1.35). El test del gradiente ALCISTA FUERTE vs BAJISTA FUERTE da t=-1.62 a 3 días y t=-0.75 a 20 días: NO hay gradiente ordenado por tendencia.",
    veredicto: "parcial — sólo sirve el extremo ALCISTA FUERTE, y en dirección CONTRARIA a la esperada",
    nota: "La hipótesis original (que trend discrimina de forma ordenada) NO se sostiene: no hay gradiente, y el test entre extremos no cruza (t=-1.62 a 3d, t=-0.75 a 20d). Lo que sí queda es un caso puntual: VENTA sobre papeles en tendencia ALCISTA FUERTE da exceso -0.287% (OOS -0.557%, t=-3.65, más fuerte fuera de muestra). ⚠ CORRECCIÓN DE INTERPRETACIÓN: exceso negativo en una señal de VENTA significa que el papel rinde PEOR que sus pares, o sea que la venta ACIERTA. Con 1 año de datos horarios este mismo caso había dado exceso POSITIVO y se interpretó como 'ahí la señal de venta falla' — con 10 años el signo es el opuesto. Verificado: retorno crudo +0.159%, exceso -0.155%, baja el 45.4% de las veces. O sea que el papel sube en términos absolutos pero menos que sus pares. NO es candidato a veto: es el subgrupo donde la venta funciona MEJOR, así que si algo habría que priorizarlo, no filtrarlo. Antes de usarlo, tener presente que el retorno crudo es positivo — sólo gana en términos relativos, que es lo que importa para ranking pero no para una venta en seco.",
  },
  {
    fecha: "2026-08-26",
    hipotesis: "Los patrones de vela deberían estimarse por activo con encogimiento (norma nueva) en vez de en pool global",
    origen: "Aplicación de la norma del proyecto (estimar por activo con encogimiento) al primer caso pendiente. Motivado por un análisis previo que halló efectos de ±1% por activo donde la tabla global mostraba 0.2%.",
    metodo: "184.766 detecciones sobre 156 tickers y 10 años con detectCandlePattern() real. Exceso a 4 días controlado por fecha, moneda y tercil de volatilidad. Split temporal 60/40. Se compara predicción global vs. por activo vs. encogida (peso calculado por método de momentos: tau² entre activos menos varianza de muestreo).",
    n: "184.766 detecciones; 26 a 188 por activo según el patrón",
    resultado: "El encogimiento se autorregula como corresponde. Peso propio por patrón: 3 Cuervos 0.647 (173 detecciones/activo, tau²=0.1201), 3 Velas Alcistas 0.613 (188, tau²=0.1115), Martillo 0.110, Engulfing Bajista 0.007, y CUATRO patrones con peso exactamente 0.000 (Doji, Engulfing Alcista, Estrella Fugaz, Marubozu) porque tau²=0: toda la varianza entre activos es ruido de muestreo. Fuera de muestra la correlación con el exceso real es diminuta con los tres métodos (por activo entre -0.021 y +0.038, encogido entre 0 y +0.022).",
    veredicto: "medido — NO se cambia la tabla global",
    nota: "Sólo 2 de los 8 patrones (3 Cuervos y 3 Velas Alcistas) tienen heterogeneidad real por activo, y aun en esos el poder predictivo fuera de muestra sigue siendo casi nulo. Reemplazar VELAS_TASAS_BASE por la versión por activo agregaría cómputo sin mejorar la predicción. Se deja la tabla global como está y queda anotado que se midió con el método correcto. VALOR DEL EJERCICIO: es la demostración de que la norma funciona en ambas direcciones — donde hay evidencia por activo (LMSW, peso 0.48) la usa, y donde no la hay (4 patrones con peso 0.000) devuelve el global sola, sin que haya que decidirlo a mano.",
  },
  {
    fecha: "2026-08-26",
    hipotesis: "dynParams / adaptiveW: cada activo tiene un W óptimo propio que mejora las señales",
    origen: "update_data.py probaba W ∈ {5,7,10,14} sobre la serie completa de cada ticker y se quedaba con el de mejor win rate; adaptiveW() lo aplicaba en producción. Se midió como parte de la revisión de constantes precalculadas.",
    metodo: "157 tickers × 10 años, split temporal 60/40, replicando backtest_w() exacto del script (cruce SMA20/50, stop 1.5×ATR, TP 2.5×ATR). Se elige el W en la primera mitad y se evalúa en la segunda, contra el W global fijo.",
    n: "157 tickers con datos OOS suficientes (≥20 trades)",
    resultado: "W por activo fuera de muestra: 47.48%. W global fijo (=5): 47.71%. Diferencia -0.23pp con t=-2.62: no es que no ayude, PERJUDICA significativamente. El W propio le gana al global en 8 de 157 activos (5%), exactamente lo esperable por azar. El orden de los W es idéntico dentro y fuera de muestra (5:47.7% > 7:46.3% > 10:44.3% > 14:42.5%) — W=5 es mejor para todos.",
    veredicto: "neutralizado — adaptiveW() devuelve el W global",
    nota: "Coherente con la norma del proyecto: acá el peso correcto para la estimación por activo es 0, porque toda la varianza entre activos es ruido de muestreo. dynParams usaba peso 1.0 sin justificarlo — mismo error que adaptiveScoreAdj y TICKER_CONFIDENCE. ⚠ BUG ADICIONAL más grave descubierto al medirlo: los 158 tickers tenían sims>=5, así que adaptiveW SIEMPRE devolvía el W aprendido y nunca llegaba al globalW. El selector de ventana de la UI (7/14/30/60) estaba siendo ignorado por completo en Oportunidades — 138 tickers calculaban con W=5 sin importar qué eligiera el usuario. Con la neutralización el selector vuelve a funcionar. ⚠ Nota aparte: el win rate ronda 47.5% en TODOS los W, o sea que la estrategia que backtest_w() usa para elegir pierde más veces de las que gana; los conf y p80adj derivados de ese win rate estaban construidos sobre esa base (no se usaban en ningún lado).",
  },
  {
    fecha: "2026-08-03",
    hipotesis: "MACD subiendo (2-3 días) + volumen bajo predice rally a 3-5 días",
    origen: "BA, GLOB, ORCL, AAL, BABA subieron juntas la semana del 27-31/7; parecía un patrón técnico común",
    metodo: "Universo completo (158 activos), julio 2026, 9 variantes (2 vs 3 días MACD, con/sin filtro RSI<50, forward 3 y 5 días)",
    n: "605 a 1194 casos con señal por variante, contra 2000-2900 sin señal",
    resultado: "Ninguna variante alcanzó significancia (máx |t|=1.35, umbral 1.96). En 7 de 9 variantes el grupo SIN señal rindió mejor que el grupo CON señal. Win rate 44-52% en todas — indistinguible de azar.",
    veredicto: "descartada",
    nota: "La suba real fue por la decisión de la Fed del 29/7 (hold hawkish + rebote), no por una señal técnica — 5 sectores sin relación entre sí subiendo juntos es la firma de un movimiento de mercado amplio (beta), no de alfa individual. Consistente con el ablation existente: macdN ya rankeaba cerca del último lugar en importancia (delta AUC -14.33).",
  },
  {
    fecha: "2026-08-03",
    hipotesis: "El retorno de la semana siguiente a una reunión FOMC es distinto al de una semana normal",
    origen: "Derivado del hallazgo anterior — si la Fed movió el mercado en julio, ¿se repite en las demás reuniones de 2026?",
    metodo: "Las 4 reuniones FOMC de 2026 ya completas (28/1, 18/3, 29/4, 17/6), universo completo, retorno 7d antes/después vs. baseline de semanas normales",
    n: "158 tickers × 4 fechas = hasta 632 observaciones nominales — pero efectivamente solo 4 eventos independientes (ver nota)",
    resultado: "t-stat 'después vs. baseline' = -4.66 (aparenta alta significancia). Pero el n nominal está inflado: las 158 observaciones de cada fecha comparten el mismo shock macro del día, no son independientes. La muestra efectiva real son 4 eventos, insuficiente para confiar en cualquier t-stat por alto que parezca.",
    veredicto: "sin conclusión (muestra insuficiente, no descartada ni confirmada)",
    nota: "Dato honesto que sí queda: de las 4 reuniones completas, 3 tuvieron retorno negativo la semana después (-2.65%, -1.30%, -0.99%) y solo 1 positivo (+0.61%). La suba de julio fue la excepción del año, no la regla. Para testear esto en serio harían falta ~80 reuniones (10 años de historia diaria ya disponible) en vez de 4 — pendiente, no prioritario por ahora.",
  },
  {
    fecha: "2026-08-03",
    hipotesis: "Reversión: caído >8% del máximo de 20 días + RSI<45 anticipa suba",
    origen: "Buscando qué precedía a las subas de las 20 acciones que más subieron en 3 meses. Replicó en el universo completo — a diferencia del patrón de MACD, este sí pasó el primer filtro",
    metodo: "Universo completo, 10 años (65.047 señales), horizontes 7 a 60 días, con tres correcciones sucesivas: exceso sobre mercado, muestra efectiva por solapamiento, y control de volatilidad",
    n: "65.047 señales nominales — pero solo ~1.056 efectivas a H=60 por solapamiento de ventanas (59/60)",
    resultado: "Prometía +1.70% (t=7.27) en la ventana de 3 meses. Al corregir: el edge cayó a +0.32pp en 10 años; a H=7 daba -0.27% neto de costos; y contra papeles de VOLATILIDAD SIMILAR el exceso se derrumbó de 2.59% a 0.24% con t=0.26 (no significativo). Neto de costos ajustado: -1.30% por operación (≈ -5.47% anual). Consistencia anual 6/11 años (55%), debajo del umbral de 65%.",
    veredicto: "descartada",
    nota: "Era exposición a beta/volatilidad, no alfa: la señal selecciona papeles 1.36x más volátiles, y comprar papeles golpeados y volátiles en un mercado que subió 10 años paga por el riesgo tomado, no por capacidad predictiva. Lección reutilizable: cuando una señal selecciona activos con volatilidad sistemáticamente distinta, el control correcto es contra activos de volatilidad comparable en la misma fecha — sin eso, casi cualquier filtro de 'papeles golpeados' parece funcionar en un mercado alcista.",
  },
  {
    fecha: "2026-08-03",
    hipotesis: "El score técnico marca COMPRA cuando el activo ya subió — ¿se puede detectar 'el paso antes'?",
    origen: "Observación directa: las señales de COMPRA FUERTE aparecían después de que el papel ya se había movido",
    metodo: "combinedSignal() real sobre 39 tickers, último año, ~2.300 señales; más 5 reglas de entrada distintas basadas en el perfil precursor (estado 7 días antes de que dispare la señal)",
    n: "2.301 señales medidas · 2.184 días evaluados para las reglas precursoras",
    resultado: "COMPRA FUERTE llega después de +13.7% (20d) y captura +0.42% — ratio 34:1, y por debajo del baseline de 0.97%. Ninguna de las 5 reglas precursoras supera al baseline; la mejor da 0.83% vs 0.89%, y 'RSI 50-65' da -0.32% (t=-3.15, peor de forma significativa). Correlación con retorno a 20d: RSI 0.004, ya-subió-10d 0.095, vs-SMA20 0.085.",
    veredicto: "no existe el paso antes (en estos indicadores)",
    nota: "Razón estructural, no de calibración: los indicadores del score se calculan A PARTIR del movimiento de precio, así que no lo anticipan — lo describen. Adelantar la señal solo agrega falsos positivos. No alcanza para afirmar que la señal esté invertida (t VENTA vs COMPRA FUERTE = 1.48), pero sí que COMPRA FUERTE no aporta sobre comprar al azar.",
  },
  {
    fecha: "2026-08-03",
    hipotesis: "Búsqueda exhaustiva: 43 indicadores técnicos + 66 combinaciones de a dos",
    origen: "Barrido sistemático buscando cualquier indicador o par que anticipe subas",
    metodo: "IC de Spearman cross-seccional por fecha, horizonte 10 días. Primero en 3 meses, después validado sobre 10 años (110.806 obs), con Bonferroni por 43 pruebas, control de volatilidad y descuento de costos",
    n: "110.806 observaciones a 10 años · 24 indicadores pasan Bonferroni",
    resultado: "HAY señal detectable: 24 indicadores pasan Bonferroni y casi todos con IC NEGATIVO — el momentum está invertido a 10 días (más RSI/ROC/sobre-SMA20 → menor retorno futuro). Signo estable entre 3 meses y 10 años en 23 de 24. Mejor combinación roc20+roc5 (IC +0.0387, t=6.26) pero IR de solo 0.18. Test operativo: exceso +0.41% cae a +0.279% (t=1.47, no significativo) al controlar volatilidad, y NETO DE COSTOS da -1.327% por operación (-33% anual).",
    veredicto: "señal real pero no operable",
    nota: "El efecto es 5-6x más chico que el costo de transacción. Implicación importante: la dirección es CONTRARIA a cómo el score técnico usa estos indicadores — el score marca COMPRA FUERTE con momentum alto, y los datos dicen que el momentum alto precede retornos menores. Eso explica por qué COMPRA FUERTE rinde debajo del baseline. Combinar indicadores casi no agrega: están muy correlacionados entre sí.",
  },
  {
    fecha: "2026-08-03",
    hipotesis: "El alfa cross-sectional es lo único validado fuera de muestra (según el traspaso: IC 0.127, IR 1.06, t=8.38)",
    origen: "Verificar la afirmación del documento de traspaso contra los 10 años de serie diaria",
    metodo: "Reproducir la fórmula documentada (rango(vol_shock) − rango(mom_1m), suavizado 10d) sobre 10 años de datos que el modelo nunca vio — se desarrolló sobre la serie horaria de ~1 año",
    n: "103 activos USD × 10 años, evaluado por período y en agregado",
    resultado: "Solo funciona en 2026, el período donde se construyó (monotonía +0.76). En el agregado 2016-2026 la monotonía entre quintil y exceso da -0.66 (INVERTIDA) y el t de Q5 da exactamente 0.00. Por período: 2016-19 mono -0.17, 2020-22 -0.94, 2023-24 -0.48, 2025 -0.78.",
    veredicto: "sobreajustado a su ventana de desarrollo",
    nota: "Lo que sí es cierto y vale rescatar: el alfa dispara ANTES que el score técnico — Q5 compra papeles que vienen -6.9% en 20 días, contra el score que marca COMPRA FUERTE después de +13.7%. El mecanismo es el correcto, el problema es que el ranking no predice. Salvedad: la reproducción usa la fórmula documentada y puede diferir en detalles de la implementación real de alpha.js.",
  },
  {
    fecha: "2026-08-17",
    hipotesis: "Patrón EVO>50 + reversión>50 + RSI<50 + MACD<0 anticipa un giro alcista",
    origen: "Continuación del hallazgo del 03-08 (mismo patrón, primera vez) — probado en tres escalas de muestra y método crecientes",
    metodo: "Corrida 1: top 20 ganadores de 30 días. Corrida 2: reconstrucción diaria sobre 10 años (validada al 34% de fidelidad contra la función real). Corrida 3: año completo con combinedSignal() real, sin aproximación. Más una extensión cruzando quintil de alfa y nivel de Fibonacci.",
    n: "27 casos (corrida 1) · 79.019 casos/10 años (corrida 2) · 1.337 casos/153 tickers (corrida 3)",
    resultado: "Corrida 1: WR 91-100% dentro del top20 (sesgado) → 51-60% con control (azar). Corrida 2: sin edge en ningún test, consistencia anual 55% (bajo el umbral 65%). Corrida 3: confirma la 1 — WR 66% sesgado vs 50% en control real. La extensión con quintil+Fibonacci encontró una combinación con +4.83% a 20d, pero con muestra efectiva real de 8 (no 164) y t=0.57 tras corrección — y el día 7 específico es el punto donde MENOS se distingue del control (t=-0.02).",
    veredicto: "descartada en las tres escalas",
    nota: "No es un patrón chico que necesite más ajuste — está sistemáticamente ausente, independientemente de cómo se lo mida. El patrón de sesgo de selección (verse espectacular dentro de un grupo ya elegido por su resultado, desaparecer en el control) se repitió de forma idéntica en las tres corridas.",
  },
  {
    fecha: "2026-08-17",
    hipotesis: "Los 20 activos que más subieron y los 20 que más bajaron en el último año se distinguen por sus indicadores técnicos",
    origen: "Comparar los extremos opuestos para buscar un patrón común, en vez de solo mirar ganadores",
    metodo: "9 indicadores medidos al inicio del período (sin lookahead): RSI, MACD, distancia a SMA50/200, volatilidad anual, distancia al máximo de 52 semanas, calidad fundamental, margen neto, deuda/patrimonio. El único que cruzó significancia (distancia al máximo, t=2.57) se validó después con ventanas móviles sobre 10 años.",
    n: "20 vs 20 (comparación inicial) · 13.739 observaciones (validación con ventanas móviles)",
    resultado: "8 de 9 indicadores sin diferencia significativa. El único con t=2.57 (activos cerca de su máximo rindieron mejor) se desarmó al validar: con ventanas móviles sobre 10 años el t corregido por solapamiento cae a 0.10, consistencia anual 5/9 (56%, bajo umbral), y lo poco que sobrevivía en los quintiles de volatilidad alta resultó ser exposición a volatilidad, no señal (en los quintiles normales, t entre -0.20 y 0.47).",
    veredicto: "descartada",
    nota: "El t=2.57 inicial era un artefacto de comparar dos grupos definidos por su propio resultado (top 20 vs bottom 20 ya conocidos) — 151 activos que en realidad compartían el mismo año, mismo ciclo, misma Fed. No eran observaciones independientes. Mismo error de fondo que el hallazgo de MACD del 03-08, en otra forma.",
  },
  {
    fecha: "2026-08-17",
    hipotesis: "Reacción del mercado a los anuncios FOMC: cuándo se mueve, cuánto, y qué sector impacta más",
    origen: "Ampliar el hallazgo del 03-08 (4 reuniones de 2026, sin conclusión) con el calendario histórico completo",
    metodo: "78 anuncios FOMC 2017-2026 (fuente federalreserve.gov), trayectoria del universo día -5 a +20, clasificado por si la reacción del día 0 fue positiva o negativa. Impacto sectorial: 22 sectores, retorno en días FOMC vs. retorno normal del mismo sector (no contra cero).",
    n: "76 eventos con datos suficientes · 22 sectores, hasta 1.976 observaciones por sector",
    resultado: "El mercado NO anticipa (días -5 a -1 planos). El salto ocurre entero el día 0 (+1.27% vs -0.94%, t=11.34) y no se revierte, pero tampoco se extiende: descontando el día 0, el recorrido posterior no es significativo (t=1.29). Por sector: ningún sector es desproporcionadamente más volátil en días FOMC (todos los ratios ≤1.06x). En sesgo direccional, 6 de 22 cruzan significancia — Tecnología el más sólido (t=3.73, n=1.790), seguido de Consumo (negativo, t=-3.51), Bonos, Materiales y Salud (negativo). Financiero, el candidato obvio por tasas, no muestra nada (t=0.64).",
    veredicto: "informativo, no operable",
    nota: "La clasificación positiva/negativa se hizo por la reacción del mercado ese día, no por el contenido del comunicado — es circular por diseño. La diferencia sectorial es la reacción del día 0 en sí, no una ventaja capturable después de verla. Ya implementado en la app como contexto (estadoFOMC()); no se agregó nada nuevo porque no hay ventaja operable que agregar.",
  },
  {
    fecha: "2026-08-25",
    hipotesis: "Si el sistema da compra/compra fuerte con volumen sobre su media, al día siguiente baja; si da venta/venta fuerte con volumen sobre su media, al día siguiente sube; si da venta con volumen bajo, al día siguiente baja",
    origen: "Hipótesis del usuario sobre la relación entre señal y vol_24h vs su media histórica",
    metodo: "80 tickers (top/bottom 20 por score W=60 + 40 de control) × 381 fechas = 15.561 obs, combinedSignal() real, exceso vs. universo del mismo día y moneda, t clusterizado por fecha",
    n: "15.561 observaciones, 381 fechas independientes",
    resultado: "Acción por acción: 51 aciertos de signo sobre 110 = 46.4% (azar 50%). Global: 2 de 3 celdas con el signo correcto, ninguna con |t|>1.96. La de mayor magnitud (COMPRA+vol alto, +0.30%) va al revés de lo esperado. El grupo de control se comportó igual o mejor que los 40 seleccionados.",
    veredicto: "descartada",
    nota: "Detonó la revisión de VOL_MEDIA_ANUAL (ver regla activa 'vol vs media móvil'): la constante usada para 'volumen vs media' tenía sesgo de anticipación y estaba desviada hasta 0.89 puntos por ticker respecto al valor real. Corregida en esta sesión.",
  },
  {
    fecha: "2026-08-25",
    hipotesis: "COMPRA con volumen muy por debajo de su media anticipa caída (Merval, señal fuerte)",
    origen: "Barrido sistemático de 83 subgrupos (dirección × fuerza × tercil de volumen × moneda) tras descartar la hipótesis anterior",
    metodo: "Panel horario 1 año, control por volatilidad, drop-one por ticker, consistencia mensual, split IS/OOS 60/40",
    n: "n=160 (ARS, señal fuerte) hasta n=865 (ARS, todas las fuerzas)",
    resultado: "Con VOL_MEDIA_ANUAL (constante rota): exceso -0.88% (t=-3.92), pasó drop-one (peor caso t=-2.10), 12/15 meses negativos (80%). Al recalcular con la media móvil de 6 meses corregida, el efecto se invirtió a horizontes >1 día: 5d pasó de -0.22% a +0.04%, 10d de -0.32% a +0.42%.",
    veredicto: "descartada — era el sesgo de la constante, no señal",
    nota: "El mejor hallazgo aparente de la sesión resultó ser un artefacto de la constante VOL_MEDIA_ANUAL con lookahead. Lección: cuando un indicador se corrige, revalidar todo lo construido sobre él antes de confiar en los números viejos.",
  },
  {
    fecha: "2026-08-25",
    hipotesis: "Búsqueda abierta de patrones de éxito: pares de condiciones (indicador×umbral) sobre el universo completo, sin hipótesis previa",
    origen: "Pedido explícito de seguir buscando patrones de éxito tras agotar la hipótesis original del usuario",
    metodo: "158 tickers × ~188 ruedas = 31.691 obs, 24 variables, ~3.400-4.700 combinaciones de a pares probadas por corrida (2 corridas: antes y después de corregir el volumen), holdout 60/40, control de volatilidad",
    n: "3.411 tests (1ª corrida) + 4.716 tests (2ª corrida, volumen corregido)",
    resultado: "1ª corrida: 69 candidatos con t>2.5 en muestra, 4 sobrevivieron fuera de muestra (ver reglas activas R/R y SMA200 Merval). 2ª corrida: 338 candidatos en muestra, 0 sobrevivieron. Las 4 mejores en muestra de la 2ª corrida compartían atrp<0.87 (baja volatilidad); ese átomo solo daba +1.01% en muestra (t=2.47) y +0.04% fuera (t=0.93).",
    veredicto: "método validado, la mayoría de los candidatos son ruido",
    nota: "El período de muestra (hasta feb-2026) tuvo un régimen donde baja volatilidad + momentum rindió bien; cualquier filtro que seleccionara esos papeles daba t>5 sin ser señal real — es una descripción del período, no un patrón. Mismo error que el rally de oct-2025 ya anotado, con otra cara. El holdout es lo único que lo destapa.",
  },
  {
    fecha: "2026-08-25",
    hipotesis: "En señales de VENTA, distinguir trend=ALCISTA FUERTE de ALCISTA/LATERAL/BAJISTA/BAJISTA FUERTE cambia el resultado a 3 días",
    origen: "Observación del usuario: 'no es lo mismo venta con señal bajista y bajista fuerte en relación al volumen'",
    metodo: "Top 20 por retorno de 60 días previos + universo completo, VENTA discriminada por trend × signo de volumen (móvil 6m), retorno a 3 días, control de volatilidad",
    n: "55 a 3.168 obs por celda según combinación",
    resultado: "VENTA+ALCISTA+volumen bajo: exceso -0.649% (t=-2.59, n=594), única celda que cruza 1.96 en el universo control, y mantiene signo en el top-20 (-0.831%). VENTA+ALCISTA FUERTE: exceso POSITIVO en top-20 con ambos signos de volumen (+0.671% n=33 vol+, +0.621% t=1.40 n=251 vol-) — ahí la señal de venta falla.",
    veredicto: "preliminar, sin confirmar — falta validación en 10 años",
    nota: "La columna de volumen bajo por trend no muestra gradiente (-0.10/-0.65/-0.12/-0.05/-0.11): una celda sobresale 6x sobre sus vecinas, que están todas pegadas. Un efecto real por tendencia raramente saltea el nivel de al lado, así que podría ser una celda con suerte en vez de mecanismo. Extender a 10 años (con la recursión de combinedSignal ya cortada, la corrida es barata) antes de operar esto.",
  },
  {
    fecha: "2026-08-25",
    hipotesis: "R/R estructural (findStructuralLevels) predice retorno por sí solo, con magnitud suficiente para operar",
    origen: "Cierre pendiente de la sesión: la medición inicial sobre 1 año de serie horaria mostraba +1.24% a 20d (t=5.69) y quedó marcada 'en observación — falta 10 años' por sólo tener 3 ventanas independientes a 45d",
    metodo: "103 tickers USD × 10 años = 42.123 obs con combinedSignal() real sobre CSV_DATA_DAILY_RAW, quintiles de R/R, control de volatilidad, corrección por ventanas independientes (antes imposible con 1 año)",
    n: "42.123 obs, 1.961 fechas, corrección por solapamiento: 84 ventanas independientes a 20 días",
    resultado: "El gradiente es real y monótono (Q1 -0.22% → Q4+Q5 +0.18% a 20d) pero 3-6x más chico que lo medido sobre 1 año. Causa: el umbral >1.97 capturaba 43% del universo, no un extremo — 40% del panel tiene RR=2.00 exacto (tope de diseño del cálculo, no valor emergente). Corregido por ventanas independientes, t cae de 5.69 a 1.25 a 20d — no alcanza para declarar el efecto sólido ni con toda la historia disponible. Sigue sin cubrir el costo (+0.18% vs 1.8%).",
    veredicto: "confirmado como efecto real pero débil — permanece en observación, no pasa a aplicada",
    nota: "La medición sobre 1 año estaba inflada por la escasez de historia, no por sobreajuste al período (a diferencia del hallazgo de atrp<0.87, este SÍ mantuvo el signo en 10 años). Lección: 'sobrevive holdout OOS' con 1 año de datos no es lo mismo que 'sobrevive con historia suficiente' — ambos chequeos hacen falta antes de declarar una regla operable. Ver REGLAS_ACTIVAS para el veredicto final actualizado.",
  },
  {
    fecha: "2026-08-25",
    hipotesis: "CIERRE DE LÍNEA: el volumen relativo del propio papel (serie temporal) predice retorno — 5 variantes probadas",
    origen: "Hipótesis sostenida del usuario a lo largo de la sesión: un pico de volumen anticipa reversión o continuación. Se probaron 5 construcciones distintas del mismo constructo antes de cerrar.",
    metodo: "Todas sobre 10 años de diarias (63.510 obs, 156 tickers) salvo la primera; control por fecha+moneda+tercil de volatilidad, t clusterizado por fecha, holdout temporal 60/40. Variantes: (1) vol_24h vs VOL_MEDIA_ANUAL, (2) vol_24h vs media móvil 6 meses, (3) vol10 = volumen de ayer vs media de las 10 ruedas previas, (4) pico de volumen firmado por día alcista (acumulación), (5) pico firmado por día bajista (distribución).",
    n: "63.510 obs, 2.150 fechas, ~6.000 obs por brazo en el test firmado",
    resultado: "Ninguna variante cruzó significancia de forma sostenible. (1) resultó ser una constante rota con lookahead (ver entrada aparte). (2) efecto sólo a 1 día, 0.10-0.26% contra costos de 1.2-1.8%. (3) el único candidato del barrido con holdout (VENTA FUERTE + vol10 normal, r10) pasó de t=2.85 en muestra a t=1.26 fuera: 0 de 1 sobrevive. (4) y (5): la diferencia acumulación menos distribución da t entre -1.30 y +0.14 según horizonte — el signo del precio en el día del pico NO discrimina.",
    veredicto: "línea cerrada — no volver a probar variantes de ventana",
    nota: "Observación que sí quedó, sin cruzar: 'pico de volumen en día bajista' es positivo en los 4 horizontes, creciendo con el plazo (+0.38% a 20d, +0.655% fuera de muestra, WR 59.1%), lo cual apunta a rebote tras capitulación y NO a agotamiento tras euforia. Máximo t=1.53. CIERRE DEFINITIVO (posterior): se probó además el marco LMSW con volumen detrendado interactuando con el retorno, y el resultado confirma que el volumen NO es el motor: el término del volumen (C2) aporta t=1.75 en Merval mientras el término de momentum puro (C1) da t=10.38. Lo que parecía efecto de volumen era momentum diario persistente de Merval. Ver REGLAS_ACTIVAS 'Persistencia direccional'. CONTRASTE: el volumen SÍ funciona cross-seccionalmente — alpha.js usa rango(vol_shock) con IC +0.127 y t=8.38 validado; probado como discriminador de señales da un gradiente con Q1 significativo (t=-2.32 a 20d) y un veto de compra a 20 días con t=2.18 en muestra que se cae a t=-1.09 fuera. La distinción que decide no es la ventana sino el marco de referencia: contra el universo del día vs contra la historia del propio papel.",
  },
];

// ── REGLAS ACTIVAS ───────────────────────────────────────────────
//
// Hipótesis que SÍ sobrevivieron validación fuera de muestra. Nacen acá
// como documentación explícita antes de entrar (si corresponde) al motor
// de señales; el estado indica si ya están aplicadas en producción o
// todavía en observación.
const REGLAS_ACTIVAS = [
  {
    fecha: "2026-08-26",
    regla: "Veto de Fibonacci: no priorizar compras sin soporte estructural cerca",
    estado: "aplicada",
    descripcion: "vetoFibonacci(sig, data) marca las COMPRA con trend=ALCISTA donde el precio está a más de 3.92% del nivel de Fibonacci más cercano y ese nivel es sólo un 'soporte' débil — es decir, comprar sin piso técnico. La señal se DEGRADA (COMPRA FUERTE pasa a COMPRA) y sale del conteo de oportunidades, pero NO se elimina.",
    evidencia: "156 tickers × 10 años (63.659 obs), exceso a 20 días controlado por fecha, moneda y tercil de volatilidad. Exceso -2.632% con t=-3.80; fuera de muestra -3.066% (t=-2.50), MÁS fuerte fuera que dentro. Win rate 41.5%. Drop-one: sin el ticker más favorable (INTC) sigue en t=-2.99. Consistencia: 8 de 8 años con exceso negativo. Funciona en los DOS mercados: ARS -3.461% (t=-3.46), USD -1.930% (t=-2.10) — es el único hallazgo de la sesión que no es exclusivo de Merval. En el universo actual veta el 3.2% de las compras.",
    uso: "Como VETO no paga comisión: filtrar una compra es gratis, así que el umbral de evidencia es más bajo que para una señal de entrada. El sesgo de supervivencia del universo juega A FAVOR: si estuvieran los papeles deslistados, el veto sería aún más necesario. Degrada en vez de eliminar porque son 424 obs sobre 93 tickers (45/93 con exceso negativo) — alcanza para bajar prioridad, no para descartar.",
  },
  {
    fecha: "2026-08-26",
    regla: "Correlación entre posiciones calculada de los datos, no declarada a mano",
    estado: "aplicada",
    descripcion: "matrizCorrelacion() calcula correlación de Pearson sobre retornos log diarios, ventana de 250 ruedas terminando en la rueda anterior. De cada conjunto de señales correlacionadas por encima de 0.70 Y EN LA MISMA DIRECCIÓN, se mantiene la de mayor convicción y las demás se degradan a NEUTRAL con nota de con quién correlacionan.",
    evidencia: "Los 4 grupos hardcodeados que había cubrían 16 de los 104 pares con correlación >0.70 del universo (85% sin contemplar), declaraban el grupo tech en 0.68-0.70 cuando su correlación real es 0.465, y referenciaban un ticker inexistente (YPF en vez de YPFD). El cálculo detecta 104 pares sobre 61 tickers, contra los 17 tickers de los grupos viejos.",
    uso: "Evita concentrar riesgo en la misma apuesta: el sizing de Kelly del backtest de cartera asume independencia entre posiciones, y con correlación alta el riesgo real es mayor que el calculado. Mejora respecto de la versión vieja: ahora exige misma dirección de señal — dos papeles correlacionados con señales opuestas no son la misma apuesta.",
  },

  {
    fecha: "2026-08-25",
    regla: "NORMA DEL PROYECTO: estimar por activo con encogimiento, no en pool global",
    estado: "aplicada — estándar para toda estimación nueva",
    descripcion: "Los activos no se comportan igual entre sí ni a lo largo del tiempo. Toda estimación de parámetros que no tenga una especificación propia debe hacerse POR ACTIVO, con ventana dinámica de 400 ruedas que termina en la RUEDA ANTERIOR (nada del día evaluado entra en los coeficientes), y encogida hacia el promedio del universo en proporción a su ruido: w = τ²/(τ²+se²), donde τ² es la varianza real entre activos (varianza observada menos error de estimación medio) y se² el error propio. Un activo con estimación precisa conserva su valor; uno ruidoso se acerca al global. El fallback al global es automático — no hay que programarlo aparte.",
    evidencia: "Experimento sobre 279.735 obs diarias, 156 tickers, 10 años, ~750 fechas fuera de muestra, usando el modelo LMSW como banco de pruebas. Spread long-short a 1 día en ARS — global: +0.081% (t=2.65), por activo puro: +0.240% (t=9.75), encogido: +0.225% (t=8.39). Fuera de muestra: global +0.257% (t=4.61), puro +0.346% (t=7.64), encogido +0.367% (t=7.36). POR ACTIVO TRIPLICA AL GLOBAL: evaluar en pool único tiraba dos tercios de la señal. Entre puro y encogido la diferencia es chica (el encogido gana fuera de muestra, pierde dentro); se elige encogido por robustez, porque es lo que impide que 'por activo' degenere en el error de TICKER_CONFIDENCE y dynParams. Ventana: se probaron 10/50/100/250/300/400 — de 250 a 400 los resultados son casi iguales (t 9.75/10.36/9.84) pero el peso propio mediano sube de 0.29 a 0.48, o sea que con 400 ruedas el activo merece casi la mitad del peso. Por debajo de 250 se degrada rápido (t=1.87 a 100 ruedas, 0.90 a 50, no estimable a 10).",
    uso: "Aplicar a toda estimación de parámetros nueva. IMPORTANTE: el encogimiento se ajusta solo a la evidencia disponible — para constructos con pocas observaciones por activo (ej. patrones de vela, que dejan 10-30 detecciones en 400 ruedas) el peso propio va a ser ~0.05 y el método devolverá prácticamente el global. Eso es correcto, no un defecto: refleja que no hay evidencia suficiente para diferenciar ese activo. Costo de cómputo: el prior global cuesta ~1s (156 regresiones) y se cachea por mes; las estimaciones individuales posteriores son instantáneas.",
  },
  {
    fecha: "2026-08-25",
    regla: "Persistencia direccional (LMSW): en Merval el retorno diario continúa, en USD no hay efecto",
    estado: "aplicada — sólo Merval; en USD se muestra sin validar",
    descripcion: "persistenciaDireccional(ticker, hastaFecha) estima por regresión rodante si el movimiento de hoy tiende a continuar o revertirse mañana. Especificación normalizada: z(r[t+1]) = c0 + c1·z(r[t]) + c2·z(r[t])·z(V[t]), donde V = log(volumen) menos su media móvil de 200 ruedas (detrendado, se adapta al régimen actual del papel). Ventana de 250 ruedas, r y V estandarizados con la propia ventana. Marco teórico: Llorente, Michaely, Saar y Wang (Review of Financial Studies, 2002) — los retornos generados por reparto de riesgo revierten, los generados por trading informado continúan; C2 mide de qué lado está el papel.",
    evidencia: "303.096 obs, 156 tickers, 10 años, TODO fuera de muestra por construcción (el C2 de cada día se estima sólo con las 250 ruedas anteriores). Spread long-short a 1 día, exceso vs. universo del mismo día: ARS +0.225% t=8.39 (fuera de muestra +0.367%, t=7.36) / USD sin efecto con ningún método ni ventana (|t| < 1.92). Drop-one: 156 de 156 corridas siguen con t>2.58 (rango 3.41-3.95), ningún ticker sostiene el resultado. Consistencia: 8 de 9 años positivos. La normalización fue decisiva: mejoró ARS de t=6.21 a 10.29 y hundió USD de 0.48 a -0.16. En USD se probaron 6 cortes distintos (iliquidez de Amihud, volumen en dólares, ETF vs acción, magnitud del movimiento, descomposición C1/C2, versión normalizada) y ninguno supera |t|=1.05; 5 de 9 años negativos. Ventana: se probaron 250/100/50/10 y empeoran de forma monótona (t 3.72 / 1.87 / 0.90 / no estimable) — acortar capta más ruido, no mejor régimen (el desvío de C2 sube de 0.159 a 0.388 y el signo se vuelve MENOS estable).",
    uso: "Criterio de RANKING, no señal de entrada: +0.248% diario contra 1.2% de comisión Merval no se opera. Badge sólido en ARS, punteado con tooltip 'sin validar' en USD (mismo criterio que el alfa preliminar). Visible en Detalle y en Replay; en Replay se corta por fecha para no mirar al futuro. ⚠ MATIZ IMPORTANTE: el efecto NO es principalmente de volumen. El término C1 solo da t=10.38 en ARS (más que el modelo completo) y el término del volumen C2 aporta t=1.75. Lo que se descubrió es que Merval tiene momentum diario persistente (C1 medio +0.070 contra -0.006 en USD) y el marco LMSW lo capta de rebote. Por eso se llama 'persistencia direccional' y no 'indicador de volumen'.",
  },
  {
    fecha: "2026-08-25",
    regla: "Vol vs media: ventana móvil de 6 meses, no constante anual",
    estado: "aplicada",
    descripcion: "volMediaMovil(data): media de vol_24h sobre las últimas 126 ruedas, terminando en la última barra de la rueda ANTERIOR (nunca incluye la barra evaluada). Reemplaza a VOL_MEDIA_ANUAL.",
    evidencia: "La constante vieja tenía rango 0.663-1.939 entre tickers cuando la cantidad que decía medir (media de largo plazo de un cociente) tiene que dar ≈1 por construcción — estaba desviada hasta 0.89 puntos en algunos tickers (53% de los tickers con desvío >0.30). Además incluía días posteriores a la fecha evaluada (sesgo de anticipación). Corregida, cobertura 157/158 tickers sin caer al fallback.",
    uso: "vol_media_mov y vol_mm_ruedas expuestos en combinedSignal(). volVsMedia(ticker, vol24h, sig) los usa automáticamente.",
  },
  {
    fecha: "2026-08-25",
    regla: "R/R estructural: gradiente real pero débil, insuficiente para operar como señal — sólo como tilt de ranking",
    estado: "en observación — validado en 10 años, no pasa a aplicada",
    descripcion: "El ratio riesgo/beneficio que ya calcula findStructuralLevels() muestra relación monótona con el retorno, pero la magnitud es demasiado chica para justificar su uso como filtro de entrada.",
    evidencia: "CIERRE con 10 años (42.123 obs, 103 tickers USD, 2018-2026), reemplaza la medición preliminar sobre 1 año horario. Por quintil de R/R, exceso a 20d con control de volatilidad: Q1[1.07-1.80] -0.215% (t=-2.27), Q2[1.80-1.89] -0.177% (t=-1.75), Q3[1.89-2.00] +0.016% (t=0.14), Q4+Q5[≥2.00] +0.182% (t=2.54). Monótono, pero 3-6x más chico que lo medido sobre 1 año (que daba +1.24% a 20d). Motivo: el umbral >1.97 no es un extremo — captura 43% del universo (mediana real 1.93), y 40% del panel tiene RR=2.00 exacto (tope de diseño del cálculo de niveles, no un valor emergente). Corregido por ventanas independientes (antes imposible con 1 año): a 20d hay 84 ventanas efectivas, t cae a 1.25 — no alcanza para declarar el efecto sólido incluso con toda la historia disponible. Sigue sin cubrir el costo de comisión (+0.18% vs 1.8%).",
    uso: "No usar como señal de entrada ni filtro. Único uso justificado: tilt de ranking dentro de las oportunidades ya filtradas por el sistema (ordenar por R/R como criterio secundario) — ahí no compite contra costos y la dirección del efecto es correcta, aunque modesta.",
  },
  {
    fecha: "2026-08-25",
    regla: "ARS bajo su SMA200 (>2.7% por debajo) predice exceso positivo hasta 20 días, se invierte a partir de 30",
    estado: "en observación — usar sólo a corto plazo, NO extender el horizonte",
    descripcion: "Reversión a la media en Merval: papeles por debajo de su media de 200 ruedas rebotan a 5-20 días.",
    evidencia: "Exceso OOS con control de volatilidad: 5d +0.45%, 10d +0.51%, 20d +0.68% — pero 30d -0.50% y 45d -2.43% (t=-5.93). El rebote de hasta 20 días se devuelve entero y se pasa de largo. 10/11 meses positivos a 10 días.",
    uso: "Tilt de ranking a corto plazo únicamente (5-20 días). Nunca usar como señal de horizonte largo — ahí es la regla contraria.",
  },
];

// ── TASAS BASE HISTÓRICAS POR BANDA DE RSI ──
//
// Medidas sobre 153 activos × 10 años (363.746 observaciones diarias),
// excluyendo las 5 series con datos degradados. Para cada banda de RSI:
// probabilidad de que en los próximos 1-4 días haya un salto de +4% o
// una caída de -4%, y retorno medio a 4 días.
//
// IMPORTANTE — cómo leer esto: la relación NO es lineal, es una U. Los
// dos extremos (RSI<30 y RSI>70) preceden movimientos más grandes en
// ambas direcciones; el medio (45-55) es la zona más quieta. Eso es
// principalmente un efecto de volatilidad, no una señal direccional.
//
// Esto es estadística descriptiva, NO una regla de trading: el spread
// entre RSI bajo y alto promedia 0.82 pp, contra un costo de operación
// de 1.2-1.8% ida y vuelta. Sirve para saber dónde está parado el
// activo en términos históricos, no para decidir una entrada.
//
// El efecto sí pasa el test de consistencia temporal (RSI bajo le gana
// a RSI alto en 81 de 118 meses = 69%, sobre el umbral de 65%), pero
// viene flojo en los últimos 12 meses (5 de 12 = 42%).
const RSI_TASAS_BASE = [
  { lo:  0, hi: 30, n: 11849, pUp: 33.0, pDn: 25.0, fwd4: 1.18, wr: 55.3 },
  { lo: 30, hi: 40, n: 45081, pUp: 25.9, pDn: 21.2, fwd4: 0.63, wr: 54.1 },
  { lo: 40, hi: 45, n: 42281, pUp: 23.6, pDn: 20.2, fwd4: 0.48, wr: 52.6 },
  { lo: 45, hi: 50, n: 51463, pUp: 21.6, pDn: 19.7, fwd4: 0.37, wr: 51.5 },
  { lo: 50, hi: 55, n: 53969, pUp: 20.2, pDn: 19.0, fwd4: 0.33, wr: 51.7 },
  { lo: 55, hi: 60, n: 49975, pUp: 19.2, pDn: 17.8, fwd4: 0.38, wr: 52.6 },
  { lo: 60, hi: 70, n: 72056, pUp: 18.8, pDn: 15.8, fwd4: 0.59, wr: 53.6 },
  { lo: 70, hi:101, n: 37072, pUp: 23.1, pDn: 17.1, fwd4: 1.01, wr: 54.1 },
];
const RSI_BASE_PROM_UP = 21.4;  // promedio general de P(+4%) en el universo

// ── VOLUMEN RELATIVO vs MEDIA PROPIA DEL ACTIVO ──
// Media anual de vol_24h por activo (152 activos, ultimo año). Cada activo
// tiene su nivel normal de actividad: comparar contra un umbral global
// mezclaba peras con manzanas. Validado: cuando vol_24h supera la media
// PROPIA del activo, sube 2.66pp mas seguido al dia siguiente, controlando
// por fecha Y por volatilidad (t=3.14).
// SALVEDAD: efecto chico (WR ~47%->50%), NO cubre costos de operar
// (1.2-1.8%), y solo 51%% de las comparaciones dan positivo — el promedio
// lo empujan pocos casos grandes, no es consistente. Es contexto, no señal.
const VOL_MEDIA_ANUAL = {"AAL":0.921,"AAPL":1.138,"ABBV":1.507,"ABT":1.434,"ADBE":1.187,"AGRO":1.452,"ALUA":1.314,"AMD":0.773,"AMT":1.618,"AMZN":1.089,"AUSO":1.481,"AVGO":1.085,"AXP":1.5,"BA":1.172,"BABA":0.964,"BAC":1.244,"BHIP":1.195,"BLK":1.642,"BMA":1.26,"BOLT":1.483,"BPAT":1.273,"BRK-B":1.396,"BYMA":1.498,"C":1.28,"CADO":1.009,"CAH":1.738,"CAPX":1.37,"CAT":1.282,"CECO2":1.939,"CELU":1.275,"CEPU":1.323,"CGPA2":1.275,"COF":1.465,"COIN":0.937,"COME":1.519,"COST":1.397,"CRM":1.16,"CTIO":1.14,"CVH":1.735,"CVS":1.568,"CVX":1.257,"DAL":1.389,"DGCU2":1.397,"DHR":1.534,"DIA":0.917,"DIS":1.278,"EDN":1.454,"F":0.975,"FDX":1.798,"FERR":1.383,"FIPL":1.208,"GBAN":1.359,"GCLA":1.228,"GE":1.33,"GGAL":1.212,"GILD":1.582,"GLD":0.889,"GLOB":1.701,"GM":1.42,"GOOGL":1.039,"GRIM":1.341,"GS":1.213,"HARG":1.311,"HD":1.419,"HON":1.654,"IBM":1.286,"INTC":0.749,"INTR":0.873,"INVJ":1.205,"IRSA":1.441,"ISRG":1.415,"IWM":1,"JNJ":1.444,"JPM":1.254,"KO":1.408,"LEDE":1.332,"LLY":1.207,"LOMA":1.407,"LONG":1.23,"LOW":1.667,"MA":1.383,"MCD":1.394,"MELI":1.323,"META":1.019,"METR":1.465,"MOLI":1.362,"MORI":1.124,"MRK":1.393,"MS":1.447,"MSFT":1.114,"MU":0.769,"NDAQ":1.755,"NET":1.382,"NFLX":1.143,"NKE":1.17,"NOW":1.095,"NVDA":0.858,"OEST":1.697,"ORCL":0.857,"OXY":1.211,"PAMP":1.187,"PATA":1.416,"PBR":1.148,"PEP":1.509,"PFE":1.119,"PG":1.358,"PLTR":0.833,"PYPL":1.21,"QCOM":1.338,"QQQ":0.901,"RBLX":1.395,"REGN":1.53,"RIGO":1.188,"ROSE":0.922,"RTX":1.527,"SAMI":1.392,"SBUX":1.539,"SCHW":1.414,"SEMI":1.186,"SHOP":1.382,"SLB":1.281,"SLV":0.743,"SNOW":1.289,"SPOT":1.41,"SPY":1.299,"SUPV":1.343,"T":1.353,"TECO2":1.458,"TGNO4":1.448,"TGSU2":1.288,"TGT":1.484,"TLT":1.248,"TMO":1.221,"TMUS":1.575,"TSLA":0.663,"TXAR":1.578,"TXN":1.659,"UAL":1.402,"UBER":1.134,"UNH":1.075,"UPS":1.381,"V":1.344,"VALO":1.322,"VIST":1.211,"VZ":1.382,"WFC":1.389,"WMT":1.385,"XLE":1.261,"XLF":1.438,"XLK":1.176,"XOM":1.506,"YPFD":1.22};

// Devuelve la desviacion del volumen actual vs la media propia del activo.
// positivo = mas actividad que lo normal para ESE activo; negativo = menos.
// ── MEDIA MÓVIL DE vol_24h (6 meses, hasta el día ANTERIOR) ────────────
//
// Reemplaza a VOL_MEDIA_ANUAL (constante fija por activo). Dos razones:
//
// 1. SESGO DE ANTICIPACIÓN. La constante era la media del vol_24h del
//    último año COMPLETO — incluía días posteriores a la fecha evaluada.
//    Al medir un patrón sobre el pasado, el indicador ya "sabía" el futuro.
//    Medido: el hallazgo "COMPRA + volumen muerto → cae" daba -0.32% a 10d
//    con la constante y +0.42% con la media móvil. Era el sesgo, no señal.
//
// 2. UN AÑO DILUYE. Una racha fuerte de volumen levanta la media anual y
//    tapa las subas siguientes. Con ventana móvil de 6 meses el umbral
//    acompaña al régimen del activo: el corte del tercil inferior pasó de
//    -0.33 (anual) a -0.11 (móvil 6m).
//
// La ventana termina en la ÚLTIMA BARRA DE LA RUEDA ANTERIOR: la barra
// evaluada nunca entra en su propia media.
//
// Validado sobre 26.591 obs (157 tickers): correlación 0.972 con la
// constante pero el signo cambia en 24.1% de los casos, y el t mejora en
// 3 de las 4 celdas (señal × signo de volumen), controlando por fecha,
// moneda y volatilidad.
const VOL_MM_RUEDAS = 126;   // ~6 meses de ruedas
const VOL_MM_MIN    = 60;    // mínimo para no descartar la mitad del panel

function volMediaMovil(data, ruedas = VOL_MM_RUEDAS) {
  if (!Array.isArray(data) || data.length < 48) return null;
  const n = data.length - 1;

  // Índices de la última barra de cada rueda, de atrás hacia adelante.
  const cierres = [];
  for (let i = n; i > 0 && cierres.length <= ruedas + 1; i--) {
    if (data[i].date !== data[i - 1].date) cierres.push(i - 1);
  }
  if (!cierres.length) return null;

  const fin = cierres[0];                       // última barra de la rueda anterior
  const ini = cierres.length > ruedas
    ? cierres[ruedas] + 1                       // ventana llena de 126 ruedas
    : 0;                                        // toda la historia disponible
  const nRuedas = Math.min(cierres.length, ruedas);
  if (nRuedas < VOL_MM_MIN) return null;

  // vol_24h por barra = volumen / media de las 24 barras que la cierran.
  // Ventana deslizante para no recalcular la suma en cada paso.
  const desde = Math.max(0, ini - 23);
  let suma = 0, acum = 0, cuenta = 0;
  for (let i = desde; i <= fin; i++) {
    suma += data[i].volume || 0;
    if (i - desde >= 24) suma -= data[i - 24].volume || 0;
    const cant = Math.min(i - desde + 1, 24);
    if (i >= ini && cant > 0) {
      const media24 = suma / cant;
      if (media24 > 0) { acum += (data[i].volume || 0) / media24; cuenta++; }
    }
  }
  if (cuenta < 24 * VOL_MM_MIN * 0.3) return null;
  return { media: acum / cuenta, ruedas: nRuedas, barras: cuenta };
}

// Devuelve la desviación del volumen actual vs la media MÓVIL del activo.
// positivo = más actividad que lo normal para ESE activo en los últimos
// 6 meses; negativo = menos.
//
// `sig` es el objeto de combinedSignal(), que ya trae la media móvil
// calculada sobre la serie. Si no está disponible (serie corta, menos de
// 60 ruedas), cae a VOL_MEDIA_ANUAL y lo marca en `fuente`.
function volVsMedia(ticker, vol24h, sig) {
  if (vol24h == null || !isFinite(vol24h)) return null;

  const movil = sig && sig.vol_media_mov;
  if (movil != null && isFinite(movil) && movil > 0) {
    return {
      media: +movil.toFixed(2),
      dif:   +(vol24h - movil).toFixed(2),
      pct:   +((vol24h / movil - 1) * 100).toFixed(0),
      fuente: "movil6m",
      ruedas: sig.vol_mm_ruedas ?? null,
    };
  }

  const tk = (ticker || "").replace(".BA", "");
  const media = VOL_MEDIA_ANUAL[tk];
  if (media == null) return null;
  return {
    media,
    dif: +(vol24h - media).toFixed(2),
    pct: +((vol24h / media - 1) * 100).toFixed(0),
    fuente: "anual",
    ruedas: null,
  };
}

function bandaRSI(rsi) {
  if (rsi == null || Number.isNaN(rsi)) return null;
  return RSI_TASAS_BASE.find(b => rsi >= b.lo && rsi < b.hi) || null;
}

// ── TASAS BASE HISTÓRICAS POR PATRÓN DE VELA ──
//
// Mismo método que RSI_TASAS_BASE: 153 activos × 10 años (363.746 obs).
// `clave` mapea con los nombres que devuelve detectCandlePattern().
//
// RESULTADO GENERAL: de 15 patrones medidos, solo 2 sobreviven corrección
// por comparaciones múltiples (Bonferroni) + control de volatilidad, y
// solo 1 pasa además el test de consistencia mensual. Los efectos son de
// 0.2-0.3 pp contra un costo de operar de 1.2-1.8% — ninguno alcanza
// como regla de entrada.
//
// Lo más importante: las etiquetas tradicionales alcista/bajista NO se
// sostienen. "Martillo" (reversión alcista de manual) es el peor de los
// 15. "Tres cuervos" (bajista de manual) es el único que pasa todos los
// tests, y predice retornos POSITIVOS. Por eso el panel muestra la tasa
// base medida al lado de la etiqueta, en vez de repetir el folklore.
const VELAS_TASAS_BASE = [
  { clave:"Marubozu Alcista", etiqueta:"alcista", n:10272, pUp:26.2, pDn:19.0, fwd4:1.04, wr:53.1, exc:+0.319, t:2.80, cons:60, veredicto:"parcial" },
  { clave:"Engulfing Alcista", etiqueta:"alcista", n:12901, pUp:22.6, pDn:18.4, fwd4:0.72, wr:53.5, exc:+0.052, t:0.35, cons:null, veredicto:"sin efecto" },
  { clave:"Estrella Fugaz",   etiqueta:"bajista", n: 9914, pUp:23.5, pDn:18.8, fwd4:0.70, wr:54.4, exc:+0.176, t:1.77, cons:null, veredicto:"sin efecto" },
  { clave:"3 Cuervos",        etiqueta:"bajista", n:43753, pUp:24.4, pDn:20.5, fwd4:0.66, wr:53.7, exc:+0.166, t:3.40, cons:70, veredicto:"pasa" },
  { clave:"Engulfing Bajista",etiqueta:"bajista", n:15983, pUp:22.2, pDn:19.6, fwd4:0.54, wr:53.7, exc:-0.021, t:-0.27, cons:null, veredicto:"sin efecto" },
  { clave:"3 Velas Alcistas", etiqueta:"alcista", n:42446, pUp:19.8, pDn:17.1, fwd4:0.52, wr:53.1, exc:-0.058, t:-0.95, cons:null, veredicto:"sin efecto" },
  { clave:"Doji",             etiqueta:"neutral", n:38506, pUp:21.6, pDn:18.7, fwd4:0.52, wr:52.5, exc:-0.019, t:-0.39, cons:null, veredicto:"sin efecto" },
  { clave:"Martillo",         etiqueta:"alcista", n:14356, pUp:21.2, pDn:19.8, fwd4:0.35, wr:50.4, exc:-0.153, t:-1.12, cons:null, veredicto:"sin efecto" },
];
const VELAS_BASELINE = { pUp:21.8, pDn:18.7, fwd4:0.55, wr:53.0 };

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
// ── CORRELACIÓN ENTRE ACTIVOS — CALCULADA, NO DECLARADA ──────────
//
// Antes había 4 grupos escritos a mano. Verificados contra 250 ruedas
// de datos reales, tenían tres problemas:
//
//   1. "YPF" no existe en el universo (el ticker es YPFD) — el grupo de
//      energía Merval estaba parcialmente roto.
//   2. Cubrían 16 de los 104 pares con correlación >0.70 del universo.
//      El 85% de las correlaciones altas quedaba sin contemplar
//      (CRM-NOW 0.80, MU-XLK 0.75, AMD-XLK 0.73, NVDA-XLK 0.72…).
//   3. El grupo tech se declaraba en 0.68-0.70 y su correlación real
//      es 0.465 — sobreestimado.
//
// Ahora se calcula de la serie diaria. Ventana de 250 ruedas terminando
// en la rueda anterior, coherente con la norma del proyecto (estimar de
// los datos, ventana dinámica, sin mirar el día en curso).
//
// Los grupos declarados se conservan sólo como semilla de respaldo por
// si la serie diaria no estuviera disponible.
const CORRELATION_GROUPS_FALLBACK = [
  ["BAC","C","WFC","AXP"],
  ["AMZN","SPY","NVDA","GOOGL"],
  ["GGAL","BMA","SUPV","VALO"],
  ["YPFD","PAMP","CEPU","TGSU2","TGNO4"],   // YPF → YPFD (estaba mal)
];

const CORR_UMBRAL = 0.70;   // umbral de "misma apuesta"
const CORR_WIN    = 250;
let _corrCache = null;

// Devuelve { "TICKER": { "OTRO": 0.83, ... } } sólo para pares > umbral
function matrizCorrelacion() {
  if (_corrCache) return _corrCache;
  const out = {};
  try {
    const src = DATA_MOD?.CSV_DATA_DAILY_RAW || {};
    const R = {};
    for (const [tk, bars] of Object.entries(src)) {
      if (!bars || bars.length < CORR_WIN + 10) continue;
      const r = [];
      // termina en la rueda ANTERIOR
      for (let i = bars.length - CORR_WIN; i < bars.length - 1; i++) {
        const c = bars[i]?.c, p = bars[i-1]?.c;
        if (c > 0 && p > 0) r.push(Math.log(c / p));
      }
      if (r.length >= CORR_WIN * 0.9) R[tk] = r;
    }
    const ks = Object.keys(R);
    // pre-calcular media y desvío
    const st = {};
    for (const k of ks) {
      const v = R[k], n = v.length;
      const m = v.reduce((a,x)=>a+x,0)/n;
      st[k] = { m, sd: Math.sqrt(v.reduce((a,x)=>a+(x-m)**2,0)/n), n };
    }
    for (let i = 0; i < ks.length; i++) {
      for (let j = i+1; j < ks.length; j++) {
        const a = ks[i], b = ks[j];
        const va = R[a], vb = R[b];
        const n = Math.min(va.length, vb.length);
        if (n < CORR_WIN * 0.9) continue;
        const xa = va.slice(-n), xb = vb.slice(-n);
        const ma = xa.reduce((s,x)=>s+x,0)/n, mb = xb.reduce((s,x)=>s+x,0)/n;
        let sxy=0, sx=0, sy=0;
        for (let k = 0; k < n; k++) { const da=xa[k]-ma, db=xb[k]-mb; sxy+=da*db; sx+=da*da; sy+=db*db; }
        if (!(sx>0) || !(sy>0)) continue;
        const c = sxy / Math.sqrt(sx*sy);
        if (c >= CORR_UMBRAL) {
          (out[a] ||= {})[b] = +c.toFixed(3);
          (out[b] ||= {})[a] = +c.toFixed(3);
        }
      }
    }
  } catch(e) { /* cae al fallback */ }
  // respaldo si no se pudo calcular nada
  if (!Object.keys(out).length) {
    for (const g of CORRELATION_GROUPS_FALLBACK)
      for (const a of g) for (const b of g)
        if (a !== b) (out[a] ||= {})[b] = 0.75;
  }
  _corrCache = out;
  return out;
}

function deduplicateCorrelated(results) {
  // De cada conjunto de señales correlacionadas entre sí, se mantiene la
  // de mayor score y las demás se degradan: son la MISMA apuesta, y
  // tomarlas todas concentra riesgo sin diversificar.
  const M = matrizCorrelacion();
  const activos = results.filter(r => r.sig && r.sig.sig !== "NEUTRAL" && r.sig.above_p80);
  const perder = new Map();   // ticker degradado → con quién correlaciona
  for (const r of activos) {
    const vecinos = M[r.ticker]; if (!vecinos) continue;
    for (const o of activos) {
      if (o === r) continue;
      const c = vecinos[o.ticker]; if (c == null) continue;
      // misma dirección: sólo entonces es la misma apuesta
      const dirR = r.sig.sig.includes("COMPRA") ? 1 : -1;
      const dirO = o.sig.sig.includes("COMPRA") ? 1 : -1;
      if (dirR !== dirO) continue;
      const convR = Math.abs((r.sig.final_sc ?? 50) - 50);
      const convO = Math.abs((o.sig.final_sc ?? 50) - 50);
      if (convO > convR || (convO === convR && o.ticker < r.ticker)) {
        perder.set(r.ticker, { con: o.ticker, corr: c });
        break;
      }
    }
  }
  return results.map(r => {
    const d = perder.get(r.ticker);
    if (!d || !r.sig) return r;
    return { ...r, sig: { ...r.sig, sig: "NEUTRAL", above_p80: false,
                          corr_dup: d.con, corr_val: d.corr } };
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
// NEUTRALIZADO (2026-08-26). Los factores estaban invertidos respecto a
// los datos, y el efecto ni siquiera es estable.
//
// MEDIDO sobre 10 años de diarias (384.419 obs):
//   ranking DECLARADO: Martes > Miércoles > Jueves > Lunes > Viernes
//   ranking REAL     : Miércoles > Viernes > Martes > Lunes > Jueves
//
// El comentario original decía "Viernes: cierre de posiciones, evitar
// señales nuevas" y le ponía el factor más bajo (0.90). El viernes es el
// SEGUNDO MEJOR día (+0.1807%, t=13.92). El jueves, con factor 1.03, es
// el peor (+0.1102%). Viernes y jueves estaban literalmente invertidos.
//
// Y el efecto no es estable: partiendo la muestra 60/40 el orden se da
// vuelta (IS: Vie>Mie>Mar>Jue>Lun · OOS: Lun>Mie>Vie>Jue>Mar). El lunes
// pasa de último a primero, el martes de tercero a último. No hay señal
// que capturar, sólo ruido con estructura aparente.
//
// Se deja en 1.0 para todos: neutro, sin tocar el score.
const DOW_FACTOR = {
  0: 1.00,  // Lunes
  1: 1.00,  // Martes
  2: 1.00,  // Miércoles
  3: 1.00,  // Jueves
  4: 1.00,  // Viernes
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
  // DESACTIVADO: mismo problema que adaptiveScoreAdj y TICKER_CONFIDENCE.
  //
  // update_data.py probaba W ∈ {5,7,10,14} sobre la serie COMPLETA de cada
  // ticker y se quedaba con el de mejor win rate. Elegir el mejor de 4 sobre
  // los mismos datos que después se usan infla el resultado por construcción.
  //
  // MEDIDO (157 tickers, 10 años, split temporal 60/40, replicando
  // backtest_w() exacto del script):
  //
  //   W elegido por activo, fuera de muestra : 47.48%
  //   W global fijo = 5, fuera de muestra    : 47.71%
  //   diferencia: -0.23pp, t = -2.62
  //
  // No es que no ayude: PERJUDICA de forma significativa. Y el W propio le
  // gana al global en 8 de 157 activos (5%), que es exactamente lo esperable
  // por azar — no hay W óptimo por activo, la elección es ruido.
  //
  // El orden de los W es idéntico dentro y fuera de muestra
  // (5:47.7% > 7:46.3% > 10:44.3% > 14:42.5%): W=5 es mejor para todos.
  //
  // Coherente con la norma del proyecto (estimar por activo con encogimiento):
  // acá el peso correcto para la estimación propia es 0, porque la varianza
  // entre activos es toda ruido de muestreo. La norma bien aplicada llega
  // sola a "usá el global"; dynParams usaba peso 1.0 sin justificarlo.
  //
  // ⚠ Nota aparte: el win rate ronda 47.5% en TODOS los W — la estrategia
  // que backtest_w() usa para elegir (cruce SMA20/50, stop 1.5×ATR,
  // TP 2.5×ATR) pierde más veces de las que gana. Los `conf` y `p80adj`
  // que dynParams deriva de ese win rate están construidos sobre esa base.
  return globalW;
}

// ── COSTOS DE TRANSACCIÓN (round-trip, broker PPI) ──
const COSTO_MERVAL = 1.2;  // % ida+vuelta acciones locales
const COSTO_CEDEAR = 1.8;  // % ida+vuelta CEDEARs (incluye spread)

// ══════════════════════════════════════════════════════════════
// NIVELES ESTRUCTURALES — soportes/resistencias reales del precio
// Usados para calcular un R/R genuino (no un múltiplo fijo de ATR)
// ══════════════════════════════════════════════════════════════
// ── SEMÁFORO VISUAL — recuadro pintado (no solo texto) ──
// Se usa en todos los paneles de indicadores técnicos y de validación:
// da fondo teñido + borde del mismo color, para que el estado se lea
// de un vistazo sin tener que leer el número.
// ── ESTADO DE MERCADO ──
//
// El sistema calculaba señales igual a las 3am que a las 2pm, sin
// distinguir si hay sesión abierta o si el último dato es de hace
// 3 días (fin de semana). Esto es lo que hacía confuso el caso de
// AAL: la señal cambió porque HUBO sesión de por medio, no porque
// el sistema "detectara tarde" — pero no había forma de verlo en
// la UI. Esta función expone ese contexto explícitamente.
//
// Horario unificado ~11:00-17:00 ART cubre tanto ByMA/Merval (11-17
// aprox.) como NYSE/Nasdaq (9:30-16:00 ET ≈ 10:30-17:00 ART en
// horario de verano EEUU). No son exactamente iguales, pero para
// mostrar "¿podría haber operado hoy?" alcanza con el solapamiento.
// ── CALENDARIO FOMC ──
//
// A diferencia del calendario de balances (que depende de una API que
// se rompe seguido), las fechas de reunión de la Fed son públicas y
// fijas con meses de anticipación — no hace falta scraping, se
// hardcodean directo del calendario oficial (federalreserve.gov).
//
// Por qué esto SÍ se justifica y el patrón de MACD/volumen que se
// descartó (ver docs/hallazgos.md) no: la "pre-FOMC announcement
// drift" es un efecto documentado en la literatura académica (Lucca &
// Moench, 2015) — no es un patrón inventado mirando 5 casos. Aun así,
// esto se muestra como CONTEXTO DE RIESGO DE EVENTO, no como señal de
// compra/venta: no convierte "faltan pocos días para la Fed" en una
// recomendación direccional, solo advierte que el movimiento de esos
// días puede no tener nada que ver con el análisis técnico.
const FOMC_2026 = [
  "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
  "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
]; // fecha del statement (2do día de cada reunión), 14:00 ET

function estadoFOMC(ahoraART) {
  const d = ahoraART || new Date(new Date().toLocaleString("en-US", {timeZone:"America/Argentina/Buenos_Aires"}));
  const hoyStr = d.toISOString().slice(0,10);
  const futuras = FOMC_2026.filter(f => f >= hoyStr);
  const pasadas = FOMC_2026.filter(f => f < hoyStr);
  if (!futuras.length) return { proxima: null, dias: null, ultima: pasadas[pasadas.length-1] || null };
  const proxima = futuras[0];
  const dias = Math.round((new Date(proxima) - new Date(hoyStr)) / 86400000);
  return { proxima, dias, ultima: pasadas[pasadas.length-1] || null };
}

function estadoMercado(ahoraART) {
  const d = ahoraART || new Date(new Date().toLocaleString("en-US", {timeZone:"America/Argentina/Buenos_Aires"}));
  const dow = d.getDay(); // 0=domingo, 6=sabado
  const horaDecimal = d.getHours() + d.getMinutes()/60;
  const esFinde = dow === 0 || dow === 6;
  const enHorario = horaDecimal >= 11 && horaDecimal < 17;
  const abierto = !esFinde && enHorario;

  let proximaApertura;
  if (abierto) {
    proximaApertura = null;
  } else {
    const next = new Date(d);
    if (esFinde || horaDecimal >= 17) {
      // saltar al proximo dia habil
      do { next.setDate(next.getDate()+1); } while (next.getDay()===0 || next.getDay()===6);
    }
    next.setHours(11,0,0,0);
    proximaApertura = next;
  }

  return {
    abierto,
    esFinde,
    horaART: d.getHours()+':'+String(d.getMinutes()).padStart(2,'0'),
    proximaApertura,
    mensaje: abierto
      ? "Mercado abierto — datos en vivo"
      : esFinde
      ? "Fin de semana — sin sesión, datos del último cierre hábil"
      : horaDecimal < 11
      ? "Pre-apertura — el mercado abre ~11:00 ART"
      : "Mercado cerrado — sesión de hoy finalizada",
  };
}

// Antigüedad del último dato embebido, en términos humanos.
function antiguedadDato(fechaISO, ahoraART) {
  if (!fechaISO) return { minutos: null, mensaje: "sin dato" };
  const ahora = ahoraART || new Date(new Date().toLocaleString("en-US", {timeZone:"America/Argentina/Buenos_Aires"}));
  const dato = new Date(fechaISO);
  const minutos = Math.round((ahora - dato) / 60000);
  if (minutos < 0) return { minutos, mensaje: "dato futuro (?)" };
  if (minutos < 60) return { minutos, mensaje: `hace ${minutos} min` };
  if (minutos < 60*24) return { minutos, mensaje: `hace ${(minutos/60).toFixed(1)}h` };
  return { minutos, mensaje: `hace ${Math.round(minutos/60/24)}d` };
}

// ── CALIDAD DE LA SERIE DE PRECIOS ──
//
// Detecta series degradadas: papeles ilíquidos del Merval donde el
// precio queda congelado por falta de operaciones, no por estabilidad
// real. El motor técnico no distingue "el precio no se movió porque
// nadie operó" de "el precio no se movió porque hay equilibrio", y
// produce señales con confianza alta sobre ruido.
//
// Caso real que motivó esto (2026-08-03): POLL mostraba COMPRA FUERTE
// con CONF 100% sin haber operado desde el 31/7, con 63% de días sin
// volumen. GAMI tuvo el precio clavado en 15.50 durante 3 meses con
// volumen 0 y después un único print de 404 (+2506%) con volumen 1906.
//
// Devuelve nivel: "ok" | "dudosa" | "degradada"
function calidadSerie(bars, lookback = 100) {
  if (!bars || bars.length < 20) return { nivel: "degradada", motivo: "serie muy corta", pctSinVol: 1, pctCongelado: 1 };
  const ult = bars.slice(-lookback);
  const n = ult.length;
  const sinVol = ult.filter(b => !b.volume).length;
  let congelado = 0;
  for (let i = 1; i < n; i++) if (ult[i].close === ult[i-1].close) congelado++;
  const pctSinVol = sinVol / n;
  const pctCongelado = congelado / (n - 1);

  // Barras desde la última con volumen real
  let stale = 0;
  for (let i = n - 1; i >= 0 && !ult[i].volume; i--) stale++;

  const motivos = [];
  if (pctSinVol > 0.30) motivos.push(`${(pctSinVol*100).toFixed(0)}% sin volumen`);
  if (pctCongelado > 0.40) motivos.push(`${(pctCongelado*100).toFixed(0)}% precio congelado`);
  if (stale > 5) motivos.push(`${stale} barras sin operar`);

  let nivel = "ok";
  if (pctSinVol > 0.30 || pctCongelado > 0.50 || stale > 10) nivel = "degradada";
  else if (pctSinVol > 0.15 || pctCongelado > 0.30 || stale > 5) nivel = "dudosa";

  return { nivel, motivo: motivos.join(" · "), pctSinVol, pctCongelado, stale };
}

// Nota colapsable. Los avisos metodológicos (por qué una señal no es
// confiable, qué mide realmente un indicador) son necesarios pero
// ocupaban párrafos enteros en pantalla. Acá quedan a un toque de
// distancia sin desaparecer: el dato importante se ve siempre, el
// desarrollo queda plegado.
function Nota({ titulo, color = "#ff9040", children }) {
  const [abierto, setAbierto] = useState(false);
  return (
    <div style={{ marginTop: "6px" }}>
      <button onClick={() => setAbierto(a => !a)}
        style={{ background: "transparent", border: "none", padding: "2px 0", cursor: "pointer",
                 fontSize: "7px", color, fontFamily: "inherit", letterSpacing: ".05em" }}>
        {abierto ? "▾" : "▸"} {titulo}
      </button>
      {abierto && (
        <div style={{ ...semBox(color, "10"), padding: "7px 8px", marginTop: "3px" }}>
          <div style={{ fontSize: "7px", color: "#b0d4e8", lineHeight: 1.7 }}>{children}</div>
        </div>
      )}
    </div>
  );
}

function semBox(color, alpha = "1f") {
  return {
    background: `${color}${alpha}`,
    border: `1px solid ${color}70`,
    borderRadius: "4px",
  };
}
// Clasifica un valor en verde/amarillo/rojo dados los cortes [malo, bueno].
// higherIsBetter=false invierte el sentido (ej. drawdown, PBO: menos es mejor).
function semaforo(valor, corteRojo, corteVerde, higherIsBetter = true) {
  if (valor == null || Number.isNaN(valor)) return "#4a7a9b"; // gris: sin dato
  const enRango = higherIsBetter ? valor >= corteVerde : valor <= corteVerde;
  const enMalo  = higherIsBetter ? valor <  corteRojo  : valor >  corteRojo;
  return enRango ? "#00ff88" : enMalo ? "#ff3355" : "#ffd700";
}

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


function combinedSignal(data, W=7, allData=null, _sinTendencia=false) {
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
  const volMM = volMediaMovil(data);
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
    // ── CORTE DE RECURSIÓN ──────────────────────────────────────────
    // combinedSignal se llama a sí misma para calcular scoreTrend. Sin el
    // flag, esa llamada vuelve a hacer lo mismo hasta llegar a 60 barras:
    // con n barras son ~n/7 llamadas anidadas, cada una recalculando
    // findStructuralLevels() completo sobre 1.680 barras.
    //
    // Medido: serie horaria (1.600 barras) ~35 ms; serie diaria (2.400
    // barras) 4.000-10.000 ms. Era la causa de que Quant Lab y Validación
    // congelaran la UI — los yields y el límite de 45 modelos trataban el
    // síntoma, no esto.
    //
    // scoreTrend sólo necesita UN paso hacia atrás (el score de hace 7
    // barras) para medir la deriva. Los niveles más profundos se calculaban
    // y se descartaban. Verificado: con y sin el corte, `rr` y `sig` dan
    // idénticos (AAPL a 2.400 barras diarias: rr 1.96 / VENTA FUERTE en
    // ambos casos). Costo: 10x menos.
    const prevSig = (!_sinTendencia && prevSlice.length>=60) ? combinedSignal(prevSlice, W, allData, true) : null;
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
    vol_media_mov: volMM ? +volMM.media.toFixed(3) : null,
    vol_mm_ruedas: volMM ? volMM.ruedas : null,
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


// ── VETO DE FIBONACCI: comprar sin soporte estructural cerca ──────
//
// Detecta compras donde el precio está en tierra de nadie: lejos de
// cualquier nivel de Fibonacci relevante, y el nivel más cercano es un
// soporte débil. Comprar ahí es comprar sin piso técnico.
//
// VALIDADO (156 tickers × 10 años, 63.659 obs, exceso a 20 días
// controlado por fecha, moneda y tercil de volatilidad):
//
//   condición: COMPRA + trend=ALCISTA + fibDist > 3.92% + zona 'soporte'
//   exceso -2.632%  t=-3.80   (n=424, WR 41.5%)
//   fuera de muestra -3.066%  t=-2.50   ← MÁS fuerte fuera que dentro
//   drop-one: sin el ticker más favorable (INTC) sigue en t=-2.99
//   consistencia: 8 de 8 años con exceso negativo
//   ARS -3.461% (t=-3.46) · USD -1.930% (t=-2.10)  ← funciona en ambos
//
// Es el único hallazgo de la sesión que funciona en los DOS mercados.
//
// Por qué el umbral de evidencia es más bajo que para una señal: como
// VETO no paga comisión — filtrar una compra es gratis. No hay que
// superar el 1.2-1.8% de costo, sólo hay que no equivocarse.
//
// El sesgo de supervivencia del universo juega A FAVOR acá: si
// estuvieran los papeles que se deslistaron, el veto sería aún más
// necesario (ver HALLAZGOS_DESCARTADOS).
//
// Cautela: 424 obs sobre 93 tickers, 45/93 con exceso negativo. Se
// apoya en pocos casos por activo, por eso NO cambia la señal a VENTA
// ni la elimina — sólo la degrada y la marca.
const FIB_DIST_LEJOS = 3.92;   // tercil superior de distancia al nivel

function vetoFibonacci(sig, data) {
  try {
    if (!sig || !data) return null;
    if (!(sig.sig || "").includes("COMPRA")) return null;
    if (sig.trend !== "ALCISTA") return null;
    const fib = calcFibonacci(data, 60);
    if (!fib || !fib.levels || !fib.levels.length) return null;
    const px = data[data.length - 1]?.close;
    if (!(px > 0)) return null;
    const cerca = [...fib.levels].sort(
      (a, b) => Math.abs(a.value - px) - Math.abs(b.value - px)
    )[0];
    if (!cerca) return null;
    const dist = Math.abs(cerca.value - px) / px * 100;
    if (dist <= FIB_DIST_LEJOS) return null;
    if (cerca.type !== "soporte") return null;   // sólo soporte débil
    return { dist: +dist.toFixed(2), nivel: cerca.label, zona: cerca.type };
  } catch (e) { return null; }
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

    // ── VETO DE FIBONACCI ──
    // Compra en tierra de nadie: lejos de cualquier nivel y con sólo un
    // soporte débil cerca. Medido -2.63% a 20d (8/8 años negativos).
    // Degrada, NO elimina: con 424 obs sobre 93 tickers la evidencia
    // alcanza para bajarle la prioridad, no para descartar la señal.
    const vetado = !!r.sig.vetoFib && sigStr.includes("COMPRA");
    if (vetado) {
      sigStr = "COMPRA";          // COMPRA FUERTE → COMPRA
    }

    // Una señal solo cuenta como oportunidad si NO es neutral
    // y no está vetada por falta de estructura de soporte
    const isOpportunity = above && sigStr !== "NEUTRAL" && !vetado;

    const sig = {
      ...r.sig,
      sig:           sigStr,
      vetoFib:       r.sig.vetoFib || null,
      vetado,
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
  // Tres cuervos (Three Black Crows) — 3 velas bajistas seguidas.
  // El manual lo llama continuación bajista, pero medido sobre 43.753
  // casos en 10 años es el ÚNICO patrón que pasa Bonferroni + control de
  // volatilidad + consistencia mensual (70%), y apunta al alza
  // (+0.166 pp de exceso). Por eso el desc no repite la etiqueta clásica.
  if (!isBull && p.close < p.open && pp.close < pp.open)
    patterns.push({ name:"3 Cuervos", type:"neutral", desc:"Medido: leve sesgo alcista posterior, contra lo que dice el manual" });
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
// ── PERSISTENCIA DIRECCIONAL (LMSW) ──────────────────────────────
//
// Estima si el retorno de HOY tiende a continuar o revertirse mañana,
// y cuánto lo modula el volumen. Basado en Llorente, Michaely, Saar y
// Wang (Review of Financial Studies, 2002): los retornos generados por
// reparto de riesgo revierten, los generados por trading especulativo
// (informado) continúan. El coeficiente C2 mide de qué lado está el
// papel HOY — y cambia con el tiempo dentro del mismo activo.
//
// Regresión sobre las últimas 250 ruedas, con r y V estandarizados
// (z-scores de la propia ventana, para que las escalas sean comparables):
//
//   z(r[t+1]) = c0 + c1·z(r[t]) + c2·z(r[t])·z(V[t])
//
// donde V = log(volumen) menos su media móvil de 200 ruedas — detrendado,
// así se adapta al régimen actual del papel y no a su historia completa.
//
// VALIDACIÓN (303.096 obs, 156 tickers, 10 años, todo OOS por construcción:
// el C2 de cada día se estima sólo con las 250 ruedas anteriores).
// Spread long-short a 1 día, exceso vs. universo del mismo día:
//
//   ARS: +0.248%  t=10.29   ✅ VALIDADO
//   USD: -0.002%  t=-0.16   ❌ SIN PODER PREDICTIVO
//
// El efecto es de MERVAL. En USD se probaron 6 cortes (iliquidez de
// Amihud, volumen en dólares, ETF vs acción, magnitud del movimiento,
// descomposición C1/C2, y esta versión normalizada) y ninguno pasa de
// |t|=1.05. No es que falte la variable: no hay señal. 5 de 9 años
// negativos. Por eso en USD se muestra con badge punteado.
//
// Ventana: 250 ruedas. Se probaron 100, 50 y 10 — empeoran de forma
// monótona (t 3.72 → 1.87 → 0.90 → no estimable). Acortar no capta
// mejor el régimen actual, capta más ruido: el desvío de C2 sube de
// 0.159 a 0.388 y el signo se vuelve MENOS estable, no más.
//
// ⚠ NO es señal de entrada: +0.248% diario contra 1.2% de comisión
// Merval. Es criterio de RANKING, que no paga comisión.
const _persCache  = new Map();   // resultado final por ticker+fecha
const _featCache  = new Map();   // series r[] y V[] por ticker (caras de calcular)
const _priorCache = new Map();   // prior global por fecha

const PERS_MA  = 200;   // detrend del volumen (ventana del paper)
const PERS_WIN = 400;   // ventana de regresión — ver nota de validación abajo

// Series derivadas de un activo: retorno log y volumen detrendado.
// Se cachean porque el detrend es O(n·200) y se pide muchas veces.
function _persFeatures(ticker) {
  if (_featCache.has(ticker)) return _featCache.get(ticker);
  let res = null;
  try {
    const d = serieLarga(ticker);
    if (d && d.length >= PERS_MA + PERS_WIN + 10) {
      const n = d.length;
      const r = new Array(n).fill(null), lv = new Array(n).fill(null), V = new Array(n).fill(null);
      for (let i=1;i<n;i++){ const c=d[i].close, p=d[i-1].close; if(c>0&&p>0) r[i]=Math.log(c/p)*100; }
      for (let i=0;i<n;i++){ const v=d[i].volume||0; lv[i]= v>0?Math.log(v):null; }
      // media móvil de 200 ruedas, sin incluir la barra evaluada
      let suma=0, cuenta=0;
      for (let j=0;j<PERS_MA;j++) if(lv[j]!=null){ suma+=lv[j]; cuenta++; }
      for (let i=PERS_MA;i<n;i++){
        if (cuenta>PERS_MA*0.7 && lv[i]!=null) V[i]=lv[i]-suma/cuenta;
        const sale=lv[i-PERS_MA], entra=lv[i];
        if (sale!=null){ suma-=sale; cuenta--; }
        if (entra!=null){ suma+=entra; cuenta++; }
      }
      res = { d, r, V, n };
    }
  } catch(e) { res = null; }
  if (_featCache.size > 200) _featCache.clear();
  _featCache.set(ticker, res);
  return res;
}

// OLS de 3 parámetros con errores estándar (hacen falta para el encogimiento)
function _ols3se(Y, X1, X2) {
  const n = Y.length; if (n < 60) return null;
  let s0=n,s1=0,s2=0,s11=0,s22=0,s12=0,sy=0,sy1=0,sy2=0;
  for (let i=0;i<n;i++){ const a=X1[i],b=X2[i],v=Y[i];
    s1+=a;s2+=b;s11+=a*a;s22+=b*b;s12+=a*b;sy+=v;sy1+=v*a;sy2+=v*b; }
  const M=[[s0,s1,s2],[s1,s11,s12],[s2,s12,s22]], B=[sy,sy1,sy2];
  const det = M[0][0]*(M[1][1]*M[2][2]-M[1][2]*M[2][1])
            - M[0][1]*(M[1][0]*M[2][2]-M[1][2]*M[2][0])
            + M[0][2]*(M[1][0]*M[2][1]-M[1][1]*M[2][0]);
  if (Math.abs(det)<1e-10) return null;
  const inv=[
    [(M[1][1]*M[2][2]-M[1][2]*M[2][1])/det,(M[0][2]*M[2][1]-M[0][1]*M[2][2])/det,(M[0][1]*M[1][2]-M[0][2]*M[1][1])/det],
    [(M[1][2]*M[2][0]-M[1][0]*M[2][2])/det,(M[0][0]*M[2][2]-M[0][2]*M[2][0])/det,(M[0][2]*M[1][0]-M[0][0]*M[1][2])/det],
    [(M[1][0]*M[2][1]-M[1][1]*M[2][0])/det,(M[0][1]*M[2][0]-M[0][0]*M[2][1])/det,(M[0][0]*M[1][1]-M[0][1]*M[1][0])/det]];
  const c=[0,0,0];
  for (let i=0;i<3;i++) c[i]=inv[i][0]*B[0]+inv[i][1]*B[1]+inv[i][2]*B[2];
  let sse=0; for(let i=0;i<n;i++){ const e=Y[i]-(c[0]+c[1]*X1[i]+c[2]*X2[i]); sse+=e*e; }
  const s2e=sse/(n-3);
  return { c, se1:Math.sqrt(Math.max(0,s2e*inv[1][1])), se2:Math.sqrt(Math.max(0,s2e*inv[2][2])) };
}

// Estimación cruda de un activo a una fecha. La ventana TERMINA EN LA RUEDA
// ANTERIOR: el último par usado es (r[n-2] → r[n-1]). Nada del día evaluado
// entra en los coeficientes.
function _persCruda(ticker, hastaFecha) {
  const F = _persFeatures(ticker); if (!F) return null;
  const { d, r, V } = F;
  let n = d.length - 1;
  if (hastaFecha) { n=-1; for (let i=0;i<d.length;i++) if (d[i].date <= hastaFecha) n=i; }
  if (n < PERS_MA + PERS_WIN + 2) return null;
  if (r[n]==null || V[n]==null) return null;
  const rw=[], vw=[];
  for (let j=n-PERS_WIN;j<=n-1;j++){ if(r[j]!=null) rw.push(r[j]); if(V[j]!=null) vw.push(V[j]); }
  if (rw.length < PERS_WIN*0.5 || vw.length < PERS_WIN*0.5) return null;
  const mR=rw.reduce((a,x)=>a+x,0)/rw.length, sR=Math.sqrt(rw.reduce((a,x)=>a+(x-mR)**2,0)/rw.length);
  const mV=vw.reduce((a,x)=>a+x,0)/vw.length, sV=Math.sqrt(vw.reduce((a,x)=>a+(x-mV)**2,0)/vw.length);
  if (!(sR>0)||!(sV>0)) return null;
  const zR=x=>(x-mR)/sR, zV=x=>(x-mV)/sV;
  const Y=[],X1=[],X2=[];
  for (let j=n-PERS_WIN;j<=n-2;j++){
    if (r[j]==null||r[j+1]==null||V[j]==null) continue;
    Y.push(zR(r[j+1])); X1.push(zR(r[j])); X2.push(zR(r[j])*zV(V[j]));
  }
  const o=_ols3se(Y,X1,X2); if(!o) return null;
  return { c0:o.c[0], c1:o.c[1], c2:o.c[2], se1:o.se1, se2:o.se2,
           zr:zR(r[n]), zv:zV(V[n]), retHoy:r[n], fecha:d[n].date, moneda:d[n].moneda };
}

// Prior global a una fecha: media de los coeficientes de todos los activos,
// más tau² (varianza REAL entre activos, descontando el error de estimación).
// Es lo que define cuánto peso merece cada activo por sí solo.
//
// Calcularlo cuesta ~1s (156 regresiones), así que se cachea por MES en vez
// de por día: el prior es un agregado de 156 activos sobre 400 ruedas y se
// mueve muy despacio, mientras que el Replay puede pedir decenas de fechas
// seguidas. Sin esto, cada paso del Replay pagaría el segundo completo.
function _persPrior(hastaFecha) {
  const key = hastaFecha ? String(hastaFecha).slice(0,7) : "ult";
  if (_priorCache.has(key)) return _priorCache.get(key);
  let res = null;
  try {
    const src = DATA_MOD?.CSV_DATA_DAILY_RAW || {};
    const est = [];
    for (const tk of Object.keys(src)) {
      const e = _persCruda(tk, hastaFecha);
      if (e && isFinite(e.c1) && isFinite(e.c2) && e.se2>0) est.push(e);
    }
    if (est.length >= 8) {
      const m0=est.reduce((a,e)=>a+e.c0,0)/est.length;
      const m1=est.reduce((a,e)=>a+e.c1,0)/est.length;
      const m2=est.reduce((a,e)=>a+e.c2,0)/est.length;
      const vt2=est.reduce((a,e)=>a+(e.c2-m2)**2,0)/Math.max(1,est.length-1);
      const vm2=est.reduce((a,e)=>a+e.se2*e.se2,0)/est.length;
      const vt1=est.reduce((a,e)=>a+(e.c1-m1)**2,0)/Math.max(1,est.length-1);
      const vm1=est.reduce((a,e)=>a+e.se1*e.se1,0)/est.length;
      res = { m0, m1, m2, tau1:Math.max(0,vt1-vm1), tau2:Math.max(0,vt2-vm2), n:est.length };
    }
  } catch(e) { res = null; }
  if (_priorCache.size > 40) _priorCache.clear();
  _priorCache.set(key, res);
  return res;
}

// ── PERSISTENCIA DIRECCIONAL (LMSW) ──────────────────────────────
//
// Estima si el retorno de HOY tiende a continuar o revertirse mañana.
// Basado en Llorente, Michaely, Saar y Wang (Review of Financial Studies,
// 2002): los retornos por reparto de riesgo revierten, los de trading
// informado continúan. El coeficiente C2 dice de qué lado está el papel
// HOY, y cambia con el tiempo dentro del mismo activo (GGAL: C2=-0.091
// en 2024, +0.019 en 2025).
//
//   z(r[t+1]) = c0 + c1·z(r[t]) + c2·z(r[t])·z(V[t])
//   V = log(volumen) − media móvil 200 ruedas (detrendado, adaptativo)
//   ventana 400 ruedas, dinámica, terminando en la RUEDA ANTERIOR
//
// ── POR ACTIVO CON ENCOGIMIENTO ──
// Los coeficientes se estiman POR ACTIVO y luego se encogen hacia el
// promedio global en proporción a su ruido: w = τ²/(τ²+se²), donde τ² es
// la varianza real entre activos y se² el error de estimación propio.
// Un activo con estimación precisa conserva su valor; uno ruidoso se
// acerca al global. Es lo que evita que "por activo" degenere en
// sobreajuste (el error de TICKER_CONFIDENCE y dynParams).
//
// VALIDACIÓN DEL MÉTODO (279.735 obs diarias, 156 tickers, 10 años,
// ~750 fechas fuera de muestra). Spread long-short a 1 día en ARS:
//
//   método        total    t        OOS      t
//   global       +0.081%   2.65    +0.257%  4.61
//   por activo   +0.240%   9.75    +0.346%  7.64
//   encogido     +0.225%   8.39    +0.367%  7.36   ← elegido
//
// Por activo TRIPLICA al global: evaluar en pool único tiraba dos tercios
// de la señal. Entre puro y encogido la diferencia es chica (el encogido
// gana fuera de muestra, pierde dentro); se elige encogido por robustez.
//
// VENTANA 400: se probaron 10/50/100/250/300/400. De 250 a 400 los
// resultados son casi iguales (t 9.75 / 10.36 / 9.84), pero el peso propio
// mediano sube de 0.29 a 0.48 — con 400 ruedas la estimación por activo es
// lo bastante precisa para merecer casi la mitad del peso. Por debajo de
// 250 se degrada rápido (t 1.87 a 100 ruedas, 0.90 a 50, no estimable a 10):
// acortar capta más ruido, no mejor régimen.
//
// ALCANCE: validado en MERVAL. En USD el efecto no existe con ningún método
// ni ventana (todos los |t| < 1.92, varios negativos) — se probaron además
// 6 cortes (Amihud, volumen en dólares, ETF vs acción, magnitud del
// movimiento, descomposición C1/C2, normalización). Badge punteado en USD.
//
// ⚠ NO es señal de entrada: +0.37% diario contra 1.2% de comisión Merval.
// Es criterio de RANKING, que no paga comisión.
//
// ⚠ MATIZ: el efecto NO es de volumen. El término C1 solo da t=10.38 en
// ARS (más que el modelo completo) y el término del volumen C2 aporta
// t=1.75. Lo que se descubrió es que Merval tiene momentum diario
// persistente (C1 medio +0.070 contra −0.006 en USD) y el marco LMSW lo
// capta de rebote. Por eso se llama "persistencia" y no "volumen".
function persistenciaDireccional(ticker, hastaFecha=null) {
  const key = ticker + "|" + (hastaFecha || "ult");
  if (_persCache.has(key)) return _persCache.get(key);
  let res = null;
  try {
    const e = _persCruda(ticker, hastaFecha);
    if (e) {
      const pr = _persPrior(hastaFecha);
      let c0=e.c0, c1=e.c1, c2=e.c2, w=1;
      if (pr) {
        const w2 = pr.tau2>0 ? pr.tau2/(pr.tau2 + e.se2*e.se2) : 0;
        const w1 = pr.tau1>0 ? pr.tau1/(pr.tau1 + e.se1*e.se1) : 0;
        c0 = w2*e.c0 + (1-w2)*pr.m0;
        c1 = w1*e.c1 + (1-w1)*pr.m1;
        c2 = w2*e.c2 + (1-w2)*pr.m2;
        w  = w2;
      }
      const pred = c0 + c1*e.zr + c2*e.zr*e.zv;
      res = {
        pred: +pred.toFixed(3),
        c1: +c1.toFixed(4),
        c2: +c2.toFixed(4),
        c2propio: +e.c2.toFixed(4),
        peso: +w.toFixed(2),            // cuánto pesa el activo vs el global
        regimen: c2 > 0 ? "continuacion" : "reversion",
        zRet: +e.zr.toFixed(2),
        zVol: +e.zv.toFixed(2),
        retHoy: +e.retHoy.toFixed(2),
        dir: pred > 0 ? "alza" : "baja",
        validado: e.moneda === "ARS",
        ventana: PERS_WIN,
        fecha: e.fecha,
      };
    }
  } catch(e) { res = null; }
  if (_persCache.size > 400) _persCache.clear();
  _persCache.set(key, res);
  return res;
}

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

// Estado del calendario de balances para un ticker.
//
// Devuelve SIEMPRE un estado explícito. Que no aparezca un aviso no debe
// poder confundirse con "no reporta pronto": mientras el calendario real
// esté incompleto, la ausencia de dato tiene que decirse, no callarse.
function estadoEarnings(ticker) {
  const tk  = (ticker || "").replace(".BA", "");
  const hoy = new Date();
  const dias = (a, b) => Math.round((new Date(a) - new Date(b)) / 86400000);

  const real = (typeof DATA_MOD !== "undefined" && DATA_MOD?.FXCA16_EARNINGS)
    ? DATA_MOD.FXCA16_EARNINGS[tk] : null;

  if (real && (real.prox || real.ultimo)) {
    return {
      estado: "real", tk,
      prox: real.prox || null,
      ultimo: real.ultimo || null,
      diasProx:   real.prox   ? dias(real.prox, hoy)   : null,
      diasUltimo: real.ultimo ? dias(hoy, real.ultimo) : null,
      todas: real.todas || [],
    };
  }

  // Respaldo aproximado mientras el calendario real no esté descargado.
  // OJO: son fechas típicas hardcodeadas, no confirmadas, y se sabe que
  // algunas están desfasadas (ej. MSFT figuraba 7/23 y reportó 7/29).
  // Se marcan como aproximadas y nunca deben tratarse como dato firme.
  const fallback = {
    "AAPL":[7,31],"MSFT":[7,23],"GOOGL":[7,22],"AMZN":[8,1],"META":[7,23],
    "NVDA":[8,20],"TSLA":[7,21],"JPM":[7,10],"KO":[7,28],"PEP":[7,23],
    "GGAL":[8,14],"YPFD":[8,7],"PAMP":[8,12],
  };
  if (fallback[tk]) {
    const [m, d] = fallback[tk];
    // Los balances son trimestrales: si la fecha base ya pasó, se avanza
    // por trimestres hasta caer en el futuro. Nunca devolver una fecha
    // pasada como "próxima" — sería un aviso falso.
    let f = new Date(hoy.getFullYear(), m - 1, d);
    let guard = 0;
    while (dias(f, hoy) < 0 && guard++ < 8) f.setMonth(f.getMonth() + 3);
    return {
      estado: "aproximado", tk,
      prox: f.toISOString().slice(0, 10),
      diasProx: dias(f, hoy), ultimo: null, diasUltimo: null, todas: [],
    };
  }

  return { estado: "sin_dato", tk };
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

// ── PERSISTENCIA DE DATOS DEL USUARIO ──
//
// Las listas y el tracker los creó el usuario a mano: no se pueden
// regenerar. El cache de precios sí (está embebido en data.js). Por eso
// el usuario tiene prioridad absoluta: si la cuota de localStorage se
// llena, se descarta cache, nunca lo del usuario.
//
// Esto antes fallaba EN SILENCIO. El cache (158 tickers × 400 barras)
// pesa ~5.4 MB contra los ~5 MB de cuota por origen, así que la llenaba;
// el setItem del usuario lanzaba QuotaExceededError y un catch vacío se
// lo tragaba. La marca aparecía en pantalla (estado de React) pero no
// sobrevivía al reload.
function guardarUsuario(key, value) {
  let s;
  try { s = typeof value === "string" ? value : JSON.stringify(value); }
  catch (e) { return { ok:false, error:"no serializable" }; }

  try {
    localStorage.setItem(key, s);
    return { ok:true };
  } catch (e) {
    // Cuota llena → liberar el cache de precios (regenerable) y reintentar
    try {
      const cacheKeys = Object.keys(localStorage).filter(k => k.startsWith(`${PREFIX}tk_`));
      for (const k of cacheKeys) localStorage.removeItem(k);
      localStorage.removeItem(`${PREFIX}meta`);
      localStorage.setItem(key, s);
      console.warn(`[FXCA16] Cuota de localStorage llena: se liberaron ${cacheKeys.length} entradas de cache para guardar "${key}". El cache se regenera solo.`);
      return { ok:true, liberado:cacheKeys.length };
    } catch (e2) {
      // Falla dura: avisar fuerte en vez de perder el dato en silencio
      console.error(`[FXCA16] NO SE PUDO GUARDAR "${key}" — el dato se va a perder al recargar.`, e2);
      return { ok:false, error:e2?.message || String(e2) };
    }
  }
}

const StorageManager = {

  // Guardar todos los tickers del CSV parseado
  async saveCSV(csvData, log) {
    log("💾 Guardando datos en storage...", "sys");
    const tickers = Object.keys(csvData);
    // Presupuesto de bytes. localStorage da ~5 MB por origen y el cache
    // completo (158 tickers × 400 barras) pesa ~5.4 MB: llenaba la cuota
    // y hacía fallar en silencio el guardado de listas y tracker.
    // Se reserva margen a propósito — el cache es regenerable desde
    // data.js, los datos del usuario no.
    const BUDGET = 3 * 1024 * 1024;
    let usado = 0, saved = 0, omitidos = 0;
    const guardados = [];
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
      const payload = JSON.stringify({
        bars: compressed,
        moneda: compressed[0]?.m || "USD",
        lastUpdate: new Date().toISOString(),
        count: compressed.length,
      });
      if (usado + payload.length > BUDGET) { omitidos++; continue; }
      try {
        await window.storage.set(`${PREFIX}tk_${tk}`, payload);
        usado += payload.length;
        guardados.push(tk);
        saved++;
      } catch(e) { log(`⚠️ No se pudo guardar ${tk}: ${e.message}`, "warn"); }
    }
    // Metadata: solo los que efectivamente entraron, para que loadAll
    // no busque tickers que se omitieron por presupuesto.
    await window.storage.set(`${PREFIX}meta`, JSON.stringify({
      tickers: guardados,
      savedAt: new Date().toISOString(),
      version: STORAGE_VERSION,
      count: saved,
    }));
    log(`✅ ${saved}/${tickers.length} tickers en cache (${(usado/1048576).toFixed(1)} MB)`, "ok");
    if (omitidos) log(`   ${omitidos} omitidos por presupuesto — se leen de data.js igual`, "info");
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
  const [verIndicadores, setVerIndicadores] = useState(false);
  const [verBacktest, setVerBacktest] = useState(false);
  const [verTablaRsi, setVerTablaRsi] = useState(false);
  const [verTablaVelas, setVerTablaVelas] = useState(false);
  // ── REPLAY: reconstruye qué decía el sistema en una fecha pasada ──
  const [rpTicker, setRpTicker]   = useState("");
  const [rpInput,  setRpInput]    = useState("");
  const [rpVent,   setRpVent]     = useState(30); // periodo del grafico del activo puntual
  const [rpVentRank, setRpVentRank] = useState(30); // periodo del ranking top20, independiente
  const [rpSel,    setRpSel]      = useState(null);
  const [rpCalc,   setRpCalc]     = useState(null);
  const [rpCargando, setRpCargando] = useState(false);
  const [rpRankTab, setRpRankTab] = useState("suben");

  // ── PANTALLA DIVIDIDA ──
  // splitMode: null (normal) | 2 | 4 — cuántos paneles mostrar a la vez.
  // paneTabs: qué tab muestra cada panel, independiente entre sí.
  const [splitMode, setSplitMode] = useState(null);
  const [calFiltro, setCalFiltro] = useState("todos");
  // Ancho real de la ventana — la grilla de paneles usa CSS (media query en
  // index.css) para apilarse en columna en celular, pero el maxHeight y el
  // padding de cada panel se ajustan acá porque eso no depende solo del
  // layout sino de cuánto scroll vertical tiene sentido en cada caso.
  const [anchoVentana, setAnchoVentana] = useState(typeof window !== "undefined" ? window.innerWidth : 1024);
  useEffect(() => {
    const onResize = () => setAnchoVentana(window.innerWidth);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);
  const esMobile = anchoVentana <= 820;

  // ── Gráfico de variación diaria embebido en el Detalle ──
  // Misma idea que en Replay, pero con estado propio para no interferir
  // con lo que el usuario esté haciendo en esa otra pestaña, y usando el
  // ticker que ya está abierto en Detalle en vez de pedir que se escriba
  // de nuevo.
  const [dtVent, setDtVent] = useState(30);
  const [dtSel, setDtSel] = useState(null);
  const [dtCalc, setDtCalc] = useState(null);
  const [dtCargando, setDtCargando] = useState(false);
  useEffect(() => { setDtSel(null); setDtCalc(null); }, [sel?.ticker]);
  const [paneTabs, setPaneTabs] = useState(["opp", "det", "replay", "cmp"]);
  const PANE_TABS_DISPONIBLES = [["opp","Oport."],["det","Detalle"],["replay","Replay"],["cmp","Comparar"],["watch","Listas"],["track","Tracker"]];
  const [rpAjustVol, setRpAjustVol] = useState(false);
  const [rpCruce, setRpCruce] = useState(null);
  const [rpCruceCargando, setRpCruceCargando] = useState(false);
  const [rpCruceProg, setRpCruceProg] = useState({ hecho: 0, total: 0 });
  // Tick cada minuto: mantiene vivo el estado de mercado (abierto/cerrado)
  // y la antigüedad del dato sin necesidad de recargar la página.
  const [nowTick, setNowTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setNowTick(t=>t+1), 60000);
    return () => clearInterval(id);
  }, []);
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

  // ══ TRACKER — registro de seguimiento con ancla de precio/fecha ══
  // A diferencia de la watchlist (lista de interés), acá cada marca queda
  // fijada al precio y momento exacto en que se marcó, para medir después
  // si el sistema acertó. Es evidencia hacia adelante: nada de lo que se
  // registre acá pudo estar contaminado por resultados ya conocidos.
  const [tracker, setTracker] = useState(() => {
    try { return JSON.parse(localStorage.getItem('fxca16_tracker') || '[]'); }
    catch { return []; }
  });
  const guardarTracker = useCallback((items) => {
    setTracker(items);
    guardarUsuario('fxca16_tracker', items);
  }, []);
  const marcarSeguimiento = useCallback((r) => {
    if (!r?.ticker || !r?.sig) return;
    setTracker(prev => {
      if (prev.some(t => t.ticker === r.ticker && !t.cerrado)) return prev;  // ya en seguimiento
      const nuevo = {
        id: `${r.ticker}_${Date.now()}`,
        ticker: r.ticker,
        nombre: r.name,
        moneda: r.moneda,
        fechaMarca: new Date().toISOString(),
        precioMarca: r.price,
        // Se congela TODO lo que el sistema decía en ese momento — es la
        // predicción que se está poniendo a prueba, no se puede editar después.
        señalMarca: r.sig.sig,
        scoreMarca: r.sig.final_sc,
        confMarca: r.sig.conf,
        rrMarca: r.sig.rr,
        alphaMarca: alphaRank?.[r.ticker]?.percentil ?? null,
        alphaPreliminar: alphaRank?.[r.ticker]?.esMerval ?? false,
        calidadMarca: calidadDe(r.ticker)?.calidad ?? null,
        entryMarca: r.sig.entry, slMarca: r.sig.sl, tp2Marca: r.sig.tp2,
        wMarca: W,
        cerrado: false,
      };
      const items = [nuevo, ...prev];
      guardarUsuario('fxca16_tracker', items);
      return items;
    });
  }, [alphaRank, W]);
  const cerrarSeguimiento = useCallback((id, precioActual) => {
    setTracker(prev => {
      const items = prev.map(t => t.id===id ? {
        ...t, cerrado:true, fechaCierre:new Date().toISOString(), precioCierre:precioActual
      } : t);
      guardarUsuario('fxca16_tracker', items);
      return items;
    });
  }, []);
  const quitarSeguimiento = useCallback((id) => {
    setTracker(prev => {
      const items = prev.filter(t => t.id !== id);
      guardarUsuario('fxca16_tracker', items);
      return items;
    });
  }, []);
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
  const qlCancelRef = useRef(false);   // permite abortar el proceso
  const [qlParams,  setQlParams]  = useState({ topN:5, holdDays:10, minProb:0.55, useKelly:true });

  const saveWatchlists = (wls) => {
    setWatchlists(wls);
    guardarUsuario('fxca16_watchlists', wls);
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
  const embeddedLastISO = useMemo(() => {
    let maxDate = "", maxHour = 0;
    for (const bars of Object.values(CSV_DATA_EMBEDDED)) {
      const last = bars[bars.length-1];
      if (!last) continue;
      if (last.d > maxDate || (last.d === maxDate && (last.h||0) > maxHour)) {
        maxDate = last.d; maxHour = last.h || 0;
      }
    }
    return maxDate ? `${maxDate}T${String(maxHour).padStart(2,"0")}:00:00` : null;
  }, []);
  const mercadoInfo = useMemo(() => estadoMercado(), [nowTick]);
  const fomcInfo = useMemo(() => estadoFOMC(), [nowTick]);
  const datoInfo = useMemo(() => antiguedadDato(embeddedLastISO), [embeddedLastISO, nowTick]);

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

  // Seed dynParams desde data.js — CONSERVADO SÓLO COMO DATO INERTE.
  // adaptiveW() está neutralizado (ver HALLAZGOS_DESCARTADOS), así que
  // getDynParam() ya no lo lee nadie. Se deja el seed para no romper el
  // formato de data.js que genera el workflow, pero NO afecta ninguna señal.
  useEffect(()=>{
    if (DYN_PARAMS_IMPORTED && Object.keys(DYN_PARAMS_IMPORTED).length > 0) {
      dynParamsRef.current = DYN_PARAMS_IMPORTED;
      setDynParams(DYN_PARAMS_IMPORTED);
      setDynParamsVersion(v=>v+1);
      lg(`dynParams: ${Object.keys(DYN_PARAMS_IMPORTED).length} tickers cargados (INERTE — no se aplican, ver tab Reglas)`, "info");
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
        if (Object.keys(porMoneda.USD).length >= 15) {
          const rkUSA = ALPHA.rankearUniverso(porMoneda.USD);
          if (rkUSA) Object.assign(comb, rkUSA.porTicker);
        }
        // Merval: señal DISTINTA a la de USA (la de vol_shock/mom_1m medía
        // invertida). Con iliquidez de Amihud + asimetría, filtrando a los
        // 20 papeles más líquidos, el IC pasa a +0.236 fuera de muestra.
        // Marcado como preliminar: 20 tickers × 50 fechas es poco para el
        // t medido — pendiente de confirmar con más historia.
        if (Object.keys(porMoneda.ARS).length >= 15) {
          const rkArs = ALPHA.rankearUniversoMerval(porMoneda.ARS);
          if (rkArs) {
            Object.entries(rkArs.porTicker).forEach(([tk,v]) => { comb[tk] = { ...v, esMerval:true }; });
          }
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
      // Veto de Fibonacci: compra sin soporte estructural cerca (validado
      // 10 años, -2.63% a 20d, 8/8 años negativos, funciona en ARS y USD)
      if (sig) sig.vetoFib = vetoFibonacci(sig, data);
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
    qlCancelRef.current = false;
    setQlRunning(true); setQlModels(null); setQlPort(null); setQlAblation(null); setQlMeta(null); setQlValid(null); setQlConsist(null);
    const yield_ = () => new Promise(r => {
      requestAnimationFrame(() => setTimeout(r, 0));
    });
    try {
      // Priorizar los tickers con más historia: entrenar 158 modelos en el
      // hilo principal congela la interfaz varios minutos. Con 70 alcanza
      // para un backtest de cartera representativo.
      const MAX_MODELOS = 45;
      const tks = rows
        .map(r => ({ tk: r.ticker, n: (serieLarga(r.ticker) || rowDataRef.current[r.ticker] || []).length }))
        .filter(x => x.n >= 200)
        .sort((a,b) => b.n - a.n)
        .slice(0, MAX_MODELOS)
        .map(x => x.tk);

      const universe = [];
      const modelInfo = [];
      let allX = [], allY = [];

      for (let i = 0; i < tks.length; i++) {
        if (qlCancelRef.current) { setQlProgress("Cancelado"); setQlRunning(false); return; }
        const tk = tks[i];
        setQlProgress(`Entrenando modelos · ${tk} (${i+1}/${tks.length})`);
        await yield_();   // ceder el hilo en CADA ticker, no cada 3
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
      const abl = Q.featureAblation(allX.slice(0,800), allY.slice(0,800), baseAuc);
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
        if (qlCancelRef.current) { setQlProgress("Cancelado"); setQlRunning(false); return; }
        setQlProgress(`Meta-modelo · ${u.ticker} (${i+1}/${Math.min(universe.length,25)})`);
        await yield_();
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
      setQlProgress("Calculando Deflated Sharpe y bootstrap...");
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
        await yield_();   // PBO recorre C(8,4)=70 combinaciones × ~300 fechas — cede el hilo antes
        if (mtx.length > 40) validacion.pbo = Q2.computePBO(mtx, 8);

        await yield_();
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
      // FIX: antes se llamaba a Q2.generarObservaciones para 40 tickers de
      // una sola vez — un bloque síncrono de cientos de recálculos de señal
      // sin ceder el hilo, que colgaba la pestaña varios segundos (más en
      // desktop, donde hay más datos cargados). Ahora se itera acá mismo,
      // cediendo el control después de cada ticker.
      setQlProgress("Midiendo consistencia mes a mes...");
      await yield_();
      try {
        const tickersConsist = rows
          .filter(r => (rowDataRef.current[r.ticker]?.length || 0) >= 400)
          .slice(0, 25)
          .map(r => r.ticker);

        const observ = [];
        for (let i = 0; i < tickersConsist.length; i++) {
          if (qlCancelRef.current) { setQlProgress("Cancelado"); setQlRunning(false); return; }
          const tk = tickersConsist[i];
          setQlProgress(`Consistencia temporal · ${tk} (${i+1}/${tickersConsist.length})`);
          await yield_();
          const data = rowDataRef.current[tk];
          const daily = [];
          const byDay = {};
          data.forEach(d => {
            const day = d.date || d.d || "";
            if (!day) return;
            if (!byDay[day]) { byDay[day] = { date: day, close: d.close }; daily.push(byDay[day]); }
            byDay[day].close = d.close;
          });
          if (daily.length < 120) continue;
          const dp = {}; daily.forEach((x,j) => dp[x.date] = j);

          for (let bi = 300; bi < data.length - 1; bi += 8) {
            let sig = null;
            try { sig = combinedSignal(data.slice(0, bi + 1), W); } catch(e) { continue; }
            if (!sig) continue;
            const di = dp[data[bi].date];
            if (di === undefined || di + W >= daily.length) continue;
            const e0 = daily[di].close, e1 = daily[di + W].close;
            if (!e0 || !e1) continue;
            observ.push({ ticker: tk, fecha: data[bi].date, sc: sig.final_sc, fwd: (e1-e0)/e0*100 });
          }
        }

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

  // ── REPLAY: reconstruir qué decía el sistema en una fecha pasada ──
  //
  // Clave metodológica: se corta la serie EN esa fecha y se recalcula.
  // El motor nunca ve datos posteriores, así que la señal es exactamente
  // la que habría mostrado ese día. Después se compara contra lo que
  // efectivamente pasó — que sí conocemos, pero el motor no vio.
  const rpBarras = useMemo(() => {
    if (!rpTicker) return null;
    const b = rowDataRef.current[rpTicker];
    if (b?.length >= 80) return b;
    const emb = CSV_DATA_EMBEDDED?.[rpTicker];
    return emb?.length >= 80 ? emb : null;
  }, [rpTicker, rows]);

  // Serie diaria para el gráfico (una barra por día, no por hora)
  const rpDias = useMemo(() => {
    if (!rpBarras) return [];
    const porDia = {};
    for (const b of rpBarras) {
      const d = b.date;
      if (!porDia[d]) porDia[d] = { date: d, open: b.open, high: b.high, low: b.low, close: b.close, volume: 0, idx: 0 };
      porDia[d].high = Math.max(porDia[d].high, b.high);
      porDia[d].low = Math.min(porDia[d].low, b.low);
      porDia[d].close = b.close;
      porDia[d].volume += b.volume || 0;
    }
    const dias = Object.values(porDia).sort((a, b) => a.date < b.date ? -1 : 1);
    // índice de la última barra intradiaria de cada día — el corte del replay
    for (const d of dias) {
      let ult = -1;
      for (let i = 0; i < rpBarras.length; i++) if (rpBarras[i].date === d.date) ult = i;
      d.idx = ult;
    }
    return dias.map((d, i) => ({
      ...d,
      ret: i > 0 ? (d.close / dias[i - 1].close - 1) * 100 : 0,
    }));
  }, [rpBarras]);

  const rpVisibles = useMemo(() => rpDias.slice(-rpVent), [rpDias, rpVent]);

  useEffect(() => { setRpCruce(null); }, [rpRankTab, rpVentRank]);

  // ── RANKING: quiénes subieron, cayeron y se mantuvieron en el período ──
  //
  // Se calcula sobre la serie diaria de todo el universo. "Se mantuvieron"
  // son los de menor variación ABSOLUTA — no los del medio de la tabla,
  // que serían simplemente los menos malos. Excluye series degradadas:
  // un papel que no opera figura como "estable" cuando en realidad no
  // tiene precio real (el caso GAMI/POLL).
  //
  // Además de la variación cruda, se calcula un EXCESO AJUSTADO POR
  // VOLATILIDAD: comparado contra la mediana de retorno de activos con
  // volatilidad similar en el mismo período. Es la misma corrección que
  // en hallazgos.md tumbó la hipótesis de reversión — un ticker que
  // "sube 15%" con vol diaria de 6% no es comparable a uno que sube 15%
  // con vol de 1%. Sin este ajuste, el ranking crudo sobre-representa
  // simplemente a los más volátiles.
  const rpRanking = useMemo(() => {
    const fuente = DATA_MOD?.CSV_DATA_DAILY_RAW || {};
    const sectorDe = {}; for (const t of TICKERS_TODOS) sectorDe[t.ticker] = t.sector;
    const out = [];
    for (const [tk, bars] of Object.entries(fuente)) {
      if (!bars || bars.length < rpVentRank + 5) continue;
      const c = bars.map(b => b.c);
      const px = c[c.length - 1];
      const ini = c[c.length - 1 - rpVentRank];
      if (!ini || !px || !isFinite(ini) || !isFinite(px)) continue;
      const tramo = bars.slice(-rpVentRank);
      const sinVol = tramo.filter(b => !b.v).length / tramo.length;
      let congelado = 0;
      for (let i = 1; i < tramo.length; i++) if (tramo[i].c === tramo[i-1].c) congelado++;
      if (sinVol > 0.3 || congelado / (tramo.length - 1) > 0.5) continue;
      const ret = (px / ini - 1) * 100;
      if (!isFinite(ret) || Math.abs(ret) > 300) continue;
      let sospechoso = false;
      for (let i = 1; i < tramo.length; i++) {
        if (tramo[i-1].c > 0 && Math.abs(tramo[i].c / tramo[i-1].c - 1) > 0.5) { sospechoso = true; break; }
      }
      // volatilidad diaria del período (desvío de retornos)
      const rets = []; for (let i = 1; i < tramo.length; i++) rets.push(tramo[i].c / tramo[i-1].c - 1);
      const m = rets.reduce((a,b)=>a+b,0) / (rets.length||1);
      const vol = Math.sqrt(rets.reduce((s,x)=>s+(x-m)**2,0) / (rets.length-1||1)) * 100;
      const r0 = rows.find(x => x.ticker === tk);
      out.push({ tk, ret, px, vol, sospechoso, moneda: bars[bars.length-1].m,
                 sector: sectorDe[tk] || "—",
                 sig: r0?.sig?.sig || null, name: r0?.name || "" });
    }
    // exceso ajustado: mediana de retorno por quintil de volatilidad
    const conVol = out.filter(x=>isFinite(x.vol));
    const ordVol = [...conVol].sort((a,b)=>a.vol-b.vol);
    const nb = 5, tam = Math.floor(ordVol.length/nb) || 1;
    for (let q = 0; q < nb; q++) {
      const bucket = ordVol.slice(q*tam, q===nb-1 ? ordVol.length : (q+1)*tam);
      if (!bucket.length) continue;
      const rets = bucket.map(x=>x.ret).sort((a,b)=>a-b);
      const mediana = rets[Math.floor(rets.length/2)];
      for (const x of bucket) x.exceso = x.ret - mediana;
    }
    const porRet = [...out].sort((a, b) => b.ret - a.ret);
    const porExc = [...out].filter(x=>x.exceso!=null).sort((a,b)=>b.exceso-a.exceso);
    const suben    = porRet.slice(0, 20);
    const caen     = porRet.slice(-20).reverse();
    const estables = [...out].sort((a,b) => Math.abs(a.ret) - Math.abs(b.ret)).slice(0, 20);

    // concentración sectorial de los que más se movieron — la misma
    // lectura que en el caso BA/GLOB/ORCL/AAL/BABA: si el movimiento
    // abarca muchos sectores sin relación, es más probable que sea un
    // catalizador de mercado amplio que una señal sectorial específica.
    const concentracion = (lista) => {
      const conteo = {};
      for (const x of lista) conteo[x.sector] = (conteo[x.sector]||0) + 1;
      const sectores = Object.entries(conteo).sort((a,b)=>b[1]-a[1]);
      const top = sectores[0];
      const distintos = sectores.length;
      const dominante = top ? top[1] / lista.length : 0;
      return { distintos, dominante, sectorTop: top?.[0], nTop: top?.[1], sectores };
    };

    return {
      suben, caen, estables,
      subenExc: porExc.slice(0, 20),
      caenExc: [...porExc].reverse().slice(0, 20),
      total: out.length,
      concSuben: concentracion(suben),
      concCaen: concentracion(caen),
    };
  }, [rpVentRank, rows]);

  // Al tocar una barra: recalcular la señal con la serie cortada ahí
  const rpAnalizar = useCallback((dia) => {
    if (!rpBarras || dia.idx < 60) return;
    setRpSel(dia.date);
    setRpCargando(true);
    setRpCalc(null);
    setTimeout(() => {
      try {
        const hasta = rpBarras.slice(0, dia.idx + 1);
        const sig = combinedSignal(hasta, W);
        const cierres = rpDias.map(d => d.close);
        const iDia = rpDias.findIndex(d => d.date === dia.date);
        const px = dia.close;
        const fwd = n => (iDia + n < rpDias.length) ? (cierres[iDia + n] / px - 1) * 100 : null;
        const prev = n => (iDia - n >= 0) ? (px / cierres[iDia - n] - 1) * 100 : null;
        setRpCalc({
          fecha: dia.date, px, sig,
          prev5: prev(5), prev10: prev(10), prev20: prev(20),
          fwd1: fwd(1), fwd3: fwd(3), fwd5: fwd(5), fwd10: fwd(10), fwd20: fwd(20),
          // máximo y mínimo alcanzados en los 20 días siguientes: dice si
          // el trade habría tocado el TP o el stop antes de cerrar
          maxFwd: (()=>{const h=rpDias.slice(iDia+1,iDia+21); return h.length?Math.max(...h.map(d=>d.high))/px*100-100:null;})(),
          minFwd: (()=>{const h=rpDias.slice(iDia+1,iDia+21); return h.length?Math.min(...h.map(d=>d.low))/px*100-100:null;})(),
          diasDisp: Math.max(0, rpDias.length-1-iDia),
          barrasUsadas: hasta.length,
        });
      } catch (e) {
        setRpCalc({ error: e?.message || "no se pudo calcular" });
      }
      setRpCargando(false);
    }, 10);
  }, [rpBarras, rpDias, W]);

  // ── Gráfico embebido en Detalle: misma mecánica que Replay, pero con
  // estado propio (dtVent/dtSel/dtCalc) y usando el ticker que ya está
  // abierto en Detalle, sin pedir que se vuelva a escribir. ──
  const construirDiasDe = useCallback((barras) => {
    const porDia = {};
    for (const b of barras) {
      const d = b.date;
      if (!porDia[d]) porDia[d] = { date: d, close: b.close, idx: 0 };
      porDia[d].close = b.close;
    }
    const dias = Object.values(porDia).sort((a, b) => a.date < b.date ? -1 : 1);
    for (const d of dias) {
      let ult = -1;
      for (let i = 0; i < barras.length; i++) if (barras[i].date === d.date) ult = i;
      d.idx = ult;
    }
    return dias;
  }, []);


  // Igual que rpBarras en Replay: si sel.data viene de un fetch en vivo
  // (badge REAL) puede ser muy corta — unas pocas barras del día, no el
  // historial completo. Este caso real lo destapó BOLT: sel.data quedaba
  // por debajo de 20 barras, calidadSerie lo marcaba "serie muy corta"
  // (motivo correcto, dato equivocado), y el gráfico no tenía con qué
  // dibujarse. Se cae al historial embebido completo cuando pasa esto,
  // mismo criterio que ya usa Replay.
  const dtBarras = useMemo(() => {
    if (sel?.data?.length >= 80) return sel.data;
    const emb = sel?.ticker ? CSV_DATA_EMBEDDED?.[sel.ticker] : null;
    return emb?.length >= 80 ? emb : (sel?.data || null);
  }, [sel?.ticker, sel?.data]);

  const dtDias = useMemo(() => {
    if (!dtBarras?.length) return [];
    const dias = construirDiasDe(dtBarras);
    return dias.map((d, i) => ({ ...d, ret: i > 0 ? (d.close / dias[i-1].close - 1) * 100 : 0 }));
  }, [sel?.ticker, dtBarras, construirDiasDe]);
  const dtVisibles = useMemo(() => dtDias.slice(-dtVent), [dtDias, dtVent]);

  const dtAnalizar = useCallback((dia) => {
    if (!dtBarras?.length || dia.idx < 60) return;
    setDtSel(dia.date);
    setDtCargando(true);
    setDtCalc(null);
    setTimeout(() => {
      try {
        const hasta = dtBarras.slice(0, dia.idx + 1);
        const sig = combinedSignal(hasta, W);
        const cierres = dtDias.map(d => d.close);
        const iDia = dtDias.findIndex(d => d.date === dia.date);
        const px = dia.close;
        const fwd = n => (iDia + n < dtDias.length) ? (cierres[iDia + n] / px - 1) * 100 : null;
        const prev = n => (iDia - n >= 0) ? (px / cierres[iDia - n] - 1) * 100 : null;
        setDtCalc({
          fecha: dia.date, px, sig,
          prev5: prev(5), prev10: prev(10), prev20: prev(20),
          fwd1: fwd(1), fwd3: fwd(3), fwd5: fwd(5), fwd10: fwd(10), fwd20: fwd(20),
          maxFwd: (()=>{const h=dtDias.slice(iDia+1,iDia+21); return h.length?Math.max(...h.map(d=>d.high))/px*100-100:null;})(),
          minFwd: (()=>{const h=dtDias.slice(iDia+1,iDia+21); return h.length?Math.min(...h.map(d=>d.low))/px*100-100:null;})(),
          diasDisp: Math.max(0, dtDias.length-1-iDia),
        });
      } catch (e) {
        setDtCalc({ error: e?.message || "no se pudo calcular" });
      }
      setDtCargando(false);
    }, 10);
  }, [dtBarras, dtDias, W]);

  // ── CRUCE AUTOMÁTICO: para el top 10 de suben/caen, ¿qué decía la
  // señal ANTES de que arrancara el movimiento? ──
  //
  // Es el mismo mecanismo del replay (cortar la serie, sin lookahead)
  // corrido en batch. Por costo — cada corte recalcula todo el motor de
  // señales, ~190ms — se muestrea la ventana en vez de recorrer cada día,
  // y se acota a 10 activos con feedback de progreso para no trabar la UI.
  const rpCorrerCruce = useCallback(() => {
    const tab = rpRankTab;
    if (tab !== "suben" && tab !== "caen") return;
    const top10 = (tab === "suben" ? rpRanking.suben : rpRanking.caen).slice(0, 10);
    if (!top10.length) return;
    setRpCruceCargando(true);
    setRpCruce(null);
    setRpCruceProg({ hecho: 0, total: top10.length });

    const buscar = tab === "suben" ? "COMPRA" : "VENTA";
    const items = [];
    let idx = 0;

    const procesarUno = () => {
      const t = top10[idx];
      try {
        const barras = rowDataRef.current[t.tk]?.length >= 80 ? rowDataRef.current[t.tk] : CSV_DATA_EMBEDDED?.[t.tk];
        const dias = barras?.length >= 80 ? construirDiasDe(barras) : null;
        const ventana = dias ? dias.slice(-rpVentRank) : [];
        if (!ventana.length || ventana.length < 5) {
          items.push({ tk: t.tk, ret: t.ret, diaSenal: null, fechaSenal: null, pctConsumido: 0 });
        } else {
          const pxIni = ventana[0].close, pxFin = ventana[ventana.length - 1].close;
          const retTotal = pxFin / pxIni - 1;
          const paso = Math.max(1, Math.ceil(ventana.length / 12));
          let hallado = null;
          for (let i = paso; i < ventana.length; i += paso) {
            const dia = ventana[i];
            if (dia.idx < 60) continue;
            let sig;
            try { sig = combinedSignal(barras.slice(0, dia.idx + 1), W); } catch (_) { continue; }
            if (sig?.sig?.includes(buscar)) {
              const consumido = retTotal !== 0 ? ((dia.close / pxIni - 1) / retTotal) * 100 : 0;
              hallado = { dia: i, fecha: dia.date, consumido: Math.max(0, Math.min(100, consumido)) };
              break;
            }
          }
          items.push({
            tk: t.tk, ret: t.ret,
            diaSenal: hallado ? hallado.dia : null,
            fechaSenal: hallado ? hallado.fecha : null,
            pctConsumido: hallado ? hallado.consumido : 0,
          });
        }
      } catch (_) {
        items.push({ tk: t.tk, ret: t.ret, diaSenal: null, fechaSenal: null, pctConsumido: 0 });
      }
      idx++;
      setRpCruceProg({ hecho: idx, total: top10.length });
      if (idx < top10.length) {
        setTimeout(procesarUno, 15);
      } else {
        setRpCruce({ tab, items });
        setRpCruceCargando(false);
      }
    };
    setTimeout(procesarUno, 15);
  }, [rpRankTab, rpRanking, rpVentRank, W, construirDiasDe]);

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

  // ── ALERTA DE NUEVAS SEÑALES ──
  //
  // Antes la señal de compra solo era visible si abrías la app. Esto
  // compara el set actual de "COMPRA FUERTE dentro del P80" contra el
  // de la última vez que se calculó, y avisa (banner + notificación
  // del navegador si el usuario la habilitó) cuando aparece un ticker
  // nuevo. Solo notifica con mercado abierto — evita que una recarga
  // en pleno fin de semana, con datos del último cierre, dispare una
  // alerta como si fuera una señal fresca.
  const [nuevasSenales, setNuevasSenales] = useState([]);
  const [notifPermiso, setNotifPermiso] = useState(
    typeof Notification !== "undefined" ? Notification.permission : "unsupported"
  );
  useEffect(() => {
    if (!rows.length) return;
    const fuertesActuales = rows
      .filter(r => r.sig?.above_p80 && r.sig?.sig === "COMPRA FUERTE")
      .map(r => r.ticker)
      .sort();

    let previo = null;
    try {
      const raw = localStorage.getItem("fxca16_ultimas_senales");
      previo = raw ? JSON.parse(raw) : null;
    } catch (_) { previo = null; }

    // Primera vez que corre (no hay snapshot previo): solo sembrar,
    // no notificar — si no, cada instalación nueva "alerta" de todo
    // el P80 del día como si fuera nuevo.
    if (previo && Array.isArray(previo.tickers)) {
      const antes = new Set(previo.tickers);
      const nuevos = fuertesActuales.filter(tk => !antes.has(tk));
      if (nuevos.length && mercadoInfo.abierto) {
        setNuevasSenales(nuevos);
        if (typeof Notification !== "undefined" && Notification.permission === "granted") {
          const cuerpo = nuevos.length === 1
            ? `${nuevos[0]} entró a COMPRA FUERTE (top P80)`
            : `${nuevos.slice(0,3).join(", ")}${nuevos.length>3?` y ${nuevos.length-3} más`:""} entraron a COMPRA FUERTE`;
          try {
            const n = new Notification("FXCA16 — Nueva señal", { body: cuerpo, tag: "fxca16-senales" });
            n.onclick = () => { window.focus(); };
          } catch (_) {}
        }
        try {
          // Beep corto, sin depender de un archivo de audio externo.
          const ctx = new (window.AudioContext || window.webkitAudioContext)();
          const o = ctx.createOscillator(), g = ctx.createGain();
          o.connect(g); g.connect(ctx.destination);
          o.frequency.value = 880; g.gain.value = 0.08;
          o.start(); o.stop(ctx.currentTime + 0.15);
        } catch (_) {}
      }
    }

    guardarUsuario("fxca16_ultimas_senales", { tickers: fuertesActuales, at: new Date().toISOString() });
  }, [rows, mercadoInfo.abierto]);

  const pedirPermisoNotif = useCallback(() => {
    if (typeof Notification === "undefined") return;
    Notification.requestPermission().then(p => setNotifPermiso(p));
  }, []);

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

      {/* ── ESTADO DE MERCADO — siempre visible, en todas las tabs ── */}
      <div style={{padding:"0 16px",marginTop:"10px"}}>
        <div style={{display:"flex",alignItems:"center",gap:"8px",flexWrap:"wrap",padding:"6px 10px",
          ...semBox(mercadoInfo.abierto?"#00ff88":datoInfo.minutos>180?"#ff9040":"#5a8fa8","10")}}>
          <span style={{width:"7px",height:"7px",borderRadius:"50%",flexShrink:0,
            background:mercadoInfo.abierto?"#00ff88":"#5a8fa8",
            boxShadow:mercadoInfo.abierto?"0 0 6px #00ff88":"none"}}/>
          <span style={{fontSize:"8px",color:mercadoInfo.abierto?"#00ff88":"#8fb4cc",fontWeight:700}}>
            {mercadoInfo.abierto?"● MERCADO ABIERTO":"○ MERCADO CERRADO"}
          </span>
          <span style={{fontSize:"7px",color:"#5a8fa8"}}>{mercadoInfo.mensaje}</span>
          <span style={{marginLeft:"auto",fontSize:"7px",color:datoInfo.minutos>180?"#ff9040":"#5a8fa8"}}>
            📊 último dato: {datoInfo.mensaje}
          </span>
          {notifPermiso!=="unsupported"&&notifPermiso!=="granted"&&(
            <button onClick={pedirPermisoNotif} className="btn off" style={{padding:"3px 8px",fontSize:"7px"}}>
              🔔 Activar alertas
            </button>
          )}
          {notifPermiso==="granted"&&(
            <span style={{fontSize:"7px",color:"#00ff88"}}>🔔 alertas activas</span>
          )}
        </div>

        {nuevasSenales.length>0&&(
          <div style={{display:"flex",alignItems:"center",gap:"8px",marginTop:"6px",padding:"8px 10px",
            ...semBox("#00ff88","20"),animation:"fd .3s ease"}}>
            <span style={{fontSize:"14px"}}>🔔</span>
            <span style={{fontSize:"9px",color:"#00ff88",fontWeight:700,flex:1}}>
              {nuevasSenales.length===1
                ? `Nueva señal: ${nuevasSenales[0]} entró a COMPRA FUERTE`
                : `${nuevasSenales.length} nuevas señales: ${nuevasSenales.join(", ")}`}
            </span>
            <button onClick={()=>setNuevasSenales([])} className="btn off" style={{padding:"3px 8px",fontSize:"7px"}}>
              Descartar
            </button>
          </div>
        )}

        {fomcInfo.dias!=null&&fomcInfo.dias<=14&&(
          <div style={{display:"flex",alignItems:"center",gap:"8px",marginTop:"6px",padding:"7px 10px",
            ...semBox(fomcInfo.dias<=3?"#ff3355":"#ffd700","14")}}>
            <span style={{fontSize:"12px"}}>🏛️</span>
            <span style={{fontSize:"8px",color:fomcInfo.dias<=3?"#ff3355":"#ffd700",fontWeight:700}}>
              Reunión de la Fed en {fomcInfo.dias===0?"HOY":fomcInfo.dias===1?"1 día":`${fomcInfo.dias} días`} ({fomcInfo.proxima})
            </span>
            <span style={{fontSize:"7px",color:"#8fb4cc",flex:1}}>
              Movimientos de precio cerca de esta fecha pueden ser por el evento, no por análisis técnico
            </span>
          </div>
        )}

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
              {[["opp","🎯 Oportunidades"],["det","🔍 Detalle"],["replay","⏪ Replay"],["cmp","⚖️ Comparar"],["watch","⭐ Listas"],["track","📌 Tracker"],["cal","📅 Calendario"],["quant","🔬 Validación"],["reglas","📖 Reglas"]].map(([k,l])=>
                <button key={k} className={`btn ${tab===k?"on":"off"}`} onClick={()=>setTab(k)}>{l}</button>
              )}
              {/* Selector de pantalla dividida (2/4 paneles) — se probó y se
                  decidió volver a vista única por simplicidad. El mecanismo
                  completo (renderTabContent, .split-grid, etc.) queda intacto
                  en el código por si se retoma más adelante; solo se oculta
                  el control para que no aparezca en la interfaz. */}
            </div>
            {!splitMode&&(
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
            )}
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
                                  return <span title={a.esMerval
                                      ? `PRELIMINAR (Merval, top 20 líquidos): percentil ${a.percentil} (Q${a.quintil})`
                                      : `Ranking alfa validado: percentil ${a.percentil} del universo (Q${a.quintil})`}
                                    style={{fontSize:"8px",marginLeft:"4px",padding:"1px 5px",background:`${c}18`,border:`1px ${a.esMerval?"dashed":"solid"} ${c}45`,borderRadius:"3px",color:c,fontWeight:700}}>
                                    {a.esMerval?"⚗α":"α"}{a.percentil}
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
                          <div style={{display:"flex",gap:"5px",marginTop:"6px"}}>
                            <button className="btn off" style={{flex:1,fontSize:"8px",color:"#ffd700",borderColor:"#ffd70040"}}
                              onClick={async(e)=>{e.stopPropagation();
                                const res = await saveTickerToGitHub(r.ticker);
                                if(res?.already) alert(`${r.ticker} ya está en seguimiento`);
                                else if(res?.ok) alert(`✅ ${r.ticker} agregado al seguimiento permanente`);
                                else alert(`❌ Error`);
                              }}>⭐ LISTAS</button>
                            <button className="btn off" style={{flex:1,fontSize:"8px",color:"#00d4ff",borderColor:"#00d4ff40"}}
                              onClick={(e)=>{e.stopPropagation(); marcarSeguimiento(r);}}>
                              📌 TRACKER</button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>

            {/* ── CONTENIDO DE UNA PESTAÑA, parametrizado por paneTab ──
                Extraido para poder reusarlo en modo pantalla dividida:
                cada panel llama a esta misma funcion con su propio tab,
                sin duplicar el codigo de cada vista. En modo normal (1
                panel) se llama una sola vez con paneTab=tab, asi que el
                comportamiento por defecto es identico al de antes. ── */}
            {(() => {
              const renderTabContent = (paneTab) => (
                <>
            {/* OPORTUNIDADES TOP P80 */}
            {paneTab==="opp"&&(()=>{
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
                      ⏱ <strong>{parciales} activos con la rueda todavía abierta.</strong> El último día es intradiario —
                      puede revertir antes del cierre.
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

                        <button className="btn off"
                          onClick={(e)=>{e.stopPropagation();marcarSeguimiento(r);}}
                          disabled={tracker.some(t=>t.ticker===r.ticker && !t.cerrado)}
                          style={{marginTop:"8px",width:"100%",fontSize:"8px",padding:"5px",
                            color: tracker.some(t=>t.ticker===r.ticker && !t.cerrado) ? "#4a7a9b" : "#ffd700",
                            borderColor: tracker.some(t=>t.ticker===r.ticker && !t.cerrado) ? "#1e3a50" : "#ffd70040"}}>
                          {tracker.some(t=>t.ticker===r.ticker && !t.cerrado) ? "📌 Ya en seguimiento" : "📌 Marcar y seguir"}
                        </button>
                      </div>
                    );
                  })}
                </div>
              </div>
              );
            })()}

            {paneTab==="det"&&(
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
                      {sel.bt.n>0 ? (
                        <div style={{textAlign:"center",padding:"12px 20px",background:g.c+"10",border:`1px solid ${g.c}30`,borderRadius:"6px"}}>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"38px",color:g.c,lineHeight:1}}>{g.l}</div>
                          <div style={{fontSize:"9px",color:g.c}}>{sel.bt.hr}%</div>
                        </div>
                      ) : (
                        <div style={{textAlign:"center",padding:"12px 16px",background:"#5a8fa810",border:"1px solid #5a8fa830",borderRadius:"6px"}}>
                          <div style={{fontSize:"9px",color:"#5a8fa8"}}>sin backtest</div>
                          <div style={{fontSize:"7px",color:"#4a7a9b"}}>0 operaciones</div>
                        </div>
                      )}
                    </div>

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
                          <div key={x.l} style={{textAlign:"center",padding:"7px",...semBox(x.c)}}>
                            <div style={{fontSize:"7px",color:"#8fb4cc",marginBottom:"2px"}}>{x.l}</div>
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
                            <div key={x.l} style={{textAlign:"center",padding:"5px",...semBox(x.c)}}>
                              <div style={{fontSize:"7px",color:"#8fb4cc"}}>{x.l}</div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"13px",color:x.c}}>{x.v}</div>
                            </div>
                          )}
                        </div>
                      </div>
                      <div className="card" style={{padding:"12px"}}>
                        <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",marginBottom:"8px"}}>
                          <span style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>INDICADORES FXCA16</span>
                          <span style={{fontSize:"6px",color:"#5a8fa8"}}>correlación con retorno futuro ≈ 0</span>
                        </div>
                        {(()=>{
                          // Solo los que aportan lectura distinta entre sí. Se sacaron:
                          // WF peso y H-Factor (parámetros internos del modelo, sin
                          // significado operativo), roc5 (casi idéntico a mom5), y
                          // SMA20/50 + bandas de Bollinger (ya están en NIVELES y en
                          // el gráfico). De 13 filas a 6.
                          const base=[
                            {l:"ROC 10h",v:`${s.roc10>=0?"+":""}${s.roc10}%`,c:s.roc10>1.5?"#00ff9d":s.roc10<-1.5?"#ff3355":"#ffd700"},
                            {l:"Mom. 5h",v:`${s.mom5>=0?"+":""}${s.mom5}%`,c:s.mom5>=0?"#00ff9d":"#ff3355"},
                            {l:"MACD",   v:(s.macd>0?"▲ ":"▼ ")+Math.abs(s.macd),c:s.macd>0?"#00ff9d":"#ff3355"},
                            {l:"RSI",    v:s.rsi,c:s.rsi>70?"#ff3355":s.rsi<30?"#00ff9d":"#5a8fa8"},
                            {l:"Vol.Div.",v:s.volDiv>0?"▲ ACUM":s.volDiv<0?"▼ DIST":"─",c:s.volDiv>0?"#00ff9d":s.volDiv<0?"#ff3355":"#ffd700"},
                            ...(()=>{ const v=volVsMedia(sel.ticker, s.vol_24h, s);
                              return v ? [{l:v.fuente==="movil6m"?`Vol vs media 6m`:`Vol vs media (anual)`, v:`${v.dif>=0?"+":""}${v.dif} (${v.pct>=0?"+":""}${v.pct}%)`,
                                c:v.dif>0?"#00ff9d":v.dif<0?"#ff9040":"#ffd700"}] : []; })(),
                            ...(()=>{ const p=persistenciaDireccional(sel.ticker);
                              if(!p) return [];
                              const col = p.pred>0?"#00ff9d":"#ff3355";
                              return [{
                                l: p.validado ? "Persistencia" : "⚗ Persistencia",
                                v: `${p.pred>0?"▲":"▼"} ${p.pred>=0?"+":""}${p.pred}σ · ${p.regimen==="continuacion"?"CONT":"REV"}`,
                                c: p.validado ? col : "#5a8fa8",
                                punteado: !p.validado,
                                tip: p.validado
                                  ? `Persistencia direccional (LMSW), ventana ${p.ventana} ruedas. Régimen ${p.regimen==="continuacion"?"de continuación: el movimiento de hoy tiende a seguir":"de reversión: el movimiento de hoy tiende a revertirse"}. C2=${p.c2} (propio ${p.c2propio}, peso ${p.peso} — el resto viene del promedio del universo, porque con ${p.ventana} ruedas la estimación individual es ruidosa). Validado en Merval: por activo con encogimiento da +0.37% fuera de muestra, t=7.36 sobre 10 años, contra +0.26% del método global. Es criterio de ranking, NO señal de entrada (+0.37% diario vs 1.2% de comisión).`
                                  : `SIN VALIDAR en USD: el efecto no existe con ningún método ni ventana (todos los |t| < 1.92 sobre 206.425 obs, varios negativos). Se probaron 6 cortes distintos. Se muestra sólo como referencia. El indicador SÍ está validado en Merval.`
                              }]; })(),
                            {l:"Régimen",v:s.regime||"neutral",c:s.regime==="bull"?"#00ff9d":s.regime==="bear"?"#ff3355":"#ffd700"},
                          ];
                          const extra=[
                            {l:"ROC 5h", v:`${s.roc5>=0?"+":""}${s.roc5}%`, c:s.roc5>1?"#00ff9d":s.roc5<-1?"#ff3355":"#ffd700"},
                            {l:"SMA 20",  v:`$${s.sma20?.toFixed(0)??"─"}`,c:"#8b5cf6"},
                            {l:"SMA 50",  v:`$${s.sma50?.toFixed(0)??"─"}`,c:"#f59e0b"},
                            {l:"BB Sup.", v:`$${s.boll?.u?.toFixed(0)??"─"}`,c:"#3b82f6"},
                            {l:"BB Inf.", v:`$${s.boll?.l?.toFixed(0)??"─"}`,c:"#3b82f6"},
                          ];
                          const fila=x=>(
                            <div key={x.l} title={x.tip||undefined}
                              style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"4px 7px",marginBottom:"3px",fontSize:"9px",...semBox(x.c,"14"),
                                ...(x.punteado?{borderStyle:"dashed"}:{}), ...(x.tip?{cursor:"help"}:{})}}>
                              <span style={{color:"#8fb4cc"}}>{x.l}</span>
                              <span style={{color:x.c,fontWeight:700}}>{x.v}</span>
                            </div>
                          );
                          return (<>
                            {base.map(fila)}
                            {verIndicadores && extra.map(fila)}
                            <button onClick={()=>setVerIndicadores(v=>!v)}
                              style={{background:"transparent",border:"none",padding:"3px 0",cursor:"pointer",fontSize:"7px",color:"#4a7a9b",fontFamily:"inherit"}}>
                              {verIndicadores ? "▾ menos" : `▸ ${extra.length} indicadores más`}
                            </button>
                            <Nota titulo="qué tanto sirven estos indicadores">
                              Medido sobre 10 años y 110.806 observaciones: la correlación de estos
                              indicadores con el retorno futuro a 10 días es ≈0 (RSI 0.004, momentum 0.095).
                              Los que sí dan significativos apuntan en dirección <strong>contraria</strong> a como
                              los usa el score — momentum alto precede retornos menores — y el efecto es
                              5-6× menor al costo de operar. Sirven para describir el estado actual,
                              no para anticipar.
                            </Nota>
                          </>);
                        })()}
                      </div>
                    </div>}

                    {sel.bt.n>0 ? (<>
                    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(85px,1fr))",gap:"5px",marginBottom:"9px"}}>
                      {(verBacktest?[
                        {l:"EF%",v:`${sel.bt.hr}%`,c:g.c},
                        {l:"P.FACTOR",v:`${sel.bt.pf}x`,c:sel.bt.pf>=1.5?"#00ff9d":sel.bt.pf>=1?"#ffd700":"#ff3355"},
                        {l:"SHARPE",v:sel.bt.sh,c:sel.bt.sh>=1?"#00ff9d":sel.bt.sh>=0?"#ffd700":"#ff3355"},
                        {l:"MAX DD",v:`${sel.bt.dd}%`,c:sel.bt.dd<10?"#00ff9d":sel.bt.dd<20?"#ffd700":"#ff3355"},
                        {l:"TRADES",v:sel.bt.n,c:"#a0cce0"},
                        {l:"WINS",v:sel.bt.hits,c:"#00ff9d"},
                        {l:"LOSSES",v:sel.bt.n-sel.bt.hits,c:"#ff3355"},
                        {l:"AVG RET",v:`${sel.bt.avg>=0?"+":""}${sel.bt.avg}%`,c:sel.bt.avg>=0.3?"#00ff9d":sel.bt.avg>=0?"#ffd700":"#ff3355"},
                        {l:"EQUITY",v:sel.bt.eq,c:sel.bt.eq>=105?"#00ff9d":sel.bt.eq>=100?"#ffd700":"#ff3355"},
                      ]:[
                        {l:"EF%",v:`${sel.bt.hr}%`,c:g.c},
                        {l:"P.FACTOR",v:`${sel.bt.pf}x`,c:sel.bt.pf>=1.5?"#00ff9d":sel.bt.pf>=1?"#ffd700":"#ff3355"},
                        {l:"SHARPE",v:sel.bt.sh,c:sel.bt.sh>=1?"#00ff9d":sel.bt.sh>=0?"#ffd700":"#ff3355"},
                        {l:"MAX DD",v:`${sel.bt.dd}%`,c:sel.bt.dd<10?"#00ff9d":sel.bt.dd<20?"#ffd700":"#ff3355"},
                      ]).map(x=>
                        <div key={x.l} style={{padding:"7px",...semBox(x.c)}}>
                          <div style={{fontSize:"7px",color:"#8fb4cc",marginBottom:"2px"}}>{x.l}</div>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.v}</div>
                        </div>
                      )}
                    </div>
                    <button onClick={()=>setVerBacktest(v=>!v)}
                      style={{background:"transparent",border:"none",padding:"2px 0 8px",cursor:"pointer",fontSize:"7px",color:"#4a7a9b",fontFamily:"inherit"}}>
                      {verBacktest ? "▾ menos métricas" : "▸ 5 métricas más del backtest"}
                    </button>
                    </>) : (
                      <div style={{...semBox("#5a8fa8","0c"),padding:"9px 10px",marginBottom:"9px"}}>
                        <div style={{fontSize:"7px",color:"#8fb4cc"}}>
                          Sin operaciones en el backtest para este activo (0 trades) — no hay suficiente historia
                          o el sistema nunca disparó una señal accionable en el período. No es un "0%" de rendimiento,
                          es ausencia de datos.
                        </div>
                      </div>
                    )}

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
                              {(()=>{
                                // Con fallback a historial embebido (dtBarras) — sin esto,
                                // cualquier ticker con badge REAL (datos en vivo, pocas
                                // barras) se marcaba falsamente como "serie ilíquida" aunque
                                // tuviera historial completo. Caso real: BOLT.
                                const q = calidadSerie(dtBarras||[]);
                                if (q.nivel==="ok") return null;
                                const esDeg = q.nivel==="degradada";
                                const c = esDeg ? "#ff3355" : "#ff9040";
                                return <span style={{fontSize:"7px",padding:"2px 7px",background:`${c}20`,border:`1px solid ${c}50`,borderRadius:"3px",color:c,fontWeight:700}}>
                                  {esDeg ? "⚠ SERIE ILÍQUIDA" : "⚠ LIQUIDEZ BAJA"} — {q.motivo||"pocas operaciones"}. El precio quedó quieto por falta de operaciones, no por estabilidad: los indicadores acá miden ruido.
                                </span>;
                              })()}
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

                          {/* ── CALENDARIO DE BALANCE ── */}
                          {(()=>{
                            const e = estadoEarnings(sel.ticker);
                            const fmtF = f => { try { const [y,m,d]=f.split("-"); return `${d}/${m}/${y}`; } catch(_) { return f; } };

                            if (e.estado === "sin_dato") return (
                              <div style={{marginBottom:"12px",padding:"9px 10px",background:"#0c182640",border:"1px dashed #1e3a50",borderRadius:"6px"}}>
                                <div style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em",marginBottom:"3px"}}>📅 CALENDARIO DE BALANCE</div>
                                <div style={{fontSize:"8px",color:"#ffd700",lineHeight:1.7}}>
                                  Sin dato de calendario para {e.tk}. <strong>No significa que no reporte pronto</strong> —
                                  verificalo aparte antes de tomar posición.
                                </div>
                              </div>
                            );

                            const prox = e.diasProx;
                            const inminente = prox != null && prox >= 0 && prox <= 7;
                            const cerca     = prox != null && prox > 7 && prox <= 21;
                            const recien    = e.diasUltimo != null && e.diasUltimo >= 0 && e.diasUltimo <= 5;
                            const c = inminente ? "#ff3355" : recien ? "#00d4ff" : cerca ? "#ffd700" : "#00ff88";

                            return (
                              <div style={{marginBottom:"12px",padding:"10px",background:`${c}0d`,border:`1px solid ${c}35`,borderRadius:"6px"}}>
                                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"6px"}}>
                                  <div>
                                    <div style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em"}}>📅 CALENDARIO DE BALANCE</div>
                                    <div style={{fontSize:"7px",color:"#5a8fa8"}}>
                                      {e.estado==="aproximado" ? "⚠ fecha aproximada — sin confirmar" : "fecha del calendario oficial"}
                                    </div>
                                  </div>
                                  <div style={{textAlign:"right"}}>
                                    {prox!=null ? (<>
                                      <div style={{fontFamily:"'Bebas Neue'",fontSize:"24px",color:c,lineHeight:1}}>
                                        {prox<=0 ? "HOY" : `${prox}d`}
                                      </div>
                                      <div style={{fontSize:"7px",color:c}}>{fmtF(e.prox)}</div>
                                    </>) : (
                                      <div style={{fontSize:"8px",color:"#5a8fa8"}}>sin próxima fecha</div>
                                    )}
                                  </div>
                                </div>

                                {(e.ultimo||prox!=null)&&(
                                  <div style={{display:"grid",gridTemplateColumns:"repeat(2,1fr)",gap:"5px",marginBottom:"6px"}}>
                                    <div style={{padding:"5px 7px",background:"#050c15",borderRadius:"3px",textAlign:"center"}}>
                                      <div style={{fontSize:"6px",color:"#4a7a9b"}}>Próximo</div>
                                      <div style={{fontSize:"11px",fontFamily:"'Bebas Neue'",color:prox!=null?c:"#4a7a9b"}}>{prox!=null?fmtF(e.prox):"—"}</div>
                                    </div>
                                    <div style={{padding:"5px 7px",background:"#050c15",borderRadius:"3px",textAlign:"center"}}>
                                      <div style={{fontSize:"6px",color:"#4a7a9b"}}>Último reportado</div>
                                      <div style={{fontSize:"11px",fontFamily:"'Bebas Neue'",color:e.ultimo?"#a0cce0":"#4a7a9b"}}>{e.ultimo?fmtF(e.ultimo):"—"}</div>
                                    </div>
                                  </div>
                                )}

                                <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                                  {inminente
                                    ? "Reporta en días. La volatilidad alrededor del balance no la captura ningún indicador técnico: un gap por sorpresa puede saltarse tu stop. Considerá esperar al reporte o reducir el tamaño."
                                    : recien
                                    ? "Reportó hace pocos días. El movimiento reciente del precio probablemente responde a la noticia, no al patrón técnico — cuidado con leerlo como señal."
                                    : cerca
                                    ? "Reporta dentro del mes. Si tu horizonte de tenencia cruza esa fecha, el trade queda expuesto al evento."
                                    : "Sin balance cercano. La señal técnica no está compitiendo con un catalizador de calendario."}
                                </div>
                              </div>
                            );
                          })()}

                          {/* ── ALFA CROSS-SECTIONAL ── */}
                          {alphaRank?.[sel.ticker]&&(()=>{
                            const a=alphaRank[sel.ticker];
                            const c=a.quintil>=5?"#00ff88":a.quintil>=4?"#a0cce0":a.quintil<=1?"#ff3355":a.quintil<=2?"#ff9040":"#ffd700";
                            // Merval usa una fórmula DISTINTA a USA (iliquidez de Amihud +
                            // asimetría, en vez de shock de volumen + momentum) — ver
                            // ALPHA.ALPHA_AMBITO.ARS. rankearUniversoMerval() no devuelve
                            // vol_shock/mom_1m en el nivel superior del objeto (solo dentro
                            // de "raw", que son valores intermedios sin usar en su fórmula),
                            // así que leerlos ahí rompía el Detalle de los 20 tickers de
                            // Merval más líquidos con un TypeError silencioso — pantalla
                            // negra sin mensaje, porque la app no tiene Error Boundary.
                            const metricas = a.esMerval ? [
                              {l:"Iliquidez (Amihud)",     v:a.amihud,   bueno:a.amihud<0},
                              {l:"Asimetría de retornos",  v:a.skew_ret, bueno:a.skew_ret<0},
                            ] : [
                              {l:"Shock de volumen", v:a.vol_shock, bueno:a.vol_shock>0.3},
                              {l:"Momentum 1 mes",   v:a.mom_1m,    bueno:a.mom_1m<-0.3},
                            ];
                            const amb = a.esMerval ? ALPHA.ALPHA_AMBITO?.ARS : ALPHA.ALPHA_AMBITO?.USD;
                            return (
                              <div style={{marginBottom:"12px",padding:"10px",background:`${c}0d`,border:`1px solid ${c}35`,borderRadius:"6px"}}>
                                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"6px"}}>
                                  <div>
                                    <div style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".1em"}}>α ALFA CROSS-SECTIONAL</div>
                                    <div style={{fontSize:"7px",color:"#5a8fa8"}}>
                                      {a.esMerval ? "Iliquidez + asimetría (Merval)" : ALPHA.ALPHA_VALIDADA.nombre}
                                    </div>
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
                                  {metricas.map(x=>(
                                    <div key={x.l} style={{padding:"5px 7px",background:"#050c15",borderRadius:"3px"}}>
                                      <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                                      <div style={{fontSize:"11px",color:x.bueno?"#00ff88":"#a0cce0",fontFamily:"'Bebas Neue'"}}>
                                        {(x.v==null||!isFinite(x.v)) ? "—" : (x.v>=0?"+":"")+x.v.toFixed(2)}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                                <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                                  {a.esMerval ? (
                                    a.quintil>=5
                                    ? "En el quintil superior por iliquidez relativa y asimetría de retornos — la señal preliminar de Merval, distinta a la de USA."
                                    : a.quintil>=4
                                    ? "Por encima de la media del universo Merval en esta métrica preliminar."
                                    : a.quintil<=1
                                    ? "En el quintil inferior de la señal preliminar de Merval."
                                    : "En la zona media del universo Merval: sin ventaja relativa clara."
                                  ) : (
                                    a.quintil>=5
                                    ? "En el quintil superior del universo. La combinación de volumen entrando sobre precio castigado es el patrón con mejor evidencia del sistema."
                                    : a.quintil>=4
                                    ? "Por encima de la media del universo en atractivo relativo."
                                    : a.quintil<=1
                                    ? "En el quintil inferior. Históricamente este grupo rinde por debajo del universo."
                                    : "En la zona media del universo: sin ventaja relativa clara."
                                  )}
                                </div>
                                <div style={{marginTop:"5px",paddingTop:"5px",borderTop:"1px solid #0f2235",fontSize:"6px",color:"#4a7a9b",lineHeight:1.6}}>
                                  {a.esMerval ? (<>
                                    Fórmula distinta a la de USA: iliquidez de Amihud + asimetría de retornos, sobre los 20 papeles más líquidos.<br/>
                                    <strong style={{color:"#ffd700"}}>Preliminar</strong> — muestra chica ({amb?.nUniverso ?? 20} tickers × pocas fechas):
                                    IC {amb?.ic ?? "—"} · t={amb?.t ?? "—"}. {amb?.nota || "Pendiente de confirmar con más historia."}
                                  </>) : (<>
                                    Promedio de los últimos {ALPHA.ALPHA_VALIDADA.metricas.suavizado} días — suavizar cancela el ruido de un solo día
                                    {a.diasPromediados ? ` (${a.diasPromediados} días con datos)` : ""}.<br/>
                                    Validado: IC {ALPHA.ALPHA_VALIDADA.metricas.ic} · IR {ALPHA.ALPHA_VALIDADA.metricas.ir} ·
                                    t={ALPHA.ALPHA_VALIDADA.metricas.t} · positivo en {ALPHA.ALPHA_VALIDADA.metricas.pctFechas}% de las fechas
                                  </>)}
                                </div>
                                {!a.esMerval&&(
                                <Nota titulo="⚠ esa validación no se sostiene fuera de su ventana" color="#ff6680">
                                  El IC/IR de arriba se midió sobre la serie horaria (~1 año), que es donde se construyó la fórmula.
                                  Reproducida sobre los <strong>10 años de serie diaria</strong> — datos que el modelo nunca vio — el
                                  ranking <strong>no discrimina</strong>: la monotonía entre quintil y exceso da <strong>-0.66</strong> (invertida)
                                  y el t de Q5 da <strong>0.00</strong>. Solo funciona en 2026, el período sobre el que se desarrolló.
                                  <br/><br/>
                                  Lo que sí es cierto: el alfa dispara <em>antes</em> que el score técnico (Q5 compra papeles que vienen -6.9%
                                  en 20 días, contra el score que marca COMPRA FUERTE después de +13.7%). El mecanismo es el correcto;
                                  el problema es que el ranking no predice. Ver <code>docs/hallazgos.md</code>.
                                </Nota>
                                )}
                              </div>
                            );
                          })()}

                          {sel.moneda==="ARS"&&!alphaRank?.[sel.ticker]&&(
                            <div style={{marginBottom:"12px",padding:"8px 10px",background:"#ff904010",border:"1px solid #ff904030",borderRadius:"5px"}}>
                              <div style={{fontSize:"7px",color:"#ff9040",fontWeight:700,marginBottom:"3px"}}>α SIN RANKING PARA {sel.ticker}</div>
                              <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                                Solo cubre los 20 papeles más líquidos del Merval — el resto mete más ruido que señal.
                              </div>
                            </div>
                          )}
                          {sel.moneda==="ARS"&&alphaRank?.[sel.ticker]&&(
                            <div style={{marginBottom:"12px",padding:"7px 10px",background:"#ffd70008",border:"1px solid #ffd70025",borderRadius:"5px",fontSize:"7px",color:"#ffd700",lineHeight:1.6}}>
                              ⚗️ Señal PRELIMINAR de Merval — fórmula distinta a USA, validada sobre poca historia (20×50 fechas).
                            </div>
                          )}

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
                                  Datos de HOY, no series point-in-time — no alimentan el score, solo advierten sobre fragilidad.
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

                    {sel.bt.n>0 && (<>
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
                    </>)}
                  </div>;
                })()}

                {/* ══════════════════════════════════════════════════
                    NOVEDADES — agregados recientes van acá, al final,
                    para poder ver de un vistazo qué se fue sumando.
                    ══════════════════════════════════════════════════ */}
                {(()=>{
                  const rsiAct = sel?.sig?.rsi;
                  const b = bandaRSI(rsiAct);
                  return (
                    <div style={{marginTop:"18px",paddingTop:"14px",borderTop:"2px dashed #1e3a50"}}>
                      <div style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".2em",marginBottom:"10px"}}>
                        ⊕ AGREGADOS RECIENTES
                      </div>

                    {/* ── YA SE HABÍA MOVIDO ANTES ──
                        Misma lógica que en Replay, con datos de hoy: contextualiza
                        la señal actual con cuánto ya se movió el precio antes de
                        que se encendiera. Conecta directo con lo medido en
                        hallazgos.md — el score se enciende, en mediana, con el
                        54% de la suba ya consumida. Se calcula sobre la serie
                        DIARIA (no horaria) para que sea comparable con Replay. */}
                    {sel?.data?.length>0&&(()=>{
                      const dias = construirDiasDe(sel.data);
                      if (dias.length < 21) return null;
                      const px = dias[dias.length-1].close;
                      const prev = n => dias.length>n ? (px/dias[dias.length-1-n].close-1)*100 : null;
                      const p5=prev(5), p10=prev(10), p20=prev(20);
                      const pc = v => v==null?"—":(v>=0?"+":"")+v.toFixed(1)+"%";
                      const cc = v => v==null?"#5a8fa8":v>0?"#00ff88":v<0?"#ff3355":"#8fb4cc";
                      const grande = p20!=null && Math.abs(p20)>8;
                      return (
                        <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                          <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"7px"}}>YA SE HABÍA MOVIDO ANTES</div>
                          <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"6px",marginBottom:grande?"8px":"0"}}>
                            {[["5d",p5],["10d",p10],["20d",p20]].map(([l,v])=>(
                              <div key={l} style={{padding:"7px",textAlign:"center",background:"#050c15",borderRadius:"4px"}}>
                                <div style={{fontSize:"6px",color:"#5a8fa8"}}>{l} previos</div>
                                <div style={{fontSize:"14px",fontFamily:"'Bebas Neue'",color:cc(v)}}>{pc(v)}</div>
                              </div>
                            ))}
                          </div>
                          {grande&&(
                            <div style={{fontSize:"7px",color:"#8fb4cc",lineHeight:1.6}}>
                              Ya se movió {pc(p20)} en 20 días. La señal técnica se calcula <em>a partir</em> del precio, no lo
                              anticipa — en mediana se enciende con más de la mitad del movimiento ya ocurrido.{" "}
                              <button onClick={()=>{ setRpInput(sel.ticker); setRpTicker(sel.ticker); setRpSel(null); setRpCalc(null); setTab("replay"); }}
                                style={{background:"none",border:"none",color:"#00d4ff",textDecoration:"underline",cursor:"pointer",fontFamily:"inherit",fontSize:"7px",padding:0}}>
                                Ver en Replay
                              </button>
                            </div>
                          )}
                        </div>
                      );
                    })()}

                    {/* ── VARIACIÓN DIARIA (gráfico interactivo) ──
                        Misma pieza que en Replay, embebida acá para no
                        tener que ir a otra pestaña: tocando una barra se
                        recalcula la señal cortando la serie en esa fecha
                        (sin ver nada posterior) y se compara contra lo
                        que pasó después. */}
                    {dtDias.length>=20&&(
                      <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"6px",flexWrap:"wrap",gap:"6px"}}>
                          <span style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>VARIACIÓN DIARIA</span>
                          <div style={{display:"flex",gap:"3px"}}>
                            {[5,10,15,30,60,90].map(v=>(
                              <button key={v} className={`btn ${dtVent===v?"on":"off"}`} style={{padding:"3px 7px",fontSize:"7px"}}
                                onClick={()=>setDtVent(v)}>{v}D</button>
                            ))}
                          </div>
                        </div>
                        <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"8px"}}>Barra = variación % de cierre a cierre, cada día.</div>
                        {(()=>{
                          const maxAbs = Math.max(...dtVisibles.map(d=>Math.abs(d.ret)), 1);
                          return (
                            <div style={{display:"flex",alignItems:"stretch",gap:"2px",height:"110px",marginBottom:"4px"}}>
                              {dtVisibles.map(d=>{
                                const h = Math.max(3, Math.abs(d.ret)/maxAbs*46);
                                const act = dtSel===d.date;
                                return (
                                  <div key={d.date} onClick={()=>dtAnalizar(d)}
                                    title={d.date+"  "+(d.ret>=0?"+":"")+d.ret.toFixed(1)+"%"}
                                    style={{flex:1,display:"flex",flexDirection:"column",justifyContent:"center",cursor:"pointer",
                                      background:act?"#00d4ff18":"transparent",borderRadius:"3px",padding:"0 1px",
                                      border:act?"1px solid #00d4ff60":"1px solid transparent"}}>
                                    <div style={{height:"50%",display:"flex",alignItems:"flex-end"}}>
                                      {d.ret>=0&&<div style={{width:"100%",height:`${h}%`,background:act?"#00ff88":"#00ff8899",borderRadius:"2px 2px 0 0"}}/>}
                                    </div>
                                    <div style={{height:"1px",background:"#1e3a50"}}/>
                                    <div style={{height:"50%"}}>
                                      {d.ret<0&&<div style={{width:"100%",height:`${h}%`,background:act?"#ff3355":"#ff335599",borderRadius:"0 0 2px 2px"}}/>}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          );
                        })()}
                        <div style={{display:"flex",justifyContent:"space-between",fontSize:"6px",color:"#5a8fa8",marginBottom:dtCalc||dtCargando?"9px":"0"}}>
                          <span>{dtVisibles[0]?.date}</span>
                          <span>tocá una barra para ver qué decía el sistema ese día</span>
                          <span>{dtVisibles[dtVisibles.length-1]?.date}</span>
                        </div>

                        {dtCargando&&(
                          <div style={{textAlign:"center",padding:"10px",color:"#00d4ff",fontSize:"8px"}}>⏳ Recalculando la señal al {dtSel}...</div>
                        )}
                        {dtCalc&&!dtCargando&&(dtCalc.error?(
                          <div style={{color:"#ff3355",fontSize:"8px"}}>Error: {dtCalc.error}</div>
                        ):(()=>{
                          const sg=dtCalc.sig, col=SC[sg?.sig]||"#5a8fa8";
                          const compra=(sg?.sig||"").includes("COMPRA"), venta=(sg?.sig||"").includes("VENTA");
                          const hz=[[20,dtCalc.fwd20],[10,dtCalc.fwd10],[5,dtCalc.fwd5],[3,dtCalc.fwd3],[1,dtCalc.fwd1]].find(([,v])=>v!=null);
                          const ok = hz ? (compra?hz[1]>0:venta?hz[1]<0:null) : null;
                          const pc=v=>v==null?"—":(v>=0?"+":"")+v.toFixed(1)+"%";
                          const cc=v=>v==null?"#5a8fa8":v>0?"#00ff88":v<0?"#ff3355":"#8fb4cc";
                          return (
                            <div style={{borderTop:"1px solid #1e3a50",paddingTop:"9px"}}>
                              <div style={{display:"flex",alignItems:"center",gap:"8px",marginBottom:"8px",flexWrap:"wrap"}}>
                                <span style={{fontFamily:"'Bebas Neue'",fontSize:"18px",color:col}}>{sg?.sig||"—"}</span>
                                <span style={{fontSize:"8px",color:"#8fb4cc"}}>{dtCalc.fecha} · conf {sg?.conf??"—"}% · RSI {sg?.rsi??"—"} · precio {dtCalc.px?.toFixed(2)}</span>
                              </div>
                              <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:"3px",marginBottom:"6px"}}>
                                {[["+1d",dtCalc.fwd1],["+3d",dtCalc.fwd3],["+5d",dtCalc.fwd5],["+10d",dtCalc.fwd10],["+20d",dtCalc.fwd20]].map(([l,v])=>(
                                  <div key={l} style={{padding:"6px 2px",textAlign:"center",...(v!=null?semBox(cc(v),"14"):{background:"#07121c",borderRadius:"4px",border:"1px dashed #1e3a50"})}}>
                                    <div style={{fontSize:"6px",color:"#8fb4cc"}}>{l}</div>
                                    <div style={{fontSize:"11px",fontFamily:"'Bebas Neue'",color:v!=null?cc(v):"#3a5a70"}}>{v!=null?pc(v):"·"}</div>
                                  </div>
                                ))}
                              </div>
                              {ok!==null&&hz&&(
                                <div style={{...semBox(ok?"#00ff88":"#ff3355","14"),padding:"7px",fontSize:"7px",color:ok?"#00ff88":"#ff3355",fontWeight:700}}>
                                  {ok?`✓ Acertó la dirección a ${hz[0]} días`:`✕ Erró la dirección a ${hz[0]} días`}
                                  {hz[0]<20&&<span style={{color:"#8fb4cc",fontWeight:400}}> (parcial)</span>}
                                </div>
                              )}
                              {!hz&&<div style={{fontSize:"7px",color:"#5a8fa8"}}>Fecha muy reciente, todavía no hay días posteriores para medir. Probá con 60D/90D.</div>}
                            </div>
                          );
                        })())}
                      </div>
                    )}

                      <div className="card" style={{padding:"12px"}}>
                        <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",marginBottom:"3px"}}>
                          <span style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>📊 RSI — CONTEXTO HISTÓRICO</span>
                          <span style={{fontSize:"6px",color:"#5a8fa8"}}>153 activos · 10 años · 363.746 obs.</span>
                        </div>
                        <div style={{fontSize:"7px",color:"#5a8fa8",lineHeight:1.6,marginBottom:"9px"}}>
                          Dónde está parado este RSI según la historia. Es estadística descriptiva,
                          <strong style={{color:"#8fb4cc"}}> no una señal de compra o venta</strong>.
                        </div>

                        {b ? (
                          <div style={{...semBox(b.pUp>=RSI_BASE_PROM_UP?"#00ff88":"#ffd700","14"),padding:"9px",marginBottom:"9px"}}>
                            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"5px"}}>
                              <span style={{fontSize:"8px",color:"#8fb4cc"}}>RSI actual <strong style={{color:"#e8f4ff",fontSize:"11px"}}>{rsiAct}</strong> → banda {b.lo}-{b.hi===101?"100":b.hi}</span>
                              <span style={{fontFamily:"'Bebas Neue'",fontSize:"18px",color:b.pUp>=RSI_BASE_PROM_UP?"#00ff88":"#ffd700"}}>{b.pUp}%</span>
                            </div>
                            <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                              En esta banda: <strong>+4% en 1-4d</strong> el {b.pUp}% de las veces (prom. {RSI_BASE_PROM_UP}%) ·
                              -4% el {b.pDn}% · retorno medio a 4d: {b.fwd4>=0?"+":""}{b.fwd4}%.
                            </div>
                          </div>
                        ) : (
                          <div style={{fontSize:"8px",color:"#5a8fa8",marginBottom:"9px"}}>Sin RSI disponible para este activo.</div>
                        )}

                        <button onClick={()=>setVerTablaRsi(v=>!v)}
                          style={{background:"transparent",border:"none",padding:"3px 0",cursor:"pointer",fontSize:"7px",color:"#4a7a9b",fontFamily:"inherit"}}>
                          {verTablaRsi ? "▾ ocultar tabla por banda" : "▸ ver las 8 bandas"}
                        </button>
                        {verTablaRsi && <div style={{fontSize:"6px",color:"#5a8fa8",margin:"3px 0 4px"}}>Barra = probabilidad histórica de salto +4% en 1-4 días, por banda de RSI.</div>}
                        {verTablaRsi && RSI_TASAS_BASE.map(x=>{
                          const activa = b && x.lo===b.lo;
                          const w = Math.round(x.pUp/35*100);
                          return (
                            <div key={x.lo} style={{display:"flex",alignItems:"center",gap:"6px",padding:"2px 5px",marginBottom:"1px",
                              borderRadius:"3px",background:activa?"#00d4ff14":"transparent",border:activa?"1px solid #00d4ff40":"1px solid transparent"}}>
                              <span style={{fontSize:"7px",color:activa?"#00d4ff":"#8fb4cc",width:"44px",flexShrink:0,fontWeight:activa?700:400}}>
                                {x.lo}-{x.hi===101?"100":x.hi}
                              </span>
                              <div style={{flex:1,height:"7px",background:"#050c15",borderRadius:"2px",overflow:"hidden"}}>
                                <div style={{width:`${w}%`,height:"100%",background:activa?"#00d4ff":"#2d6a8f"}}/>
                              </div>
                              <span style={{fontSize:"7px",color:activa?"#00d4ff":"#8fb4cc",width:"34px",textAlign:"right"}}>{x.pUp}%</span>
                              <span style={{fontSize:"6px",color:"#ff6680",width:"34px",textAlign:"right"}}>-{x.pDn}%</span>
                            </div>
                          );
                        })}

                        <Nota titulo="ojo con leer esto como señal">
                          <div style={{fontSize:"7px",color:"#ffb380",lineHeight:1.7}}>
                            La relación es una <strong>U, no una rampa</strong>:
                            los dos extremos (RSI&lt;30 y RSI&gt;70) preceden movimientos más grandes en <em>ambas</em> direcciones — es
                            volatilidad, no dirección. El medio (45-55) es la zona más quieta.
                            El spread entre RSI bajo y alto promedia 0.82 pp, <strong>por debajo del costo de operar (1.2-1.8% ida y vuelta)</strong>,
                            así que no alcanza como regla de entrada por sí solo.
                            El efecto pasa el test de consistencia (81 de 118 meses = 69%, umbral 65%) pero viene flojo últimamente (5 de los últimos 12 meses).
                          </div>
                        </Nota>
                      </div>

                      {/* ── PATRONES DE VELA — CONTEXTO HISTÓRICO ── */}
                      {(()=>{
                        const cand = detectCandlePattern(sel?.data||[]);
                        const actual = cand?.patterns?.[0]?.name || null;
                        const fila = actual ? VELAS_TASAS_BASE.find(v=>v.clave===actual) : null;
                        const colVer = v => v==="pasa"?"#00ff88":v==="parcial"?"#ffd700":"#5a8fa8";
                        return (
                          <div className="card" style={{padding:"12px",marginTop:"9px"}}>
                            <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",marginBottom:"3px"}}>
                              <span style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>🕯️ PATRONES DE VELA — CONTEXTO HISTÓRICO</span>
                              <span style={{fontSize:"6px",color:"#5a8fa8"}}>153 activos · 10 años</span>
                            </div>
                            <div style={{fontSize:"7px",color:"#5a8fa8",lineHeight:1.6,marginBottom:"9px"}}>
                              Qué rindió realmente cada patrón, medido — al lado de la etiqueta que le da el manual.
                            </div>

                            {fila ? (
                              <div style={{...semBox(colVer(fila.veredicto),"14"),padding:"9px",marginBottom:"9px"}}>
                                <div style={{fontSize:"8px",color:"#8fb4cc",marginBottom:"4px"}}>
                                  Patrón detectado hoy: <strong style={{color:"#e8f4ff",fontSize:"10px"}}>{actual}</strong>
                                  <span style={{color:"#5a8fa8"}}> (el manual lo llama {fila.etiqueta})</span>
                                </div>
                                <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                                  Medido sobre {fila.n.toLocaleString()} casos: <strong>{fila.fwd4>=0?"+":""}{fila.fwd4}%</strong> a 4d (baseline {VELAS_BASELINE.fwd4}%) —
                                  <strong style={{color:colVer(fila.veredicto)}}> {fila.veredicto==="pasa"?"pasa los tests":fila.veredicto==="parcial"?"falla consistencia mensual":"sin efecto medible"}</strong>.
                                </div>
                              </div>
                            ) : (
                              <div style={{fontSize:"8px",color:"#5a8fa8",marginBottom:"9px"}}>Sin patrón de vela detectado hoy en este activo.</div>
                            )}

                            <button onClick={()=>setVerTablaVelas(v=>!v)}
                              style={{background:"transparent",border:"none",padding:"3px 0",cursor:"pointer",fontSize:"7px",color:"#4a7a9b",fontFamily:"inherit"}}>
                              {verTablaVelas ? "▾ ocultar todos los patrones" : "▸ ver los 8 patrones medidos"}
                            </button>
                            {verTablaVelas && <div style={{fontSize:"6px",color:"#5a8fa8",margin:"3px 0 4px"}}>Barra = exceso de retorno a 4 días vs. baseline, controlado por volatilidad.</div>}
                            {verTablaVelas && VELAS_TASAS_BASE.map(v=>{
                              const act = actual===v.clave;
                              const c = colVer(v.veredicto);
                              return (
                                <div key={v.clave} style={{display:"flex",alignItems:"center",gap:"5px",padding:"3px 5px",marginBottom:"1px",borderRadius:"3px",
                                  background:act?"#00d4ff14":"transparent",border:act?"1px solid #00d4ff40":"1px solid transparent"}}>
                                  <span style={{fontSize:"7px",color:act?"#00d4ff":"#8fb4cc",width:"86px",flexShrink:0,fontWeight:act?700:400}}>{v.clave}</span>
                                  <span style={{fontSize:"6px",width:"38px",flexShrink:0,color:v.etiqueta==="alcista"?"#00ff8880":v.etiqueta==="bajista"?"#ff335580":"#5a8fa8"}}>{v.etiqueta}</span>
                                  <div style={{flex:1,height:"6px",background:"#050c15",borderRadius:"2px",position:"relative",overflow:"hidden"}}>
                                    <div style={{position:"absolute",left:"50%",top:0,bottom:0,width:"1px",background:"#1e3a50"}}/>
                                    <div style={{position:"absolute",top:0,bottom:0,background:v.exc>=0?"#2d8f6a":"#8f2d3d",
                                      left:v.exc>=0?"50%":`${50-Math.min(45,Math.abs(v.exc)*100)}%`,
                                      width:`${Math.min(45,Math.abs(v.exc)*100)}%`}}/>
                                  </div>
                                  <span style={{fontSize:"6px",color:v.exc>=0?"#00ff88":"#ff6680",width:"36px",textAlign:"right"}}>{v.exc>=0?"+":""}{v.exc}</span>
                                  <span style={{fontSize:"6px",color:c,width:"22px",textAlign:"right"}}>{v.veredicto==="pasa"?"★":v.veredicto==="parcial"?"~":"—"}</span>
                                </div>
                              );
                            })}

                            <Nota titulo="las etiquetas del manual no se sostienen">
                              <div style={{fontSize:"7px",color:"#ffb380",lineHeight:1.7}}>
                                De 15 patrones medidos, solo 2 sobreviven
                                corrección por comparaciones múltiples más control de volatilidad, y <strong>solo 1 pasa además el test de
                                consistencia mensual: "3 Cuervos"</strong> — que es un patrón <em>bajista</em> de manual y predice retornos
                                <em> positivos</em> (+0.166 pp, t=3.40, gana en 70% de los meses).
                                En el otro extremo, "Martillo" (la reversión alcista clásica) es el <strong>peor</strong> de los 15 medidos.
                                <br/><br/>
                                Y aun el mejor caso mueve 0.2-0.3 pp, <strong>muy por debajo del costo de operar (1.2-1.8% ida y vuelta)</strong>:
                                ninguno alcanza como regla de entrada por sí solo. Sirven para describir lo que pasó, no para pronosticar.
                              </div>
                            </Nota>
                          </div>
                        );
                      })()}


                    </div>
                  );
                })()}
              </div>
            )}

            {/* ══ TAB: SEGUIMIENTO ══ */}
            {/* ══ TAB: TRACKER — seguimiento con evidencia hacia adelante ══ */}
            {paneTab==="track"&&(()=>{
              const activos  = tracker.filter(t=>!t.cerrado);
              const cerrados = tracker.filter(t=>t.cerrado);

              // Precio actual: primero rowDataRef (histórico vivo), luego rows (última corrida)
              const precioActual = (tk) => {
                const b = rowDataRef.current[tk];
                if (b?.length) return b[b.length-1].close;
                return rows.find(r=>r.ticker===tk)?.price ?? null;
              };

              const calcResultado = (t) => {
                const px = t.cerrado ? t.precioCierre : precioActual(t.ticker);
                if (px == null || !t.precioMarca) return null;
                const ret = (px - t.precioMarca) / t.precioMarca * 100;
                const dirEsperada = t.señalMarca?.includes("COMPRA") ? 1 : t.señalMarca?.includes("VENTA") ? -1 : 0;
                const acierto = dirEsperada !== 0 ? (ret * dirEsperada > 0) : null;
                const dias = Math.max(0, Math.round((new Date(t.cerrado?t.fechaCierre:new Date()) - new Date(t.fechaMarca)) / 86400000));
                return { px, ret, acierto, dias };
              };

              // Estadística agregada — la evidencia real del sistema
              const conResultado = tracker.map(t=>({...t, r:calcResultado(t)})).filter(t=>t.r);
              const nTotal = conResultado.length;
              const aciertos = conResultado.filter(t=>t.r.acierto===true).length;
              const fallos   = conResultado.filter(t=>t.r.acierto===false).length;
              const conVeredicto = aciertos+fallos;
              const winRate = conVeredicto ? Math.round(aciertos/conVeredicto*100) : null;
              const retProm = nTotal ? (conResultado.reduce((a,t)=>a+t.r.ret,0)/nTotal) : null;

              const TrackCard = ({t}) => {
                const r = calcResultado(t);
                if (!r) return null;
                const dirAlcista = t.señalMarca?.includes("COMPRA");
                const color = r.acierto===true ? "#00ff88" : r.acierto===false ? "#ff3355" : "#ffd700";
                return (
                  <div className="card" style={{padding:"12px",marginBottom:"8px",borderLeft:`3px solid ${color}`}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:"7px"}}>
                      <div>
                        <div style={{display:"flex",alignItems:"center",gap:"6px"}}>
                          <span style={{fontFamily:"'Bebas Neue'",fontSize:"20px",color:"#e8f4ff"}}>{t.ticker}</span>
                          <span className="badge" style={{background:SC[t.señalMarca]+"20",color:SC[t.señalMarca],border:`1px solid ${SC[t.señalMarca]}40`,fontSize:"7px"}}>{t.señalMarca}</span>
                          {t.cerrado&&<span style={{fontSize:"7px",padding:"1px 6px",background:"#4a7a9b20",borderRadius:"3px",color:"#a0cce0"}}>CERRADO</span>}
                        </div>
                        <div style={{fontSize:"7px",color:"#5a8fa8",marginTop:"2px"}}>
                          Marcado {new Date(t.fechaMarca).toLocaleDateString('es-AR')} · {r.dias}d atrás · W{t.wMarca}D
                        </div>
                      </div>
                      <div style={{textAlign:"right"}}>
                        <div style={{fontFamily:"'Bebas Neue'",fontSize:"22px",color}}>{r.ret>=0?"+":""}{r.ret.toFixed(2)}%</div>
                        {r.acierto!=null&&<div style={{fontSize:"7px",color,fontWeight:700}}>{r.acierto?"✓ acertó dirección":"✗ falló dirección"}</div>}
                      </div>
                    </div>

                    <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"5px",marginBottom:"7px"}}>
                      {[
                        {l:"PRECIO MARCA", v:FP(t.precioMarca,t.moneda)},
                        {l:t.cerrado?"PRECIO CIERRE":"PRECIO HOY", v:FP(r.px,t.moneda)},
                        {l:"SCORE / CONF", v:`${t.scoreMarca} / ${t.confMarca}%`},
                      ].map(x=>(
                        <div key={x.l} style={{textAlign:"center",padding:"4px",background:"#050c15",borderRadius:"3px"}}>
                          <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                          <div style={{fontSize:"11px",fontFamily:"'Bebas Neue'",color:"#e8f4ff"}}>{x.v}</div>
                        </div>
                      ))}
                    </div>

                    {(t.alphaMarca!=null || t.calidadMarca!=null) && (
                      <div style={{display:"flex",gap:"8px",marginBottom:"7px",fontSize:"7px",color:"#a0cce0"}}>
                        {t.alphaMarca!=null && <span>{t.alphaPreliminar?"⚗":""}α percentil {t.alphaMarca}{t.alphaPreliminar?" (preliminar)":""}</span>}
                        {t.calidadMarca!=null && <span>· Calidad {t.calidadMarca}/100</span>}
                        {t.rrMarca!=null && <span>· R/R {t.rrMarca}x</span>}
                      </div>
                    )}

                    {/* Barra visual: qué tanto se movió respecto a lo esperado */}
                    <div style={{marginBottom:"7px"}}>
                      <div style={{height:"6px",background:"#0c1826",borderRadius:"3px",position:"relative",overflow:"hidden"}}>
                        <div style={{position:"absolute",left:"50%",top:0,bottom:0,width:"1px",background:"#1e3a50"}}/>
                        <div style={{position:"absolute",top:0,bottom:0,borderRadius:"3px",
                          left: r.ret>=0 ? "50%" : `${50-Math.min(48,Math.abs(r.ret)*4)}%`,
                          width:`${Math.min(48,Math.abs(r.ret)*4)}%`,
                          background: r.ret>=0?"#00ff88":"#ff3355", opacity:.85}}/>
                      </div>
                    </div>

                    {!t.cerrado ? (
                      <div style={{display:"flex",gap:"6px"}}>
                        <button className="btn off" onClick={()=>{setSel(rows.find(x=>x.ticker===t.ticker)||{ticker:t.ticker,moneda:t.moneda,name:t.nombre});setTab("det");}}
                          style={{flex:1,fontSize:"8px",padding:"5px"}}>🔍 Ver detalle actual</button>
                        <button className="btn off" onClick={()=>cerrarSeguimiento(t.id, r.px)}
                          style={{flex:1,fontSize:"8px",padding:"5px",color:"#a0cce0"}}>✓ Cerrar seguimiento</button>
                        <button className="btn off" onClick={()=>quitarSeguimiento(t.id)}
                          style={{fontSize:"8px",padding:"5px 8px",color:"#ff3355"}}>✕</button>
                      </div>
                    ) : (
                      <button className="btn off" onClick={()=>quitarSeguimiento(t.id)}
                        style={{width:"100%",fontSize:"8px",padding:"5px",color:"#ff3355"}}>✕ Eliminar registro</button>
                    )}
                  </div>
                );
              };

              return (
                <div className="fade">
                  <div style={{padding:"9px 12px",background:"#07101a",border:"1px solid #1e3a50",borderRadius:"6px",marginBottom:"12px"}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"4px"}}>📌 TRACKER — EVIDENCIA HACIA ADELANTE</div>
                    <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.7}}>
                      Congela la predicción en ese momento (precio, señal, score) y no se puede editar después —
                      es evidencia que ninguna validación retrospectiva reemplaza.
                    </div>
                  </div>

                  {/* Estadística agregada */}
                  {nTotal>0&&(
                    <div className="card" style={{padding:"12px",marginBottom:"12px"}}>
                      <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".1em",marginBottom:"8px"}}>RESULTADO ACUMULADO</div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"6px"}}>
                        {[
                          {l:"REGISTROS", v:nTotal, c:"#e8f4ff"},
                          {l:"WIN RATE",  v:winRate!=null?`${winRate}%`:"—", c:winRate>=55?"#00ff88":winRate!=null&&winRate<45?"#ff3355":"#ffd700"},
                          {l:"RET. PROMEDIO", v:retProm!=null?`${retProm>=0?"+":""}${retProm.toFixed(2)}%`:"—", c:retProm>0?"#00ff88":"#ff3355"},
                          {l:"ACTIVOS", v:activos.length, c:"#a0cce0"},
                        ].map(x=>(
                          <div key={x.l} style={{textAlign:"center",padding:"6px 3px",background:"#050c15",borderRadius:"4px"}}>
                            <div style={{fontSize:"6px",color:"#4a7a9b"}}>{x.l}</div>
                            <div style={{fontFamily:"'Bebas Neue'",fontSize:"17px",color:x.c}}>{x.v}</div>
                          </div>
                        ))}
                      </div>
                      {nTotal<20 && (
                        <div style={{marginTop:"8px",padding:"6px 8px",background:"#ffd70010",borderRadius:"3px",fontSize:"7px",color:"#ffd700",lineHeight:1.6}}>
                          Con {nTotal} registro{nTotal!==1?"s":""} la muestra todavía es chica para sacar conclusiones.
                          A partir de ~30-40 empieza a ser representativa.
                        </div>
                      )}
                      {(()=>{
                        // Separar el desempeño de α validado (USA) vs α preliminar (Merval),
                        // para poder confirmar si el Merval termina sosteniéndose o no.
                        const conAlpha = conResultado.filter(t=>t.alphaMarca!=null);
                        const usa = conAlpha.filter(t=>!t.alphaPreliminar);
                        const merval = conAlpha.filter(t=>t.alphaPreliminar);
                        if (!usa.length && !merval.length) return null;
                        const resumen = (arr) => {
                          if (!arr.length) return null;
                          const ret = arr.reduce((a,t)=>a+t.r.ret,0)/arr.length;
                          const con = arr.filter(t=>t.r.acierto!=null);
                          const wr = con.length ? Math.round(con.filter(t=>t.r.acierto).length/con.length*100) : null;
                          return { n:arr.length, ret, wr };
                        };
                        const rU = resumen(usa), rM = resumen(merval);
                        return (
                          <div style={{marginTop:"8px",display:"grid",gridTemplateColumns:"1fr 1fr",gap:"6px"}}>
                            <div style={{padding:"7px 8px",background:"#00ff8808",border:"1px solid #00ff8825",borderRadius:"4px"}}>
                              <div style={{fontSize:"6px",color:"#4a7a9b"}}>α USA — VALIDADO</div>
                              {rU ? (
                                <div style={{fontSize:"9px",color:"#00ff88"}}>
                                  n={rU.n} · {rU.wr!=null?`${rU.wr}% acierto`:"—"} · {rU.ret>=0?"+":""}{rU.ret.toFixed(2)}%
                                </div>
                              ) : <div style={{fontSize:"8px",color:"#4a7a9b"}}>sin registros aún</div>}
                            </div>
                            <div style={{padding:"7px 8px",background:"#ffd70008",border:"1px dashed #ffd70030",borderRadius:"4px"}}>
                              <div style={{fontSize:"6px",color:"#4a7a9b"}}>⚗ MERVAL — PRELIMINAR</div>
                              {rM ? (
                                <div style={{fontSize:"9px",color:"#ffd700"}}>
                                  n={rM.n} · {rM.wr!=null?`${rM.wr}% acierto`:"—"} · {rM.ret>=0?"+":""}{rM.ret.toFixed(2)}%
                                </div>
                              ) : <div style={{fontSize:"8px",color:"#4a7a9b"}}>sin registros aún</div>}
                            </div>
                          </div>
                        );
                      })()}
                    </div>
                  )}

                  {activos.length>0 && (
                    <>
                      <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".1em",marginBottom:"6px"}}>EN SEGUIMIENTO ({activos.length})</div>
                      {activos.map(t=><TrackCard key={t.id} t={t}/>)}
                    </>
                  )}

                  {cerrados.length>0 && (
                    <>
                      <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".1em",margin:"14px 0 6px"}}>CERRADOS ({cerrados.length})</div>
                      {cerrados.map(t=><TrackCard key={t.id} t={t}/>)}
                    </>
                  )}

                  {tracker.length===0 && (
                    <div style={{textAlign:"center",padding:"40px",color:"#4a7a9b"}}>
                      <div style={{fontSize:"32px",marginBottom:"8px"}}>📌</div>
                      <div style={{fontSize:"11px",marginBottom:"4px"}}>Todavía no marcaste ninguna acción</div>
                      <div style={{fontSize:"9px"}}>Andá a Oportunidades y usá "📌 Marcar y seguir" en cualquier card</div>
                    </div>
                  )}
                </div>
              );
            })()}

            {paneTab==="watch"&&(
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
            {/* ══ TAB: REPLAY ══ */}
            {paneTab==="replay"&&(()=>{
              const r0 = rows.find(x=>x.ticker===rpTicker);
              const q  = rpBarras ? calidadSerie(rpBarras) : null;
              const f  = calidadDe(rpTicker);
              const maxAbs = Math.max(...rpVisibles.map(d=>Math.abs(d.ret)), 1);
              const pc = v => v==null ? "—" : (v>=0?"+":"")+v.toFixed(1)+"%";
              const cc = v => v==null ? "#5a8fa8" : v>0 ? "#00ff88" : v<0 ? "#ff3355" : "#8fb4cc";
              return (
                <div>
                  {/* buscador */}
                  <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"7px"}}>⏪ REPLAY — ¿QUÉ DECÍA EL SISTEMA ESE DÍA?</div>
                    <div style={{display:"flex",gap:"6px",flexWrap:"wrap"}}>
                      <input value={rpInput} onChange={e=>setRpInput(e.target.value.toUpperCase())}
                        onKeyDown={e=>{if(e.key==="Enter"){setRpTicker(rpInput.trim());setRpSel(null);setRpCalc(null);}}}
                        placeholder="Ticker: AAPL, GGAL, PBR..."
                        style={{flex:1,minWidth:"120px",background:"#050c15",border:"1px solid #1e3a50",borderRadius:"5px",padding:"7px 10px",color:"#e8f4ff",fontSize:"11px",fontFamily:"inherit"}}/>
                      <button className="btn on" onClick={()=>{setRpTicker(rpInput.trim());setRpSel(null);setRpCalc(null);}}>Analizar</button>
                    </div>
                    {!rpTicker&&(
                      <div style={{fontSize:"7px",color:"#5a8fa8",marginTop:"7px",lineHeight:1.7}}>
                        Escribí un ticker y tocá cualquier barra del gráfico. El sistema recalcula la señal
                        <strong> cortando la serie en esa fecha</strong> — sin ver nada posterior — y la compara contra
                        lo que efectivamente pasó después.
                      </div>
                    )}
                  </div>

                  {/* ── RANKING DEL PERÍODO ── */}
                  <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"8px",flexWrap:"wrap",gap:"6px"}}>
                      <span style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>🏆 RANKING · ÚLTIMOS {rpVentRank} DÍAS</span>
                      <div style={{display:"flex",gap:"3px"}}>
                        {[5,15,30,60,90].map(v=>(
                          <button key={v} className={`btn ${rpVentRank===v?"on":"off"}`} style={{padding:"3px 7px",fontSize:"7px"}}
                            onClick={()=>setRpVentRank(v)}>{v}D</button>
                        ))}
                      </div>
                    </div>
                    <div style={{display:"flex",gap:"4px",marginBottom:"6px"}}>
                      {[["suben",`▲ Suben`,"#00ff88"],["caen",`▼ Caen`,"#ff3355"],["estables",`■ Estables`,"#ffd700"]].map(([k,l,c])=>(
                        <button key={k} onClick={()=>setRpRankTab(k)}
                          style={{flex:1,padding:"5px",fontSize:"8px",fontFamily:"inherit",cursor:"pointer",borderRadius:"4px",
                            background:rpRankTab===k?`${c}20`:"#0c1926",color:rpRankTab===k?c:"#5a8fa8",
                            border:`1px solid ${rpRankTab===k?c+"60":"#1e3a50"}`,fontWeight:rpRankTab===k?700:400}}>{l}</button>
                      ))}
                    </div>
                    {rpRankTab!=="estables"&&(
                      <label style={{display:"flex",alignItems:"center",gap:"5px",marginBottom:"8px",cursor:"pointer",fontSize:"7px",color:"#8fb4cc"}}>
                        <input type="checkbox" checked={rpAjustVol} onChange={e=>setRpAjustVol(e.target.checked)} style={{accentColor:"#00d4ff"}}/>
                        Ajustar por volatilidad
                      </label>
                    )}
                    {rpAjustVol&&rpRankTab!=="estables"&&(
                      <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"8px",lineHeight:1.6}}>
                        Retorno menos la mediana de activos de volatilidad similar — evita que domine el ranking el más volátil.
                      </div>
                    )}
                    <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"6px"}}>Barra = variación % en el período elegido, por activo.</div>
                    {(()=>{
                      const conc = rpRankTab==="suben" ? rpRanking.concSuben : rpRankTab==="caen" ? rpRanking.concCaen : null;
                      if (!conc || conc.distintos < 5) return null;
                      const amplio = conc.dominante < 0.35 && conc.distintos >= 12;
                      return (
                        <div style={{...semBox(amplio?"#ffd700":"#5a8fa8","10"),padding:"7px 8px",marginBottom:"8px",fontSize:"7px",lineHeight:1.6}}>
                          {amplio ? (
                            <span style={{color:"#ffd700"}}>
                              ⚠ {conc.distintos} sectores sin uno dominante — patrón de <strong>catalizador de mercado amplio</strong>, no señal sectorial.
                            </span>
                          ) : (
                            <span style={{color:"#8fb4cc"}}>
                              {conc.nTop} de 20 son del sector <strong>{conc.sectorTop}</strong> — posible catalizador del rubro.
                            </span>
                          )}
                        </div>
                      );
                    })()}
                    {(()=>{
                      const base = rpRankTab==="suben" ? (rpAjustVol?rpRanking.subenExc:rpRanking.suben)
                                 : rpRankTab==="caen"  ? (rpAjustVol?rpRanking.caenExc:rpRanking.caen)
                                 : rpRanking.estables;
                      const lista = base||[];
                      if(!lista.length) return <div style={{fontSize:"8px",color:"#5a8fa8",padding:"8px"}}>Sin datos suficientes para este período.</div>;
                      const campo = rpAjustVol&&rpRankTab!=="estables" ? "exceso" : "ret";
                      const maxAbs = Math.max(...lista.map(x=>Math.abs(x[campo]??x.ret)),0.1);
                      return (<>
                        {lista.map((x,i)=>{
                          const v = x[campo] ?? x.ret;
                          return (
                          <div key={x.tk} onClick={()=>{setRpInput(x.tk);setRpTicker(x.tk);setRpSel(null);setRpCalc(null);}}
                            style={{display:"flex",alignItems:"center",gap:"6px",padding:"4px 6px",marginBottom:"2px",cursor:"pointer",
                              borderRadius:"3px",background:rpTicker===x.tk?"#00d4ff14":"#050c15",
                              border:rpTicker===x.tk?"1px solid #00d4ff40":"1px solid transparent"}}>
                            <span style={{fontSize:"7px",color:"#3a5a70",width:"14px",flexShrink:0}}>{i+1}</span>
                            <span style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:"#e8f4ff",width:"46px",flexShrink:0}}>{x.tk}</span>
                            {x.sospechoso&&<span title="Salto de más de 50% en un día: probable split no ajustado, no un movimiento real"
                              style={{fontSize:"8px",color:"#ff9040",flexShrink:0}}>⚠</span>}
                            <span style={{fontSize:"6px",color:x.moneda==="ARS"?"#00d4ff":"#5a9bff",width:"18px",flexShrink:0}}>{x.moneda==="ARS"?"AR":"US"}</span>
                            <span style={{fontSize:"6px",color:"#5a8fa8",width:"56px",flexShrink:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={x.sector}>{x.sector}</span>
                            <div style={{flex:1,height:"5px",background:"#07121c",borderRadius:"2px",position:"relative",overflow:"hidden",minWidth:"24px"}}>
                              <div style={{position:"absolute",left:"50%",top:0,bottom:0,width:"1px",background:"#1e3a50"}}/>
                              <div style={{position:"absolute",top:0,bottom:0,background:v>=0?"#00ff88":"#ff3355",
                                left:v>=0?"50%":`${50-Math.abs(v)/maxAbs*48}%`,
                                width:`${Math.min(48,Math.abs(v)/maxAbs*48)}%`}}/>
                            </div>
                            <span style={{fontSize:"9px",color:v>0?"#00ff88":v<0?"#ff3355":"#8fb4cc",width:"52px",textAlign:"right",flexShrink:0,fontWeight:700}}>
                              {v>=0?"+":""}{v.toFixed(1)}%
                            </span>
                            {campo==="exceso"&&<span style={{fontSize:"6px",color:"#5a8fa8",width:"38px",textAlign:"right",flexShrink:0}}>(crudo {x.ret>=0?"+":""}{x.ret.toFixed(1)}%)</span>}
                            {x.sig&&<span style={{fontSize:"6px",color:SC[x.sig]||"#5a8fa8",width:"30px",textAlign:"right",flexShrink:0}}>
                              {x.sig.includes("COMPRA FUERTE")?"C++":x.sig.includes("COMPRA")?"C":x.sig.includes("VENTA FUERTE")?"V--":x.sig.includes("VENTA")?"V":"—"}
                            </span>}
                          </div>
                        );})}
                        <div style={{fontSize:"6px",color:"#4a7a9b",marginTop:"6px",lineHeight:1.6}}>
                          {rpRanking.total} activos con datos válidos · tocá cualquiera para analizarlo.
                          {rpRankTab==="estables"&&" \"Estables\" = menor variación absoluta, no los del medio de la tabla."}
                          {" Se excluyen series sin operaciones reales (precio congelado o sin volumen), que aparecerían como estables sin serlo."}
                        </div>
                      </>);
                    })()}

                    {/* ── CRUCE AUTOMÁTICO: qué decía la señal antes del movimiento ── */}
                    {(rpRankTab==="suben"||rpRankTab==="caen")&&(
                      <div style={{marginTop:"10px",paddingTop:"9px",borderTop:"1px dashed #1e3a50"}}>
                        <button onClick={rpCorrerCruce} disabled={rpCruceCargando}
                          className="btn on" style={{width:"100%",padding:"7px",fontSize:"8px",opacity:rpCruceCargando?0.6:1}}>
                          {rpCruceCargando ? `⏳ Analizando ${rpCruceProg.hecho}/${rpCruceProg.total}...`
                            : `🔍 ¿Qué decía la señal ANTES de moverse? — analizar los ${Math.min(10,(rpRankTab==="suben"?rpRanking.suben:rpRanking.caen).length)}`}
                        </button>
                        <div style={{fontSize:"6px",color:"#5a8fa8",marginTop:"4px",lineHeight:1.6}}>
                          Corta la serie en cada punto, sin lookahead, y busca cuándo marcó {rpRankTab==="suben"?"COMPRA":"VENTA"}.
                          Puede tardar hasta medio minuto (~10 activos).
                        </div>
                        {rpCruce&&rpCruce.tab===rpRankTab&&(
                          <div style={{marginTop:"8px"}}>
                            {rpCruce.items.map(it=>(
                              <div key={it.tk} style={{padding:"6px 7px",marginBottom:"3px",...semBox(
                                it.diaSenal==null?"#5a8fa8":it.pctConsumido>=60?"#ff3355":it.pctConsumido>=30?"#ffd700":"#00ff88","12")}}>
                                <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline"}}>
                                  <span style={{fontFamily:"'Bebas Neue'",fontSize:"12px",color:"#e8f4ff"}}>{it.tk}</span>
                                  <span style={{fontSize:"8px",color:it.ret>=0?"#00ff88":"#ff3355",fontWeight:700}}>{it.ret>=0?"+":""}{it.ret.toFixed(1)}% total</span>
                                </div>
                                <div style={{fontSize:"7px",color:"#b0d4e8",marginTop:"2px",lineHeight:1.5}}>
                                  {it.diaSenal==null ? (
                                    <span style={{color:"#5a8fa8"}}>Nunca marcó {rpRankTab==="suben"?"COMPRA":"VENTA"} en estos {rpVentRank} días</span>
                                  ) : (
                                    <>Marcó {rpRankTab==="suben"?"COMPRA":"VENTA"} el día <strong>{it.diaSenal}</strong> de {rpVentRank}
                                    {" "}({it.fechaSenal}) — para entonces ya llevaba <strong>{it.pctConsumido.toFixed(0)}%</strong> del
                                    movimiento total consumido.</>
                                  )}
                                </div>
                              </div>
                            ))}
                            {rpCruce.items.length>0&&(()=>{
                              const conSenal = rpCruce.items.filter(x=>x.diaSenal!=null);
                              if(!conSenal.length) return (
                                <div style={{fontSize:"7px",color:"#5a8fa8",marginTop:"6px"}}>
                                  Ninguno de los {rpCruce.items.length} marcó la señal esperada durante el período.
                                </div>
                              );
                              const media = conSenal.reduce((a,x)=>a+x.pctConsumido,0)/conSenal.length;
                              return (
                                <div style={{...semBox(media>=50?"#ff3355":"#ffd700","14"),padding:"8px",marginTop:"6px"}}>
                                  <div style={{fontSize:"8px",color:media>=50?"#ff3355":"#ffd700",fontWeight:700,marginBottom:"5px"}}>
                                    En promedio, {media.toFixed(0)}% del movimiento ya estaba consumido cuando la señal disparó
                                    ({conSenal.length} de {rpCruce.items.length} llegaron a marcarla).
                                  </div>
                                  <div style={{display:"flex",alignItems:"center",gap:"6px"}}>
                                    <span style={{fontSize:"6px",color:"#8fb4cc",flex:1}}>
                                      Esto es {rpCruce.items.length} casos de una muestra — no es una prueba estadística.
                                      Para la versión rigurosa (10 años, corrección por comparaciones múltiples) mirá Validación.
                                    </span>
                                    <button onClick={()=>setTab("quant")} className="btn off" style={{padding:"4px 8px",fontSize:"7px",flexShrink:0}}>🔬 Validación</button>
                                  </div>
                                </div>
                              );
                            })()}
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  {rpTicker && !rpBarras && (
                    <div className="card" style={{padding:"14px",textAlign:"center",color:"#ff9040",fontSize:"9px"}}>
                      No hay datos suficientes para <strong>{rpTicker}</strong> (se necesitan 80 barras).
                      Probá con un ticker del universo.
                    </div>
                  )}

                  {rpBarras && (<>
                    {/* mini dashboard */}
                    <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                      <div style={{display:"flex",alignItems:"baseline",gap:"8px",marginBottom:"8px",flexWrap:"wrap"}}>
                        <span style={{fontFamily:"'Bebas Neue'",fontSize:"26px",color:"#e8f4ff"}}>{rpTicker}</span>
                        <span style={{fontSize:"9px",color:"#5a8fa8"}}>{r0?.name||""}</span>
                        <span style={{marginLeft:"auto",fontFamily:"'Bebas Neue'",fontSize:"22px",color:"#00d4ff"}}>
                          {rpDias.length?rpDias[rpDias.length-1].close.toFixed(2):"—"}
                        </span>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(72px,1fr))",gap:"5px",marginBottom:"8px"}}>
                        {[["Señal hoy",r0?.sig?.sig||"—",SC[r0?.sig?.sig]||"#5a8fa8"],
                          ["RSI",r0?.sig?.rsi??"—",r0?.sig?.rsi>70?"#ff3355":r0?.sig?.rsi<30?"#00ff88":"#8fb4cc"],
                          ["Calidad fund.",f?.calidad??"—",f?.fragil?"#ff3355":f?.calidad>=70?"#00ff88":"#ffd700"],
                          ["Serie",q?.nivel||"—",q?.nivel==="ok"?"#00ff88":q?.nivel==="dudosa"?"#ffd700":"#ff3355"],
                        ].map(([l,v,c])=>(
                          <div key={l} style={{padding:"6px",textAlign:"center",...semBox(c,"12")}}>
                            <div style={{fontSize:"6px",color:"#8fb4cc",marginBottom:"2px"}}>{l}</div>
                            <div style={{fontSize:"10px",color:c,fontWeight:700}}>{v}</div>
                          </div>
                        ))}
                      </div>
                      {(f?.fragil||q?.nivel!=="ok")&&(
                        <div style={{...semBox("#ff3355","12"),padding:"7px",fontSize:"7px",color:"#ffb3c0",lineHeight:1.6}}>
                          {f?.fragil&&<div>⚠ Empresa marcada frágil{f.banderas?.length?": "+f.banderas.join(" · "):""}</div>}
                          {q&&q.nivel!=="ok"&&<div>⚠ Serie de precios {q.nivel}{q.motivo?": "+q.motivo:""} — los indicadores acá miden ruido</div>}
                        </div>
                      )}
                    </div>

                    {/* grafico de barras */}
                    <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"9px"}}>
                        <span style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em"}}>VARIACIÓN DIARIA</span>
                        <div style={{display:"flex",gap:"4px"}}>
                          {[5,15,30,60,90].map(v=>(
                            <button key={v} className={`btn ${rpVent===v?"on":"off"}`} style={{padding:"3px 9px",fontSize:"8px"}}
                              onClick={()=>setRpVent(v)}>{v}D</button>
                          ))}
                        </div>
                      </div>
                      <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"6px"}}>Barra = variación % de cierre a cierre, cada día.</div>
                      <div style={{display:"flex",alignItems:"stretch",gap:"2px",height:"120px",marginBottom:"4px"}}>
                        {rpVisibles.map(d=>{
                          const h = Math.max(3, Math.abs(d.ret)/maxAbs*46);
                          const act = rpSel===d.date;
                          return (
                            <div key={d.date} onClick={()=>rpAnalizar(d)}
                              title={d.date+"  "+pc(d.ret)}
                              style={{flex:1,display:"flex",flexDirection:"column",justifyContent:"center",cursor:"pointer",
                                background:act?"#00d4ff18":"transparent",borderRadius:"3px",padding:"0 1px",
                                border:act?"1px solid #00d4ff60":"1px solid transparent"}}>
                              <div style={{height:"50%",display:"flex",alignItems:"flex-end"}}>
                                {d.ret>=0&&<div style={{width:"100%",height:`${h}%`,background:act?"#00ff88":"#00ff8899",borderRadius:"2px 2px 0 0"}}/>}
                              </div>
                              <div style={{height:"1px",background:"#1e3a50"}}/>
                              <div style={{height:"50%"}}>
                                {d.ret<0&&<div style={{width:"100%",height:`${h}%`,background:act?"#ff3355":"#ff335599",borderRadius:"0 0 2px 2px"}}/>}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                      <div style={{display:"flex",justifyContent:"space-between",fontSize:"6px",color:"#5a8fa8"}}>
                        <span>{rpVisibles[0]?.date}</span>
                        <span>tocá una barra para ver qué decía el sistema ese día</span>
                        <span>{rpVisibles[rpVisibles.length-1]?.date}</span>
                      </div>
                    </div>

                    {/* resultado del replay */}
                    {rpCargando&&(
                      <div className="card" style={{padding:"16px",textAlign:"center",color:"#00d4ff",fontSize:"9px"}}>
                        ⏳ Recalculando la señal al {rpSel}...
                      </div>
                    )}
                    {rpCalc&&!rpCargando&&(rpCalc.error?(
                      <div className="card" style={{padding:"14px",color:"#ff3355",fontSize:"9px"}}>Error: {rpCalc.error}</div>
                    ):(()=>{
                      const sg = rpCalc.sig;
                      const col = SC[sg?.sig]||"#5a8fa8";
                      const compra = (sg?.sig||"").includes("COMPRA");
                      const venta  = (sg?.sig||"").includes("VENTA");
                      // Horizonte más largo con datos disponibles
                      const hz = [[20,rpCalc.fwd20],[10,rpCalc.fwd10],[5,rpCalc.fwd5],[3,rpCalc.fwd3],[1,rpCalc.fwd1]].find(([,v])=>v!=null);
                      const ok = hz ? (compra ? hz[1]>0 : venta ? hz[1]<0 : null) : null;
                      // ¿habría tocado stop o TP antes de cerrar?
                      const tocoTP  = compra && sg?.tp1!=null && rpCalc.maxFwd!=null && (sg.tp1/rpCalc.px-1)*100 <= rpCalc.maxFwd;
                      const tocoSL  = compra && sg?.sl!=null  && rpCalc.minFwd!=null && (sg.sl /rpCalc.px-1)*100 >= rpCalc.minFwd;
                      const cand = (()=>{ try { return detectCandlePattern(rpBarras.slice(0, rpDias.find(d=>d.date===rpCalc.fecha).idx+1)); } catch(_) { return null; } })();
                      const banda = bandaRSI(sg?.rsi);
                      return (
                        <div className="card" style={{padding:"12px",borderLeft:`3px solid ${col}`}}>
                          <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"3px"}}>
                            ⏪ EL SISTEMA, EL {rpCalc.fecha}
                          </div>
                          <div style={{fontSize:"7px",color:"#5a8fa8",marginBottom:"9px"}}>
                            Calculado con {rpCalc.barrasUsadas.toLocaleString()} barras hasta esa fecha — sin ver nada posterior
                          </div>

                          <div style={{display:"flex",alignItems:"center",gap:"9px",marginBottom:"10px",flexWrap:"wrap"}}>
                            <span style={{fontFamily:"'Bebas Neue'",fontSize:"24px",color:col}}>{sg?.sig||"—"}</span>
                            <span style={{fontSize:"9px",color:"#8fb4cc"}}>conf {sg?.conf??"—"}% · precio {rpCalc.px?.toFixed(2)}</span>
                          </div>

                          {/* SCORES DE ESE DIA */}
                          <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"4px",letterSpacing:".08em"}}>SCORES ESE DÍA</div>
                          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(64px,1fr))",gap:"4px",marginBottom:"10px"}}>
                            {[["FX-Técnico",sg?.fx_sc],["EVO",sg?.evo_sc],["Momentum",sg?.mom_sc],["Reversión",sg?.rev_sc],["Final",sg?.final_sc]]
                              .filter(([,v])=>v!=null).map(([l,v])=>(
                              <div key={l} style={{padding:"5px",textAlign:"center",background:"#050c15",borderRadius:"4px"}}>
                                <div style={{fontSize:"6px",color:"#5a8fa8"}}>{l}</div>
                                <div style={{fontSize:"12px",fontFamily:"'Bebas Neue'",color:v>=60?"#00ff88":v>=45?"#ffd700":"#ff9040"}}>{typeof v==="number"?v.toFixed(0):v}</div>
                              </div>
                            ))}
                          </div>

                          {/* INDICADORES DE ESE DIA */}
                          <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"4px",letterSpacing:".08em"}}>INDICADORES ESE DÍA</div>
                          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(78px,1fr))",gap:"4px",marginBottom:"4px"}}>
                            {[["RSI",sg?.rsi,sg?.rsi>70?"#ff3355":sg?.rsi<30?"#00ff88":"#8fb4cc"],
                              ["MACD",sg?.macd,sg?.macd>0?"#00ff88":"#ff3355"],
                              ["ROC 10h",sg?.roc10!=null?sg.roc10+"%":null,sg?.roc10>0?"#00ff88":"#ff3355"],
                              ["Mom 5h",sg?.mom5!=null?sg.mom5+"%":null,sg?.mom5>0?"#00ff88":"#ff3355"],
                              ["ATR",sg?.atr,"#a0cce0"],
                              ["Régimen",sg?.regime,"#ffd700"],
                              ["Vol 24h",sg?.vol_24h!=null?sg.vol_24h+"x":null,"#a0cce0"],
                              ...(()=>{ const vv=volVsMedia(rpTicker||sel?.ticker, sg?.vol_24h, sg);
                                return vv ? [[vv.fuente==="movil6m"?"Vol vs media 6m":"Vol vs media (anual)",`${vv.dif>=0?"+":""}${vv.dif} (${vv.pct>=0?"+":""}${vv.pct}%)`,
                                  vv.dif>0?"#00ff9d":vv.dif<0?"#ff9040":"#ffd700"]] : []; })(),
                              ...(()=>{ const p=persistenciaDireccional(rpTicker||sel?.ticker, rpCalc.fecha);
                                if(!p) return [];
                                return [[p.validado?"Persistencia":"⚗ Persistencia",
                                  `${p.pred>0?"▲":"▼"} ${p.pred>=0?"+":""}${p.pred}σ`,
                                  p.validado?(p.pred>0?"#00ff9d":"#ff3355"):"#5a8fa8"]]; })(),
                              ["Tendencia",sg?.trend,"#8fb4cc"],
                            ].filter(([,v])=>v!=null&&v!=="").map(([l,v,c])=>(
                              <div key={l} style={{display:"flex",justifyContent:"space-between",padding:"4px 6px",background:"#050c15",borderRadius:"3px",fontSize:"8px"}}>
                                <span style={{color:"#5a8fa8"}}>{l}</span><span style={{color:c,fontWeight:700}}>{v}</span>
                              </div>
                            ))}
                          </div>
                          {banda&&(
                            <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"10px",lineHeight:1.6}}>
                              RSI {sg.rsi} → banda {banda.lo}-{banda.hi===101?"100":banda.hi}: históricamente {banda.pUp}% de salto +4% en 1-4d
                              (promedio general {RSI_BASE_PROM_UP}%) y {banda.pDn}% de caída -4%.
                            </div>
                          )}
                          {cand?.patterns?.length>0&&(
                            <div style={{fontSize:"7px",color:"#8fb4cc",marginBottom:"10px"}}>
                              🕯️ Vela ese día: <strong>{cand.patterns[0].name}</strong>
                              {(()=>{const v=VELAS_TASAS_BASE.find(x=>x.clave===cand.patterns[0].name);
                                return v?<span style={{color:"#5a8fa8"}}> — medido: {v.fwd4>=0?"+":""}{v.fwd4}% a 4d (baseline {VELAS_BASELINE.fwd4}%), {v.veredicto}</span>:null;})()}
                            </div>
                          )}

                          {/* NIVELES QUE PROPONIA */}
                          {sg?.entry!=null&&(<>
                            <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"4px",letterSpacing:".08em"}}>NIVELES QUE PROPONÍA</div>
                            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(60px,1fr))",gap:"4px",marginBottom:"10px"}}>
                              {[["Entrada",sg.entry,"#00d4ff"],["Stop",sg.sl,"#ff3355"],["TP1",sg.tp1,"#00ff88"],["TP2",sg.tp2,"#00ff88"],["R/R",sg.rr!=null?sg.rr+"x":null,sg.rr>=2?"#00ff88":"#ffd700"]]
                                .filter(([,v])=>v!=null).map(([l,v,c])=>(
                                <div key={l} style={{padding:"5px",textAlign:"center",background:"#050c15",borderRadius:"4px"}}>
                                  <div style={{fontSize:"6px",color:"#5a8fa8"}}>{l}</div>
                                  <div style={{fontSize:"10px",color:c,fontWeight:700}}>{typeof v==="number"?v.toFixed(2):v}</div>
                                </div>
                              ))}
                            </div>
                          </>)}

                          <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"4px",letterSpacing:".08em"}}>YA SE HABÍA MOVIDO ANTES</div>
                          <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"5px",marginBottom:"10px"}}>
                            {[["5d",rpCalc.prev5],["10d",rpCalc.prev10],["20d",rpCalc.prev20]].map(([l,v])=>(
                              <div key={l} style={{padding:"6px",textAlign:"center",background:"#050c15",borderRadius:"4px"}}>
                                <div style={{fontSize:"6px",color:"#5a8fa8"}}>{l} previos</div>
                                <div style={{fontSize:"12px",fontFamily:"'Bebas Neue'",color:cc(v)}}>{pc(v)}</div>
                              </div>
                            ))}
                          </div>

                          <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",marginBottom:"4px"}}>
                            <span style={{fontSize:"7px",color:"#4a7a9b",letterSpacing:".08em"}}>QUÉ PASÓ DESPUÉS</span>
                            <span style={{fontSize:"6px",color:"#5a8fa8"}}>{rpCalc.diasDisp} días transcurridos desde esa fecha</span>
                          </div>
                          <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:"3px",marginBottom:"6px"}}>
                            {[["+1d",rpCalc.fwd1],["+3d",rpCalc.fwd3],["+5d",rpCalc.fwd5],["+10d",rpCalc.fwd10],["+20d",rpCalc.fwd20]].map(([l,v])=>(
                              <div key={l} style={{padding:"6px 2px",textAlign:"center",...(v!=null?semBox(cc(v),"14"):{background:"#07121c",borderRadius:"4px",border:"1px dashed #1e3a50"})}}>
                                <div style={{fontSize:"6px",color:"#8fb4cc"}}>{l}</div>
                                <div style={{fontSize:"11px",fontFamily:"'Bebas Neue'",color:v!=null?cc(v):"#3a5a70"}}>{v!=null?pc(v):"·"}</div>
                              </div>
                            ))}
                          </div>
                          {(rpCalc.maxFwd!=null)&&(
                            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"5px",marginBottom:"9px"}}>
                              <div style={{padding:"5px",textAlign:"center",background:"#050c15",borderRadius:"4px"}}>
                                <div style={{fontSize:"6px",color:"#5a8fa8"}}>máximo alcanzado (20d)</div>
                                <div style={{fontSize:"11px",color:"#00ff88"}}>{pc(rpCalc.maxFwd)}</div>
                              </div>
                              <div style={{padding:"5px",textAlign:"center",background:"#050c15",borderRadius:"4px"}}>
                                <div style={{fontSize:"6px",color:"#5a8fa8"}}>mínimo alcanzado (20d)</div>
                                <div style={{fontSize:"11px",color:"#ff3355"}}>{pc(rpCalc.minFwd)}</div>
                              </div>
                            </div>
                          )}

                          {compra&&sg?.tp1!=null&&rpCalc.maxFwd!=null&&(
                            <div style={{...semBox(tocoTP&&!tocoSL?"#00ff88":tocoSL?"#ff3355":"#ffd700","12"),padding:"7px",marginBottom:"6px",fontSize:"7px",lineHeight:1.6,color:"#b0d4e8"}}>
                              {tocoSL&&tocoTP && <>Tocó <strong style={{color:"#00ff88"}}>TP1</strong> y el <strong style={{color:"#ff3355"}}>stop</strong> en 20 días — el orden no se puede saber con datos diarios.</>}
                              {tocoSL&&!tocoTP && <>Habría tocado el <strong style={{color:"#ff3355"}}>stop</strong> ({sg.sl.toFixed(2)}) antes de llegar al TP1.</>}
                              {!tocoSL&&tocoTP && <>Habría alcanzado <strong style={{color:"#00ff88"}}>TP1</strong> ({sg.tp1.toFixed(2)}) sin tocar el stop.</>}
                              {!tocoSL&&!tocoTP && <>No llegó ni al TP1 ({sg.tp1.toFixed(2)}) ni al stop ({sg.sl.toFixed(2)}) en 20 días.</>}
                            </div>
                          )}

                          {ok!==null&&hz&&(
                            <div style={{...semBox(ok?"#00ff88":"#ff3355","14"),padding:"8px"}}>
                              <div style={{fontSize:"9px",color:ok?"#00ff88":"#ff3355",fontWeight:700,marginBottom:"3px"}}>
                                {ok?`✓ Acertó la dirección a ${hz[0]} días`:`✕ Erró la dirección a ${hz[0]} días`}
                                {hz[0]<20&&<span style={{color:"#8fb4cc",fontWeight:400}}> (parcial — faltan días para 20d)</span>}
                              </div>
                              <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.6}}>
                                Un caso aislado no dice nada del sistema — tocá varias barras para ver el patrón.
                                {rpCalc.prev20!=null&&Math.abs(rpCalc.prev20)>5&&(
                                  <> Acá ya se había movido <strong>{pc(rpCalc.prev20)}</strong> antes de la señal.</>
                                )}
                              </div>
                            </div>
                          )}
                          {!hz&&(
                            <div style={{fontSize:"7px",color:"#5a8fa8",padding:"6px 0",lineHeight:1.6}}>
                              Fecha demasiado reciente para evaluar. Probá con la ventana <strong>60D</strong> o <strong>90D</strong> y
                              tocá una barra más antigua — ahí sí hay días posteriores para medir.
                            </div>
                          )}
                        </div>
                      );
                    })())}

                  </>)}
                </div>
              );
            })()}

            {paneTab==="cmp"&&(()=>{
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


            {/* ══ TAB: CALENDARIO ══ */}
            {paneTab==="cal"&&(()=>{
              const hoy = new Date(new Date().toLocaleString("en-US",{timeZone:"America/Argentina/Buenos_Aires"}));
              const hoyStr = hoy.toISOString().slice(0,10);
              const diasHasta = f => Math.round((new Date(f) - new Date(hoyStr)) / 86400000);

              // Reuniones de la Fed — futuras, con la ultima pasada como referencia
              const fedEventos = FOMC_2026.filter(f=>f>=hoyStr).map(f=>({
                tipo:"fed", fecha:f, dias:diasHasta(f),
                titulo:"Reunión FOMC", sub:"Decisión de tasas + comunicado 14:00 ET",
              }));

              // Balances — todos los activos con fecha "prox" futura conocida
              const earn = DATA_MOD?.FXCA16_EARNINGS || {};
              const earnEventos = [];
              for (const [tk, info] of Object.entries(earn)) {
                if (!info?.prox || info.prox < hoyStr) continue;
                const meta = TICKERS_TODOS.find(t=>t.ticker===tk);
                earnEventos.push({
                  tipo:"earn", fecha:info.prox, dias:diasHasta(info.prox),
                  titulo:tk, sub:meta?.name||tk, sector:meta?.sector||"—",
                  moneda:meta ? (TICKERS_MERVAL.some(m=>m.ticker===tk)?"ARS":"USD") : "USD",
                });
              }

              const filtroCal = calFiltro;
              const todosEventos = [...fedEventos, ...earnEventos].sort((a,b)=>a.dias-b.dias);
              const visibles = filtroCal==="fed" ? fedEventos
                : filtroCal==="earn" ? earnEventos
                : todosEventos;

              return (
                <div className="fade">
                  <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"3px"}}>📅 CALENDARIO — QUÉ VIENE</div>
                    <div style={{fontSize:"7px",color:"#5a8fa8",lineHeight:1.6,marginBottom:"9px"}}>
                      Fechas conocidas de antemano — el único tipo de catalizador que se puede anticipar de verdad.
                      No dice qué va a pasar, solo cuándo puede haber un movimiento que no tenga que ver con la técnica.
                    </div>
                    <div style={{display:"flex",gap:"4px"}}>
                      {[["todos",`Todos (${todosEventos.length})`],["fed",`🏛️ Fed (${fedEventos.length})`],["earn",`📊 Balances (${earnEventos.length})`]].map(([k,l])=>(
                        <button key={k} onClick={()=>setCalFiltro(k)}
                          style={{flex:1,padding:"6px",fontSize:"8px",fontFamily:"inherit",cursor:"pointer",borderRadius:"4px",
                            background:filtroCal===k?"#1a6eff":"#0c1926",color:filtroCal===k?"#fff":"#5a8fa8",
                            border:`1px solid ${filtroCal===k?"#1a6eff":"#1e3a50"}`,fontWeight:filtroCal===k?700:400}}>{l}</button>
                      ))}
                    </div>
                  </div>

                  {earnEventos.length===0&&(filtroCal==="todos"||filtroCal==="earn")&&(
                    <div style={{...semBox("#ffd700","10"),padding:"9px 10px",marginBottom:"10px"}}>
                      <div style={{fontSize:"7px",color:"#ffd700",lineHeight:1.6}}>
                        ⚠ Sin fechas de balance cargadas todavía. El calendario de earnings del sistema está vacío —
                        no significa que ningún activo reporte pronto, significa que esa fuente de datos no trajo
                        resultados. Verificá manualmente antes de operar cerca de un balance.
                      </div>
                    </div>
                  )}

                  {visibles.length===0 ? (
                    <div style={{textAlign:"center",padding:"20px",color:"#5a8fa8",fontSize:"9px"}}>Sin eventos en este filtro.</div>
                  ) : visibles.map((ev,i)=>{
                    const urgente = ev.dias<=3;
                    const color = ev.tipo==="fed" ? (urgente?"#ff3355":"#ffd700") : (urgente?"#ff9040":"#00d4ff");
                    return (
                      <div key={i}
                        onClick={ev.tipo==="earn" ? ()=>{ const r=rows.find(x=>x.ticker===ev.titulo); setSel(r||{ticker:ev.titulo,moneda:ev.moneda,name:ev.sub,sector:ev.sector}); setTab("det"); } : undefined}
                        style={{display:"flex",alignItems:"center",gap:"9px",padding:"9px 10px",marginBottom:"5px",
                          borderRadius:"5px",cursor:ev.tipo==="earn"?"pointer":"default",...semBox(color,"10")}}>
                        <div style={{width:"38px",textAlign:"center",flexShrink:0}}>
                          <div style={{fontFamily:"'Bebas Neue'",fontSize:"18px",color,lineHeight:1}}>{ev.dias===0?"HOY":ev.dias}</div>
                          {ev.dias!==0&&<div style={{fontSize:"6px",color:"#5a8fa8"}}>días</div>}
                        </div>
                        <span style={{fontSize:"14px"}}>{ev.tipo==="fed"?"🏛️":"📊"}</span>
                        <div style={{flex:1,minWidth:0}}>
                          <div style={{fontSize:ev.tipo==="fed"?"9px":"12px",fontFamily:ev.tipo==="fed"?"inherit":"'Bebas Neue'",color:"#e8f4ff",fontWeight:ev.tipo==="fed"?700:400}}>{ev.titulo}</div>
                          <div style={{fontSize:"7px",color:"#8fb4cc"}}>{ev.sub}{ev.sector&&ev.sector!=="—"?" · "+ev.sector:""}</div>
                        </div>
                        <span style={{fontSize:"7px",color:"#5a8fa8",flexShrink:0}}>{ev.fecha}</span>
                      </div>
                    );
                  })}
                </div>
              );
            })()}

            {/* ══ TAB: QUANT LAB ══ */}
            {paneTab==="quant"&&(
              <div className="fade">
                {/* Controles */}
                <div style={{padding:"10px 12px",background:"#07101a",border:"1px solid #1e3a50",borderRadius:"6px",marginBottom:"10px"}}>
                  <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"8px"}}>
                    🔬 VALIDACIÓN CUANTITATIVA — modelo aprendido + backtest de cartera
                  </div>
                  <div style={{fontSize:"8px",color:"#b0d4e8",lineHeight:1.7,marginBottom:"10px"}}>
                    A diferencia del resto de la app, acá el sistema <strong style={{color:"#00ff88"}}>aprende los pesos desde los datos</strong>
                    (regresión logística, K-fold purgado) y simula una <strong style={{color:"#00ff88"}}>cartera completa</strong> con costos reales.
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
                  <div style={{display:"flex",gap:"8px",alignItems:"center",flexWrap:"wrap"}}>
                    <button className={`btn ${qlRunning?"off":"on"}`} onClick={runQuantLab} disabled={qlRunning||!rows.length}
                      style={{padding:"8px 20px",fontSize:"10px"}}>
                      {qlRunning?"⏳ Procesando...":"▶ ENTRENAR Y VALIDAR"}
                    </button>
                    {qlRunning&&(
                      <button className="btn off" onClick={()=>{qlCancelRef.current=true;}}
                        style={{padding:"8px 16px",fontSize:"10px",color:"#ff3355",borderColor:"#ff335540"}}>
                        ✕ CANCELAR
                      </button>
                    )}
                    {!rows.length&&<span style={{fontSize:"8px",color:"#ff3355"}}>Ejecutá el sistema primero</span>}
                  </div>
                  {qlRunning&&(
                    <div style={{marginTop:"8px",padding:"8px 10px",background:"#ffd70010",border:"1px solid #ffd70030",borderRadius:"4px"}}>
                      <div style={{fontSize:"9px",color:"#ffd700",fontWeight:700,marginBottom:"3px"}}>{qlProgress||"Preparando..."}</div>
                      <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.6}}>
                        Corre en el mismo hilo que la interfaz — va a responder con lentitud. Podés cambiar de pestaña, sigue en segundo plano.
                      </div>
                    </div>
                  )}
                  {!qlRunning&&qlProgress&&<div style={{fontSize:"8px",color:"#ffd700",marginTop:"6px"}}>{qlProgress}</div>}
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
                            <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>CURVA DE CAPITAL (neta de comisiones)</div>
                            <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"4px"}}>Evolución de $1.000 invertidos siguiendo cada señal del backtest, ya con comisiones descontadas.</div>
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
                      Un t-stat alto puede venir de un solo mes excepcional. Cuenta en cuántos meses el filtro superó al mercado —
                      <strong style={{color:"#a0cce0"}}> señal real: &gt;65%, azar: ~50%.</strong>
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
                        <div style={{padding:"9px",...semBox(qlValid.dsr.significativo?"#00ff88":qlValid.dsr.dsr>=0.9?"#ffd700":"#ff3355")}}>
                          <div style={{fontSize:"7px",color:"#8fb4cc",marginBottom:"2px"}}>DEFLATED SHARPE RATIO</div>
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
                        <div style={{padding:"9px",...semBox(qlValid.pbo.color)}}>
                          <div style={{fontSize:"7px",color:"#8fb4cc",marginBottom:"2px"}}>PROBABILIDAD DE SOBREAJUSTE (PBO)</div>
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

                    {qlModels?.length>0&&(()=>{
                      const top = [...qlModels].sort((a,b)=>b.auc-a.auc)[0];
                      return (
                        <div style={{display:"flex",alignItems:"center",gap:"6px",padding:"6px 8px",marginBottom:"10px",...semBox("#5a8fa8","0c")}}>
                          <span style={{fontSize:"7px",color:"#8fb4cc",flex:1}}>
                            Esto es de la <strong>cartera completa</strong>, no de un ticker — para ver cómo se ve
                            un caso individual, mirá el de mejor AUC ({top.ticker}) en Replay.
                          </span>
                          <button
                            onClick={()=>{ setRpInput(top.ticker); setRpTicker(top.ticker); setRpSel(null); setRpCalc(null); setTab("replay"); }}
                            className="btn off" style={{padding:"4px 8px",fontSize:"7px",flexShrink:0}}>⏪ {top.ticker}</button>
                        </div>
                      );
                    })()}

                    {/* Bootstrap */}
                    {qlValid.boot&&(
                      <div style={{padding:"9px",background:"#050c15",borderRadius:"5px",marginBottom:"8px"}}>
                        <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>
                          BOOTSTRAP · {qlValid.boot.nBoot} remuestreos — ¿cuán frágil es el resultado?
                        </div>
                        <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"6px"}}>Rango probable de Sharpe/CAGR/MaxDD si el backtest se repitiera con distinta muestra.</div>
                        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:"6px",marginBottom:"6px"}}>
                          {[
                            {l:"SHARPE",  o:qlValid.boot.sharpe, s:"", c:semaforo(qlValid.boot.sharpe.p50, 0, 1)},
                            {l:"CAGR",    o:qlValid.boot.cagr,   s:"%", c:semaforo(qlValid.boot.cagr.p50, 0, 5)},
                            {l:"MAX DD",  o:qlValid.boot.maxDD,  s:"%", c:semaforo(qlValid.boot.maxDD.p50, -20, -10)},
                          ].map(x=>(
                            <div key={x.l} style={{textAlign:"center",padding:"5px",...semBox(x.c)}}>
                              <div style={{fontSize:"6px",color:"#8fb4cc"}}>{x.l} · IC 90%</div>
                              <div style={{fontFamily:"'Bebas Neue'",fontSize:"14px",color:x.c}}>{x.o.p50}{x.s}</div>
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
                        ⚠️ <strong>Unicidad de muestras: {qlValid.uniq.avgUniqueness}</strong> — las etiquetas se solapan, la muestra
                        <strong style={{color:"#ff9040"}}> efectiva</strong> es ~<strong>{qlValid.uniq.effectiveN}</strong> casos, no el total nominal.
                      </div>
                    )}

                    {/* Regímenes */}
                    {qlValid.regimes?.length>0&&(
                      <div style={{marginTop:"8px"}}>
                        <div style={{fontSize:"7px",color:"#4a7a9b",marginBottom:"2px"}}>
                          RENDIMIENTO POR RÉGIMEN DE MERCADO — ¿gana siempre o solo en bull?
                        </div>
                        <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"5px"}}>Retorno de la señal separado por contexto de mercado (n operaciones, win rate, retorno medio, Sharpe).</div>
                        {qlValid.regimes.map(r=>{
                          const cRow = semaforo(r.avgRet, -0.3, 0.3);
                          return (
                          <div key={r.regime} style={{display:"flex",alignItems:"center",gap:"6px",padding:"4px 7px",marginBottom:"2px",...semBox(cRow,"12"),borderLeft:`3px solid ${cRow}`}}>
                            <span style={{fontSize:"7px",color:"#a0cce0",width:"120px",flexShrink:0}}>{r.regime}</span>
                            <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:"4px",flex:1,fontSize:"7px"}}>
                              {[
                                {l:"n",v:r.n,c:"#a0cce0"},
                                {l:"WR",v:r.winRate+"%",c:semaforo(r.winRate,45,55)},
                                {l:"AVG",v:(r.avgRet>=0?"+":"")+r.avgRet+"%",c:semaforo(r.avgRet,-0.3,0.3)},
                                {l:"SR",v:r.sharpe,c:semaforo(r.sharpe,0,0.3)},
                              ].map(x=>(
                                <div key={x.l} style={{textAlign:"center"}}>
                                  <span style={{color:"#4a7a9b",fontSize:"6px"}}>{x.l} </span>
                                  <span style={{color:x.c,fontWeight:600}}>{x.v}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                          );
                        })}
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
                      Un modelo secundario filtra las señales del primario: <em>"¿vale la pena tomar esta en particular?"</em>
                      Con {COSTO_CEDEAR}% de costo por operación, filtrar vale tanto como acertar.
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
                        <div key={m.ticker}
                          onClick={()=>{
                            const moneda = rows.find(x=>x.ticker===m.ticker)?.moneda || "USD";
                            setSel(rows.find(x=>x.ticker===m.ticker)||{ticker:m.ticker,moneda,name:m.ticker,sector:""});
                            setTab("det");
                          }}
                          style={{display:"flex",alignItems:"center",gap:"6px",padding:"4px 7px",marginBottom:"2px",cursor:"pointer",
                          ...semBox(m.mejora>0?"#00ff88":"#ff3355","12"),borderLeft:`3px solid ${m.mejora>0?"#00ff88":"#ff3355"}`}}>
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
                          <button
                            onClick={e=>{ e.stopPropagation(); setRpInput(m.ticker); setRpTicker(m.ticker); setRpSel(null); setRpCalc(null); setTab("replay"); }}
                            title="Ver casos concretos en Replay"
                            style={{flexShrink:0,background:"transparent",border:"1px solid #1e3a50",borderRadius:"3px",padding:"3px 5px",fontSize:"9px",color:"#00d4ff",cursor:"pointer"}}>⏪</button>
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
                    <div style={{fontSize:"6px",color:"#5a8fa8",marginBottom:"8px"}}>
                      Cuánto empeora el modelo (AUC) al sacar cada indicador — delta alto = aporta información real.
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
                      <strong>AUC</strong> 0.50=azar, &gt;0.55=real, &gt;0.60=fuerte · <strong>Brier Skill</strong> &gt;0 supera predecir la tasa base.
                      Todo fuera de muestra (K-fold purgado).
                    </div>
                    <div style={{maxHeight:"340px",overflowY:"auto",WebkitOverflowScrolling:"touch"}}>
                      {qlModels.map(m=>{
                        const ok = m.auc>=0.55, mid = m.auc>=0.52;
                        const c = ok?"#00ff88":mid?"#ffd700":"#ff3355";
                        return (
                          <div key={m.ticker}
                            onClick={()=>{
                              const moneda = rows.find(x=>x.ticker===m.ticker)?.moneda || "USD";
                              setSel(rows.find(x=>x.ticker===m.ticker)||{ticker:m.ticker,moneda,name:m.ticker,sector:""});
                              setTab("det");
                            }}
                            style={{display:"flex",alignItems:"center",gap:"6px",padding:"5px 7px",marginBottom:"3px",cursor:"pointer",...semBox(c,"12"),borderLeft:`3px solid ${c}`}}>
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
                            <button
                              onClick={e=>{ e.stopPropagation(); setRpInput(m.ticker); setRpTicker(m.ticker); setRpSel(null); setRpCalc(null); setTab("replay"); }}
                              title="Ver casos concretos en Replay"
                              style={{flexShrink:0,background:"transparent",border:"1px solid #1e3a50",borderRadius:"3px",padding:"3px 5px",fontSize:"9px",color:"#00d4ff",cursor:"pointer"}}>⏪</button>
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

            {paneTab==="reglas"&&(
              <div className="fade">
                <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                  <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"4px"}}>
                    📖 REGLAS E HIPÓTESIS — memoria institucional
                  </div>
                  <div style={{fontSize:"7px",color:"#5a8fa8",lineHeight:1.6}}>
                    Todo lo que se investigó fuera del pipeline normal, con método y resultado explícito.
                    Objetivo: no volver a "descubrir" y operar la misma idea unos meses después pensando
                    que es nueva, y saber qué está realmente aplicado en el sistema hoy.
                  </div>
                </div>

                {/* ── REGLAS ACTIVAS ── */}
                {REGLAS_ACTIVAS.length>0&&(
                  <div className="card" style={{padding:"12px",marginBottom:"10px"}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"4px"}}>
                      ✅ REGLAS QUE SOBREVIVIERON VALIDACIÓN ({REGLAS_ACTIVAS.length})
                    </div>
                    <div style={{fontSize:"7px",color:"#5a8fa8",marginBottom:"10px",lineHeight:1.6}}>
                      Hipótesis confirmadas fuera de muestra. "Aplicada" = ya está en el código en producción.
                      "En observación" = confirmada pero pendiente de más validación antes de usarse como señal.
                    </div>
                    {REGLAS_ACTIVAS.map((r,i)=>(
                      <div key={i} style={{...semBox("#00ff9d","10"),padding:"9px",marginBottom:i<REGLAS_ACTIVAS.length-1?"6px":0}}>
                        <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",marginBottom:"4px",gap:"8px"}}>
                          <span style={{fontSize:"9px",color:"#00ff9d",fontWeight:700}}>{r.regla}</span>
                          <span style={{fontSize:"6px",color:r.estado==="aplicada"?"#00ff9d":"#ffd700",flexShrink:0,
                            border:`1px solid ${r.estado==="aplicada"?"#00ff9d":"#ffd700"}`,borderRadius:"3px",padding:"1px 5px",whiteSpace:"nowrap"}}>
                            {r.estado==="aplicada"?"✓ APLICADA":"⏳ EN OBSERVACIÓN"}
                          </span>
                        </div>
                        <div style={{fontSize:"6px",color:"#5a8fa8",flexShrink:0,marginBottom:"4px"}}>{r.fecha}</div>
                        <div style={{fontSize:"7px",color:"#8fb4cc",lineHeight:1.6,marginBottom:"3px"}}>
                          {r.descripcion}
                        </div>
                        <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.6,marginBottom:"3px"}}>
                          <strong style={{color:"#5a8fa8"}}>Evidencia:</strong> {r.evidencia}
                        </div>
                        <div style={{fontSize:"7px",color:"#5a8fa8",lineHeight:1.6,fontStyle:"italic"}}>
                          <strong style={{color:"#5a8fa8",fontStyle:"normal"}}>Uso:</strong> {r.uso}
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                                {/* ── HIPÓTESIS DESCARTADAS ── */}
                {HALLAZGOS_DESCARTADOS.length>0&&(
                  <div className="card" style={{padding:"12px"}}>
                    <div style={{fontSize:"8px",color:"#4a7a9b",letterSpacing:".12em",marginBottom:"4px"}}>
                      🗄️ HIPÓTESIS PROBADAS Y DESCARTADAS ({HALLAZGOS_DESCARTADOS.length})
                    </div>
                    <div style={{fontSize:"7px",color:"#5a8fa8",marginBottom:"10px",lineHeight:1.6}}>
                      Patrones que parecían prometedores con pocos casos, testeados contra el universo completo,
                      y descartados por falta de significancia estadística. Se guardan acá para no re-testear
                      la misma idea más adelante pensando que es nueva.
                    </div>
                    {HALLAZGOS_DESCARTADOS.map((h,i)=>(
                      <div key={i} style={{...semBox("#ff9040","10"),padding:"9px",marginBottom:i<HALLAZGOS_DESCARTADOS.length-1?"6px":0}}>
                        <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",marginBottom:"4px",gap:"8px"}}>
                          <span style={{fontSize:"9px",color:"#ff9040",fontWeight:700}}>{h.hipotesis}</span>
                          <span style={{fontSize:"6px",color:"#5a8fa8",flexShrink:0,marginLeft:"8px",whiteSpace:"nowrap"}}>{h.fecha}</span>
                        </div>
                        {h.veredicto&&<div style={{fontSize:"6px",color:"#4a7a9b",marginBottom:"4px"}}>{h.veredicto}</div>}
                        <div style={{fontSize:"7px",color:"#8fb4cc",lineHeight:1.6,marginBottom:"3px"}}>
                          <strong style={{color:"#5a8fa8"}}>Método:</strong> {h.metodo} · n={h.n}
                        </div>
                        <div style={{fontSize:"7px",color:"#b0d4e8",lineHeight:1.6,marginBottom:"3px"}}>
                          <strong style={{color:"#5a8fa8"}}>Resultado:</strong> {h.resultado}
                        </div>
                        <div style={{fontSize:"7px",color:"#5a8fa8",lineHeight:1.6,fontStyle:"italic"}}>
                          {h.nota}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
                </>
              );
              if (!splitMode) return renderTabContent(tab);
              const cols = splitMode === 4 ? 2 : 2;
              const filas = splitMode === 4 ? 2 : 1;
              return (
                <div className="split-grid" style={{gridTemplateColumns:`repeat(${cols},1fr)`,gridTemplateRows:`repeat(${filas},1fr)`}}>
                  {paneTabs.slice(0, splitMode).map((pt, i) => (
                    <div key={i} style={{border:"1px solid #1e3a50",borderRadius:"6px",padding:esMobile?"10px":"8px",background:"#050c1560",minWidth:0,
                      maxHeight:esMobile?"52vh":"78vh",overflowY:"auto",WebkitOverflowScrolling:"touch"}}>
                      <div style={{display:"flex",gap:"3px",flexWrap:"wrap",marginBottom:"7px",
                        position:"sticky",top:0,background:"#050c15",paddingBottom:"5px",zIndex:1}}>
                        {PANE_TABS_DISPONIBLES.map(([k,l])=>(
                          <button key={k}
                            onClick={()=>setPaneTabs(p=>{const n=[...p]; n[i]=k; return n;})}
                            style={{padding:esMobile?"5px 8px":"3px 6px",fontSize:esMobile?"9px":"7px",fontFamily:"inherit",cursor:"pointer",borderRadius:"3px",
                              background:pt===k?"#1a6eff":"#0c1926",color:pt===k?"#fff":"#5a8fa8",border:`1px solid ${pt===k?"#1a6eff":"#1e3a50"}`}}>
                            {l}
                          </button>
                        ))}
                      </div>
                      {renderTabContent(pt)}
                    </div>
                  ))}
                </div>
              );
            })()}


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