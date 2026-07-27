// ══════════════════════════════════════════════════════════════════════
//  FXCA16 — CAPA AVANZADA DE VALIDACIÓN
//
//  El problema que resuelve: un backtest con buen Sharpe puede ser
//  puro azar si probaste muchas configuraciones. Estas herramientas
//  distinguen una estrategia real de un ajuste al ruido.
//
//   11. Meta-labeling — modelo secundario que decide SI operar
//   12. Unicidad de muestras — corrige etiquetas solapadas
//   13. Deflated Sharpe Ratio — Sharpe corregido por múltiples pruebas
//   14. PBO (CSCV) — probabilidad de que el backtest esté sobreajustado
//   15. Bootstrap de la curva de equity — IC de las métricas
//   16. Segmentación por régimen — ¿cuándo funciona y cuándo no?
//   17. Selección de cartera consciente de correlación
// ══════════════════════════════════════════════════════════════════════

import {
  mean, std, trainLogistic, predictProba, aucRoc,
  extractFeatures, tripleBarrier, purgedKFold, FEATURE_NAMES,
  fitPlatt, applyPlatt, portfolioMetrics,
} from './quant.js';

const clip = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

// ══════════════════════════════════════════════════════════════
// 11. META-LABELING  (López de Prado, cap. 3)
//
//  Modelo primario  → ¿en qué dirección?  (ya lo tenemos)
//  Modelo secundario → ¿vale la pena operar ESTA señal?
//
//  Por qué importa: el primario suele tener buen recall pero mala
//  precisión (dispara mucho). El secundario filtra los falsos
//  positivos SIN cambiar la dirección. Sube la precisión y baja
//  el número de operaciones, que es exactamente lo que querés
//  cuando cada operación cuesta 1.8%.
// ══════════════════════════════════════════════════════════════
export const META_FEATURES = [
  'primProb',     // confianza del modelo primario
  'volRegime',    // volatilidad relativa actual
  'trendStr',     // fuerza de tendencia
  'distSMA200',   // extensión respecto a la media larga
  'volumeConf',   // confirmación por volumen
  'recentWR',     // win-rate reciente del primario en este activo
  'atrRatio',     // ATR actual vs ATR medio
  'dayRange',     // rango del día vs promedio
];

function buildMetaFeatures(daily, i, primProb, recentWR) {
  const w = daily.slice(Math.max(0, i - 200), i + 1);
  const n = w.length - 1;
  if (n < 60) return null;
  const px = w[n].close;

  const sma = p => mean(w.slice(-p).map(d => d.close));
  const a20 = sma(20), a50 = sma(50), a200 = sma(Math.min(200, w.length));

  const rets = [];
  for (let k = 1; k <= 20 && k <= n; k++) {
    rets.push((w[n - k + 1].close - w[n - k].close) / w[n - k].close);
  }
  const volNow = std(rets);
  const retsLong = [];
  for (let k = 1; k <= 60 && k <= n; k++) {
    retsLong.push((w[n - k + 1].close - w[n - k].close) / w[n - k].close);
  }
  const volBase = std(retsLong) || 1e-6;

  let tr = 0;
  for (let k = n - 13; k <= n; k++) {
    if (k < 1) continue;
    tr += Math.max(w[k].high - w[k].low, Math.abs(w[k].high - w[k - 1].close), Math.abs(w[k].low - w[k - 1].close));
  }
  const atr = tr / 14;
  let trLong = 0, cnt = 0;
  for (let k = Math.max(1, n - 59); k <= n; k++) {
    trLong += Math.max(w[k].high - w[k].low, Math.abs(w[k].high - w[k - 1].close), Math.abs(w[k].low - w[k - 1].close));
    cnt++;
  }
  const atrBase = cnt ? trLong / cnt : atr;

  const vols = w.map(d => d.volume || 0);
  const vNow = mean(vols.slice(-5)), vBase = mean(vols.slice(-30)) || 1;

  return {
    primProb: (primProb - 0.5) * 4,
    volRegime: clip((volNow / volBase - 1) * 2, -3, 3),
    trendStr: clip((a20 / a50 - 1) * 40, -3, 3),
    distSMA200: clip((px / a200 - 1) * 15, -3, 3),
    volumeConf: clip((vNow / vBase - 1) * 2, -3, 3),
    recentWR: (recentWR - 0.5) * 4,
    atrRatio: clip((atr / (atrBase || atr) - 1) * 3, -3, 3),
    dayRange: clip(((w[n].high - w[n].low) / px * 100 - 2) / 2, -3, 3),
  };
}

/**
 * Entrena la capa secundaria.
 * Solo se entrena sobre los casos donde el primario dijo "operar",
 * y la etiqueta es: ¿acertó el primario? (1) o ¿falló? (0)
 */
export function trainMetaModel(daily, primaryModel, primaryCal, {
  threshold = 0.5, maxH = 20, cost = 0.018, pt = 2.0, sl = 1.5,
} = {}) {
  if (!daily || daily.length < 200) return null;

  const rows = [];
  const recentOutcomes = [];

  for (let i = 60; i < daily.length - maxH; i++) {
    const f = extractFeatures(daily, i);
    if (!f) continue;
    const x = FEATURE_NAMES.map(k => f[k]);
    const raw = predictProba(primaryModel, x);
    const p = primaryCal ? applyPlatt(primaryCal, raw) : raw;

    // El primario solo "dispara" por encima del umbral
    if (p < threshold) continue;

    const lab = tripleBarrier(daily, i, f._atr, { pt, sl, maxH, cost });
    if (!lab) continue;

    const recentWR = recentOutcomes.length >= 5
      ? mean(recentOutcomes.slice(-15)) : 0.5;

    const mf = buildMetaFeatures(daily, i, p, recentWR);
    if (!mf) continue;

    rows.push({
      x: META_FEATURES.map(k => mf[k]),
      y: lab.label,          // ¿el primario acertó?
      ret: lab.ret,
      idx: i,
      primP: p,
    });
    recentOutcomes.push(lab.label);
  }

  if (rows.length < 50) return null;

  const X = rows.map(r => r.x), y = rows.map(r => r.y);

  // Validación purgada del secundario
  const folds = purgedKFold(X.length, 5, maxH, 0.01);
  const oos = new Array(X.length).fill(null);
  for (const fd of folds) {
    const m = trainLogistic(fd.train.map(i => X[i]), fd.train.map(i => y[i]), { epochs: 250 });
    fd.test.forEach(i => { oos[i] = predictProba(m, X[i]); });
  }
  const vIdx = oos.map((p, i) => p !== null ? i : -1).filter(i => i >= 0);
  if (vIdx.length < 30) return null;

  const pOos = vIdx.map(i => oos[i]);
  const yOos = vIdx.map(i => y[i]);
  const cal = fitPlatt(pOos, yOos);
  const pCal = pOos.map(p => applyPlatt(cal, p));

  const model = trainLogistic(X, y, { epochs: 400 });

  // Comparar: primario solo vs primario + meta
  const primOnlyRet = mean(rows.map(r => r.ret));
  const primOnlyWR = mean(rows.map(r => r.y));

  // Con filtro meta al 55%
  const kept = vIdx.filter((_, k) => pCal[k] >= 0.55);
  const metaRet = kept.length ? mean(kept.map(i => rows[i].ret)) : 0;
  const metaWR = kept.length ? mean(kept.map(i => rows[i].y)) : 0;

  return {
    model, cal,
    n: X.length,
    auc: aucRoc(pOos, yOos),
    sinMeta: {
      trades: rows.length,
      winRate: +(primOnlyWR * 100).toFixed(1),
      avgRet: +(primOnlyRet * 100).toFixed(3),
    },
    conMeta: {
      trades: kept.length,
      winRate: +(metaWR * 100).toFixed(1),
      avgRet: +(metaRet * 100).toFixed(3),
      filtrado: +((1 - kept.length / vIdx.length) * 100).toFixed(1),
    },
    mejora: +((metaWR - primOnlyWR) * 100).toFixed(1),
  };
}

export function metaProbability(daily, i, primProb, metaModel, metaCal, recentWR = 0.5) {
  const mf = buildMetaFeatures(daily, i, primProb, recentWR);
  if (!mf || !metaModel) return null;
  const raw = predictProba(metaModel, META_FEATURES.map(k => mf[k]));
  return metaCal ? applyPlatt(metaCal, raw) : raw;
}

// ══════════════════════════════════════════════════════════════
// 12. UNICIDAD DE MUESTRAS
//
//  Etiquetas que se solapan en el tiempo NO son independientes.
//  Si tenés 500 etiquetas con horizonte 20, en realidad tenés
//  ~25 observaciones independientes. Ignorar esto infla toda
//  medida de significancia.
// ══════════════════════════════════════════════════════════════
export function sampleUniqueness(indices, horizon, totalBars) {
  const conc = new Array(totalBars).fill(0);
  indices.forEach(i => {
    for (let t = i; t < Math.min(totalBars, i + horizon); t++) conc[t]++;
  });
  const u = indices.map(i => {
    let s = 0, c = 0;
    for (let t = i; t < Math.min(totalBars, i + horizon); t++) {
      if (conc[t] > 0) { s += 1 / conc[t]; c++; }
    }
    return c ? s / c : 1;
  });
  return {
    weights: u,
    avgUniqueness: +mean(u).toFixed(3),
    // tamaño de muestra efectivo: lo que realmente tenés
    effectiveN: Math.round(u.reduce((a, b) => a + b, 0)),
  };
}

// ══════════════════════════════════════════════════════════════
// 13. DEFLATED SHARPE RATIO  (Bailey & López de Prado, 2014)
//
//  Si probás 100 configuraciones, la mejor tendrá buen Sharpe
//  aunque todas sean ruido. El DSR responde:
//  "¿cuál es la probabilidad de que este Sharpe sea real,
//   dado que probé N veces?"
//
//  DSR > 0.95 → evidencia sólida
//  DSR < 0.90 → probablemente sobreajuste
// ══════════════════════════════════════════════════════════════
function normCdf(z) {
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = 0.3989423 * Math.exp(-z * z / 2);
  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  return z > 0 ? 1 - p : p;
}
// versión estable por búsqueda binaria (evita constantes frágiles)
function ppf(p) {
  p = clip(p, 1e-10, 1 - 1e-10);
  let lo = -8, hi = 8;
  for (let k = 0; k < 80; k++) {
    const mid = (lo + hi) / 2;
    if (normCdf(mid) < p) lo = mid; else hi = mid;
  }
  return (lo + hi) / 2;
}

export function deflatedSharpe(returns, nTrials, { periodsPerYear = 252 } = {}) {
  const n = returns.length;
  if (n < 20) return null;
  const mu = mean(returns), sd = std(returns);
  if (sd <= 0) return null;

  const sr = mu / sd;                       // Sharpe por período
  const srAnnual = sr * Math.sqrt(periodsPerYear);

  // Momentos superiores (afectan la incertidumbre del Sharpe)
  const m3 = mean(returns.map(r => ((r - mu) / sd) ** 3));
  const m4 = mean(returns.map(r => ((r - mu) / sd) ** 4));

  // Sharpe esperado del MEJOR entre nTrials estrategias sin skill
  const gamma = 0.5772156649;               // Euler–Mascheroni
  const N = Math.max(2, nTrials);
  const e = Math.E;
  const srMax = Math.sqrt(1 / (n - 1)) * (
    (1 - gamma) * ppf(1 - 1 / N) + gamma * ppf(1 - 1 / (N * e))
  );

  // Estadístico deflactado
  const den = Math.sqrt(1 - m3 * sr + ((m4 - 1) / 4) * sr * sr);
  if (!isFinite(den) || den <= 0) return null;
  const z = ((sr - srMax) * Math.sqrt(n - 1)) / den;
  const dsr = normCdf(z);

  // Sharpe mínimo requerido para significancia
  const minTRL = 1 + (1 - m3 * sr + ((m4 - 1) / 4) * sr * sr) * ((ppf(0.95) / (sr - srMax)) ** 2);

  return {
    sharpe: +srAnnual.toFixed(3),
    sharpePeriod: +sr.toFixed(4),
    sharpeUmbral: +(srMax * Math.sqrt(periodsPerYear)).toFixed(3),
    dsr: +dsr.toFixed(4),
    skew: +m3.toFixed(3),
    kurtosis: +m4.toFixed(3),
    nTrials: N,
    minTrackRecord: isFinite(minTRL) ? Math.round(minTRL) : null,
    significativo: dsr >= 0.95,
    veredicto: dsr >= 0.95 ? 'ROBUSTO'
      : dsr >= 0.90 ? 'DUDOSO'
        : 'PROBABLE SOBREAJUSTE',
  };
}

// ══════════════════════════════════════════════════════════════
// 14. PBO — Probability of Backtest Overfitting  (CSCV)
//
//  Parte la serie en S bloques. Prueba todas las combinaciones
//  de mitad-entrenamiento / mitad-validación. Si la config que
//  fue mejor en train queda sistemáticamente por debajo de la
//  mediana en test, el backtest está sobreajustado.
//
//  PBO < 0.20 → confiable
//  PBO > 0.50 → el backtest no vale nada
// ══════════════════════════════════════════════════════════════
function combinations(arr, k) {
  const out = [];
  const rec = (start, cur) => {
    if (cur.length === k) { out.push([...cur]); return; }
    for (let i = start; i < arr.length; i++) { cur.push(arr[i]); rec(i + 1, cur); cur.pop(); }
  };
  rec(0, []);
  return out;
}

export function computePBO(matrix, S = 8) {
  // matrix: [nPeriods][nConfigs] de retornos
  const T = matrix.length, C = matrix[0]?.length || 0;
  if (T < S * 4 || C < 2) return null;

  const blockSize = Math.floor(T / S);
  const blocks = [];
  for (let s = 0; s < S; s++) {
    blocks.push(matrix.slice(s * blockSize, s === S - 1 ? T : (s + 1) * blockSize));
  }

  const combos = combinations([...Array(S).keys()], S / 2);
  const logits = [];

  for (const cIS of combos) {
    const isSet = new Set(cIS);
    const IS = [], OOS = [];
    blocks.forEach((b, i) => (isSet.has(i) ? IS : OOS).push(...b));
    if (!IS.length || !OOS.length) continue;

    const srIS = [], srOOS = [];
    for (let c = 0; c < C; c++) {
      const ri = IS.map(r => r[c]), ro = OOS.map(r => r[c]);
      const si = std(ri), so = std(ro);
      srIS.push(si > 0 ? mean(ri) / si : 0);
      srOOS.push(so > 0 ? mean(ro) / so : 0);
    }
    const best = srIS.indexOf(Math.max(...srIS));
    // rango relativo del ganador en OOS
    const sorted = [...srOOS].sort((a, b) => a - b);
    const rank = sorted.indexOf(srOOS[best]) + 1;
    const w = rank / (C + 1);
    logits.push(Math.log(clip(w, 1e-6, 1 - 1e-6) / (1 - clip(w, 1e-6, 1 - 1e-6))));
  }

  if (!logits.length) return null;
  const pbo = logits.filter(l => l <= 0).length / logits.length;

  return {
    pbo: +pbo.toFixed(3),
    nCombos: logits.length,
    nConfigs: C,
    veredicto: pbo < 0.20 ? 'BACKTEST CONFIABLE'
      : pbo < 0.50 ? 'CONFIANZA MODERADA'
        : 'BACKTEST SOBREAJUSTADO',
    color: pbo < 0.20 ? '#00ff88' : pbo < 0.50 ? '#ffd700' : '#ff3355',
  };
}

// ══════════════════════════════════════════════════════════════
// 15. BOOTSTRAP de la curva de equity
//    Remuestrea los retornos para obtener IC de las métricas.
//    Responde: "¿cuán frágil es este Sharpe?"
// ══════════════════════════════════════════════════════════════
export function bootstrapMetrics(returns, { nBoot = 400, periodsPerYear = 252, seed = 12345 } = {}) {
  const n = returns.length;
  if (n < 30) return null;
  let s = seed >>> 0;
  const rnd = () => { s = (Math.imul(s, 1664525) + 1013904223) >>> 0; return s / 0xffffffff; };

  const sharpes = [], cagrs = [], dds = [];
  for (let b = 0; b < nBoot; b++) {
    const samp = new Array(n);
    for (let i = 0; i < n; i++) samp[i] = returns[(rnd() * n) | 0];
    const mu = mean(samp), sd = std(samp);
    sharpes.push(sd > 0 ? (mu / sd) * Math.sqrt(periodsPerYear) : 0);

    let eq = 1, peak = 1, mdd = 0;
    for (const r of samp) {
      eq *= (1 + r);
      peak = Math.max(peak, eq);
      mdd = Math.min(mdd, eq / peak - 1);
    }
    cagrs.push((Math.pow(eq, periodsPerYear / n) - 1) * 100);
    dds.push(mdd * 100);
  }
  const q = (arr, p) => { const a = [...arr].sort((x, y) => x - y); return a[Math.floor(p * (a.length - 1))]; };

  return {
    sharpe: { p05: +q(sharpes, 0.05).toFixed(2), p50: +q(sharpes, 0.5).toFixed(2), p95: +q(sharpes, 0.95).toFixed(2) },
    cagr: { p05: +q(cagrs, 0.05).toFixed(2), p50: +q(cagrs, 0.5).toFixed(2), p95: +q(cagrs, 0.95).toFixed(2) },
    maxDD: { p05: +q(dds, 0.05).toFixed(2), p50: +q(dds, 0.5).toFixed(2), p95: +q(dds, 0.95).toFixed(2) },
    probSharpePositivo: +(sharpes.filter(x => x > 0).length / nBoot).toFixed(3),
    probSharpe1: +(sharpes.filter(x => x > 1).length / nBoot).toFixed(3),
    nBoot,
  };
}

// ══════════════════════════════════════════════════════════════
// 16. SEGMENTACIÓN POR RÉGIMEN
//    ¿La estrategia gana siempre, o solo en mercados alcistas?
//    Una estrategia que solo funciona en bull es beta disfrazada.
// ══════════════════════════════════════════════════════════════
export function regimeAnalysis(trades, benchmarkDaily) {
  if (!trades?.length || !benchmarkDaily?.length) return null;

  const byDate = {};
  benchmarkDaily.forEach((d, i) => { byDate[d.date] = i; });

  const regimeOf = (date) => {
    const i = byDate[date];
    if (i === undefined || i < 60) return null;
    const w = benchmarkDaily.slice(Math.max(0, i - 60), i + 1);
    const px = w[w.length - 1].close;
    const sma50 = mean(w.slice(-50).map(d => d.close));
    const rets = [];
    for (let k = 1; k < w.length; k++) rets.push((w[k].close - w[k - 1].close) / w[k - 1].close);
    const vol = std(rets) * Math.sqrt(252);
    const bull = px > sma50;
    const highVol = vol > 0.25;
    return `${bull ? 'ALCISTA' : 'BAJISTA'} / ${highVol ? 'VOL ALTA' : 'VOL BAJA'}`;
  };

  const groups = {};
  trades.forEach(t => {
    const r = regimeOf(t.entryDate);
    if (!r) return;
    (groups[r] ||= []).push(t);
  });

  return Object.entries(groups).map(([regime, ts]) => {
    const rets = ts.map(t => t.ret);
    const wins = ts.filter(t => t.ret > 0).length;
    const sd = std(rets);
    return {
      regime,
      n: ts.length,
      winRate: +(wins / ts.length * 100).toFixed(1),
      avgRet: +mean(rets).toFixed(2),
      sharpe: sd > 0 ? +(mean(rets) / sd).toFixed(2) : 0,
      total: +rets.reduce((a, b) => a + b, 0).toFixed(1),
    };
  }).sort((a, b) => b.n - a.n);
}

// ══════════════════════════════════════════════════════════════
// 17. SELECCIÓN CONSCIENTE DE CORRELACIÓN
//    Tomar 5 señales de tech megacap no es diversificar:
//    es una sola apuesta con 5 tickets.
// ══════════════════════════════════════════════════════════════
export function correlationMatrix(seriesMap, window = 60) {
  const tickers = Object.keys(seriesMap);
  const rets = {};
  tickers.forEach(t => {
    const s = seriesMap[t].slice(-window - 1);
    const r = [];
    for (let i = 1; i < s.length; i++) r.push((s[i].close - s[i - 1].close) / s[i - 1].close);
    rets[t] = r;
  });
  const corr = {};
  tickers.forEach(a => {
    corr[a] = {};
    tickers.forEach(b => {
      const ra = rets[a], rb = rets[b];
      const n = Math.min(ra.length, rb.length);
      if (n < 10) { corr[a][b] = 0; return; }
      const ma = mean(ra.slice(-n)), mb = mean(rb.slice(-n));
      let num = 0, da = 0, db = 0;
      for (let i = 0; i < n; i++) {
        const xa = ra[ra.length - n + i] - ma, xb = rb[rb.length - n + i] - mb;
        num += xa * xb; da += xa * xa; db += xb * xb;
      }
      corr[a][b] = (da * db) > 0 ? num / Math.sqrt(da * db) : 0;
    });
  });
  return corr;
}

export function selectDiversified(candidates, corrMatrix, maxN, maxCorr = 0.75) {
  // candidates ya ordenados por atractivo (mejor primero)
  const chosen = [];
  for (const c of candidates) {
    if (chosen.length >= maxN) break;
    const tooClose = chosen.some(s => Math.abs(corrMatrix?.[c.ticker]?.[s.ticker] ?? 0) > maxCorr);
    if (!tooClose) chosen.push(c);
  }
  // si quedaron slots libres, completar relajando el límite
  if (chosen.length < maxN) {
    for (const c of candidates) {
      if (chosen.length >= maxN) break;
      if (!chosen.includes(c)) chosen.push(c);
    }
  }
  return chosen;
}
