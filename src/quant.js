// ══════════════════════════════════════════════════════════════════════
//  FXCA16 — MOTOR CUANTITATIVO
//  Convierte el sistema de "tablero de indicadores" a "modelo validado".
//
//  Capas:
//   1. Etiquetado triple-barrera (López de Prado) — define qué es "ganar"
//   2. Extracción de features + regresión logística — APRENDE los pesos
//   3. Calibración de probabilidad (Platt) + Brier/reliability
//   4. Validación purged K-fold con embargo — sin fuga temporal
//   5. Backtest de CARTERA — Sharpe, Sortino, MaxDD, CAGR, Calmar
//   6. Kelly fraccional — sizing derivado de la probabilidad calibrada
// ══════════════════════════════════════════════════════════════════════

// ── util ──
export const mean = a => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;
export const std = a => {
  if (a.length < 2) return 0;
  const m = mean(a);
  return Math.sqrt(a.reduce((s, v) => s + (v - m) ** 2, 0) / (a.length - 1));
};
const clip = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

// ══════════════════════════════════════════════════════════════
// 1. ETIQUETADO TRIPLE-BARRERA
//    En vez de "¿subió en N días?" (que ignora el camino), define
//    tres barreras: take-profit, stop-loss y límite temporal.
//    La primera que se toca determina la etiqueta. Es el estándar
//    para ML financiero porque respeta cómo se opera de verdad.
// ══════════════════════════════════════════════════════════════
export function tripleBarrier(bars, i, atr, { pt = 2.0, sl = 1.5, maxH = 20, cost = 0 } = {}) {
  const entry = bars[i]?.close;
  if (!entry || !atr || atr <= 0) return null;
  const upper = entry + atr * pt;
  const lower = entry - atr * sl;
  const end = Math.min(bars.length - 1, i + maxH);

  for (let j = i + 1; j <= end; j++) {
    const b = bars[j];
    // Conservador: si en la misma barra se tocan ambas, asume el stop
    if (b.low <= lower) return { label: 0, ret: (lower - entry) / entry - cost, bars: j - i, hit: 'SL' };
    if (b.high >= upper) return { label: 1, ret: (upper - entry) / entry - cost, bars: j - i, hit: 'TP' };
  }
  const exit = bars[end]?.close ?? entry;
  const ret = (exit - entry) / entry - cost;
  return { label: ret > 0 ? 1 : 0, ret, bars: end - i, hit: 'TIME' };
}

// ══════════════════════════════════════════════════════════════
// 2. FEATURES — vector numérico normalizado
//    Cada feature está escalada para que la regresión no quede
//    dominada por la de mayor magnitud.
// ══════════════════════════════════════════════════════════════
export const FEATURE_NAMES = [
  'roc10', 'roc5', 'macdN', 'smaSpread', 'px_vs_sma200',
  'bbPos', 'rsiN', 'volRatio', 'atrPct', 'mom5', 'trendAlign', 'volTrend',
];

export function extractFeatures(bars, i) {
  if (i < 60 || i >= bars.length) return null;
  const w = bars.slice(Math.max(0, i - 200), i + 1);
  const n = w.length - 1;
  const px = w[n].close;
  if (!px) return null;

  const sma = p => { const s = w.slice(-p); return s.length ? mean(s.map(d => d.close)) : px; };
  const a20 = sma(20), a50 = sma(50), a200 = sma(Math.min(200, w.length));

  const roc = p => { const prev = w[n - p]?.close; return prev ? (px - prev) / prev * 100 : 0; };
  const roc10 = roc(10), roc5 = roc(5), mom5 = roc(5);

  // MACD normalizado por precio
  const ema = (arr, p) => { const k = 2 / (p + 1); let e = arr[0]; for (const v of arr) e = v * k + e * (1 - k); return e; };
  const cl = w.map(d => d.close);
  const macd = ema(cl, 12) - ema(cl, 26);
  const macdN = macd / px * 100;

  // Bollinger: posición relativa 0..1
  const s20 = w.slice(-20).map(d => d.close);
  const m20 = mean(s20), sd20 = std(s20);
  const bbPos = sd20 > 0 ? clip((px - (m20 - 2 * sd20)) / (4 * sd20), 0, 1) : 0.5;

  // RSI normalizado
  let g = 0, l = 0;
  for (let k = n - 13; k <= n; k++) { const d = w[k].close - w[k - 1].close; d > 0 ? g += d : l -= d; }
  const rsi = l === 0 ? 100 : 100 - 100 / (1 + (g / 14) / (l / 14));

  // ATR como % del precio
  let tr = 0;
  for (let k = n - 13; k <= n; k++) {
    tr += Math.max(w[k].high - w[k].low, Math.abs(w[k].high - w[k - 1].close), Math.abs(w[k].low - w[k - 1].close));
  }
  const atrPct = (tr / 14) / px * 100;

  const vols = w.map(d => d.volume || 0);
  const vRecent = mean(vols.slice(-5)), vBase = mean(vols.slice(-20));
  const volRatio = vBase > 0 ? vRecent / vBase : 1;
  const vOld = mean(vols.slice(-40, -20));
  const volTrend = vOld > 0 ? vBase / vOld : 1;

  return {
    roc10: clip(roc10 / 5, -3, 3),
    roc5: clip(roc5 / 3, -3, 3),
    macdN: clip(macdN / 2, -3, 3),
    smaSpread: clip((a20 / a50 - 1) * 50, -3, 3),
    px_vs_sma200: clip((px / a200 - 1) * 20, -3, 3),
    bbPos: (bbPos - 0.5) * 4,
    rsiN: (rsi - 50) / 20,
    volRatio: clip((volRatio - 1) * 2, -3, 3),
    atrPct: clip((atrPct - 2) / 2, -3, 3),
    mom5: clip(mom5 / 3, -3, 3),
    trendAlign: (px > a20 ? 1 : -1) + (a20 > a50 ? 1 : -1) + (a50 > a200 ? 1 : -1),
    volTrend: clip((volTrend - 1) * 2, -3, 3),
    _atr: (tr / 14),
  };
}

// ══════════════════════════════════════════════════════════════
// 3. REGRESIÓN LOGÍSTICA — aprende los pesos desde los datos
//    Reemplaza las constantes escritas a mano (+20 ROC, +18 BB...).
//    Con regularización L2 para evitar sobreajuste.
// ══════════════════════════════════════════════════════════════
export function trainLogistic(X, y, { epochs = 300, lr = 0.08, l2 = 0.01 } = {}) {
  if (!X.length) return null;
  const d = X[0].length;
  let w = new Array(d).fill(0), b = 0;
  const N = X.length;

  for (let ep = 0; ep < epochs; ep++) {
    const gw = new Array(d).fill(0);
    let gb = 0;
    for (let i = 0; i < N; i++) {
      let z = b;
      for (let j = 0; j < d; j++) z += w[j] * X[i][j];
      const p = 1 / (1 + Math.exp(-clip(z, -30, 30)));
      const err = p - y[i];
      for (let j = 0; j < d; j++) gw[j] += err * X[i][j];
      gb += err;
    }
    for (let j = 0; j < d; j++) w[j] -= lr * (gw[j] / N + l2 * w[j]);
    b -= lr * (gb / N);
  }
  return { w, b };
}

export function predictProba(model, x) {
  if (!model) return 0.5;
  let z = model.b;
  for (let j = 0; j < x.length; j++) z += model.w[j] * x[j];
  return 1 / (1 + Math.exp(-clip(z, -30, 30)));
}

// ══════════════════════════════════════════════════════════════
// 4. CALIBRACIÓN (Platt scaling) + métricas de calidad probabilística
//    Un modelo puede acertar la dirección pero mentir en la confianza.
//    Calibrar significa: "cuando digo 70%, gana el 70% de las veces".
// ══════════════════════════════════════════════════════════════
export function fitPlatt(probs, outcomes, { epochs = 400, lr = 0.15 } = {}) {
  let A = 1, B = 0;
  const N = probs.length;
  if (!N) return { A, B };
  const logit = p => Math.log(clip(p, 1e-6, 1 - 1e-6) / (1 - clip(p, 1e-6, 1 - 1e-6)));
  for (let e = 0; e < epochs; e++) {
    let gA = 0, gB = 0;
    for (let i = 0; i < N; i++) {
      const z = A * logit(probs[i]) + B;
      const q = 1 / (1 + Math.exp(-clip(z, -30, 30)));
      const err = q - outcomes[i];
      gA += err * logit(probs[i]);
      gB += err;
    }
    A -= lr * gA / N;
    B -= lr * gB / N;
  }
  return { A, B };
}

export function applyPlatt(cal, p) {
  if (!cal) return p;
  const lp = Math.log(clip(p, 1e-6, 1 - 1e-6) / (1 - clip(p, 1e-6, 1 - 1e-6)));
  return 1 / (1 + Math.exp(-clip(cal.A * lp + cal.B, -30, 30)));
}

export function brierScore(probs, outcomes) {
  if (!probs.length) return null;
  return +mean(probs.map((p, i) => (p - outcomes[i]) ** 2)).toFixed(4);
}

// Brier skill score vs. predecir siempre la tasa base
export function brierSkill(probs, outcomes) {
  const bs = brierScore(probs, outcomes);
  const base = mean(outcomes);
  const bsRef = mean(outcomes.map(o => (base - o) ** 2));
  return bsRef > 0 ? +(1 - bs / bsRef).toFixed(4) : 0;
}

export function reliabilityCurve(probs, outcomes, bins = 10) {
  const out = [];
  for (let b = 0; b < bins; b++) {
    const lo = b / bins, hi = (b + 1) / bins;
    const idx = probs.map((p, i) => (p >= lo && p < hi) ? i : -1).filter(i => i >= 0);
    if (idx.length < 3) continue;
    out.push({
      binLo: +lo.toFixed(2), binHi: +hi.toFixed(2), n: idx.length,
      predicted: +mean(idx.map(i => probs[i])).toFixed(3),
      actual: +mean(idx.map(i => outcomes[i])).toFixed(3),
    });
  }
  return out;
}

// AUC-ROC por el método de rangos (Mann-Whitney)
export function aucRoc(probs, outcomes) {
  const pos = probs.filter((_, i) => outcomes[i] === 1);
  const neg = probs.filter((_, i) => outcomes[i] === 0);
  if (!pos.length || !neg.length) return 0.5;
  let wins = 0;
  const step = Math.max(1, Math.floor(pos.length * neg.length / 200000)); // muestreo si es enorme
  let cnt = 0;
  for (let i = 0; i < pos.length; i += 1) {
    for (let j = 0; j < neg.length; j += step) {
      wins += pos[i] > neg[j] ? 1 : pos[i] === neg[j] ? 0.5 : 0;
      cnt++;
    }
  }
  return cnt ? +(wins / cnt).toFixed(4) : 0.5;
}

// ══════════════════════════════════════════════════════════════
// 5. PURGED K-FOLD con embargo
//    En series temporales el K-fold normal filtra información:
//    una muestra de train puede solaparse con el horizonte de una
//    de test. Purgar + embargo elimina esa fuga.
// ══════════════════════════════════════════════════════════════
export function purgedKFold(n, k = 5, horizon = 20, embargoPct = 0.01) {
  const folds = [];
  const size = Math.floor(n / k);
  const emb = Math.ceil(n * embargoPct);
  for (let f = 0; f < k; f++) {
    const tLo = f * size, tHi = (f === k - 1) ? n : (f + 1) * size;
    const test = [];
    for (let i = tLo; i < tHi; i++) test.push(i);
    const train = [];
    for (let i = 0; i < n; i++) {
      if (i >= tLo - horizon - emb && i < tHi + horizon + emb) continue; // purga + embargo
      train.push(i);
    }
    if (train.length > 30 && test.length > 5) folds.push({ train, test });
  }
  return folds;
}

// ══════════════════════════════════════════════════════════════
// 6. KELLY FRACCIONAL
//    f* = (p·b − q) / b   con b = payoff (R/R)
//    Se usa 1/4 de Kelly: Kelly completo es óptimo en crecimiento
//    pero con drawdowns intolerables en la práctica.
// ══════════════════════════════════════════════════════════════
export function kellyFraction(p, b, fraction = 0.25, cap = 0.05) {
  if (!b || b <= 0) return 0;
  const q = 1 - p;
  const f = (p * b - q) / b;
  return clip(f * fraction, 0, cap);
}

// ══════════════════════════════════════════════════════════════
// 7. MÉTRICAS DE CARTERA
// ══════════════════════════════════════════════════════════════
export function portfolioMetrics(equity, periodsPerYear = 252) {
  if (!equity || equity.length < 3) return null;
  const rets = [];
  for (let i = 1; i < equity.length; i++) {
    if (equity[i - 1] > 0) rets.push(equity[i] / equity[i - 1] - 1);
  }
  if (!rets.length) return null;

  const mu = mean(rets), sd = std(rets);
  const downside = rets.filter(r => r < 0);
  const dsd = downside.length ? Math.sqrt(mean(downside.map(r => r * r))) : 0;

  // Max drawdown
  let peak = equity[0], maxDD = 0, ddDur = 0, curDur = 0;
  for (const v of equity) {
    if (v > peak) { peak = v; curDur = 0; } else { curDur++; ddDur = Math.max(ddDur, curDur); }
    maxDD = Math.min(maxDD, v / peak - 1);
  }

  const years = equity.length / periodsPerYear;
  const totalRet = equity[equity.length - 1] / equity[0] - 1;
  const cagr = years > 0 && equity[0] > 0
    ? Math.pow(equity[equity.length - 1] / equity[0], 1 / years) - 1 : 0;

  const sharpe = sd > 0 ? (mu / sd) * Math.sqrt(periodsPerYear) : 0;
  const sortino = dsd > 0 ? (mu / dsd) * Math.sqrt(periodsPerYear) : 0;
  const calmar = maxDD < 0 ? cagr / Math.abs(maxDD) : 0;

  return {
    totalRet: +(totalRet * 100).toFixed(2),
    cagr: +(cagr * 100).toFixed(2),
    sharpe: +sharpe.toFixed(2),
    sortino: +sortino.toFixed(2),
    maxDD: +(maxDD * 100).toFixed(2),
    ddDuration: ddDur,
    calmar: +calmar.toFixed(2),
    volAnual: +(sd * Math.sqrt(periodsPerYear) * 100).toFixed(2),
    nPeriods: equity.length,
  };
}

// ══════════════════════════════════════════════════════════════
// 8. BACKTEST DE CARTERA
//    Lo que faltaba: simula la estrategia COMPLETA, no ticker a
//    ticker. Cada día toma las mejores señales, asigna capital,
//    descuenta costos y sigue la curva de equity real.
// ══════════════════════════════════════════════════════════════
export function portfolioBacktest(universe, {
  topN = 5,          // posiciones simultáneas máximas
  costPct = 1.8,     // costo round-trip %
  holdDays = 10,     // horizonte de tenencia
  minProb = 0.55,    // probabilidad mínima para entrar
  useKelly = true,
  capital0 = 1000000,
  maxWeight = 0.25,  // límite por posición
} = {}) {
  // universe: [{ ticker, daily:[{date,open,high,low,close,volume}], model, cal }]
  const dateSet = new Set();
  universe.forEach(u => u.daily.forEach(d => dateSet.add(d.date)));
  const dates = [...dateSet].sort();
  if (dates.length < 80) return null;

  const idxByTicker = {};
  universe.forEach(u => {
    idxByTicker[u.ticker] = {};
    u.daily.forEach((d, i) => { idxByTicker[u.ticker][d.date] = i; });
  });

  let cash = capital0;
  const positions = [];      // {ticker, shares, entry, entryDate, exitDate, stop, target}
  const equity = [];
  const trades = [];
  const startIdx = 70;

  for (let di = startIdx; di < dates.length; di++) {
    const today = dates[di];

    // ── 1. Cerrar posiciones que vencen o tocan barrera ──
    for (let pi = positions.length - 1; pi >= 0; pi--) {
      const pos = positions[pi];
      const u = universe.find(x => x.ticker === pos.ticker);
      const bi = idxByTicker[pos.ticker]?.[today];
      if (bi === undefined) continue;
      const bar = u.daily[bi];
      let exitPx = null, reason = null;

      if (bar.low <= pos.stop) { exitPx = pos.stop; reason = 'SL'; }
      else if (bar.high >= pos.target) { exitPx = pos.target; reason = 'TP'; }
      else if (di >= pos.exitIdx) { exitPx = bar.close; reason = 'TIME'; }

      if (exitPx) {
        const gross = pos.shares * exitPx;
        const net = gross * (1 - costPct / 200); // mitad del costo a la salida
        cash += net;
        const pnl = net - pos.costBasis;
        trades.push({
          ticker: pos.ticker, entryDate: pos.entryDate, exitDate: today,
          entry: +pos.entry.toFixed(2), exit: +exitPx.toFixed(2),
          ret: +(pnl / pos.costBasis * 100).toFixed(2), pnl: +pnl.toFixed(0), reason,
          prob: +pos.prob.toFixed(3),
        });
        positions.splice(pi, 1);
      }
    }

    // ── 2. Buscar nuevas señales ──
    const slots = topN - positions.length;
    if (slots > 0) {
      const cands = [];
      for (const u of universe) {
        if (positions.some(p => p.ticker === u.ticker)) continue;
        const bi = idxByTicker[u.ticker]?.[today];
        if (bi === undefined || bi < 60) continue;
        const f = extractFeatures(u.daily, bi);
        if (!f) continue;
        const x = FEATURE_NAMES.map(k => f[k]);
        const raw = predictProba(u.model, x);
        const p = applyPlatt(u.cal, raw);
        if (p >= minProb) cands.push({ u, bi, p, atr: f._atr });
      }
      cands.sort((a, b) => b.p - a.p);

      for (const c of cands.slice(0, slots)) {
        const bar = c.u.daily[c.bi];
        const entry = bar.close;
        const atr = c.atr || entry * 0.02;
        const stop = entry - atr * 1.5;
        const target = entry + atr * 2.0;
        const b = (target - entry) / (entry - stop);   // payoff
        const equityNow = cash + positions.reduce((s, p) => {
          const bi2 = idxByTicker[p.ticker]?.[today];
          const u2 = universe.find(x => x.ticker === p.ticker);
          return s + p.shares * (bi2 !== undefined ? u2.daily[bi2].close : p.entry);
        }, 0);

        const w = useKelly ? kellyFraction(c.p, b, 0.25, maxWeight) : 1 / topN;
        const alloc = Math.min(equityNow * w, cash * 0.95);
        if (alloc < equityNow * 0.01) continue;   // muy chico, no vale el costo

        const shares = Math.floor(alloc / entry);
        if (shares <= 0) continue;
        const costBasis = shares * entry * (1 + costPct / 200); // mitad del costo a la entrada
        if (costBasis > cash) continue;
        cash -= costBasis;
        positions.push({
          ticker: c.u.ticker, shares, entry, costBasis,
          entryDate: today, exitIdx: di + holdDays,
          stop, target, prob: c.p,
        });
      }
    }

    // ── 3. Marcar a mercado ──
    const mtm = positions.reduce((s, p) => {
      const bi = idxByTicker[p.ticker]?.[today];
      const u = universe.find(x => x.ticker === p.ticker);
      return s + p.shares * (bi !== undefined ? u.daily[bi].close : p.entry);
    }, 0);
    equity.push(cash + mtm);
  }

  const metrics = portfolioMetrics(equity);
  const wins = trades.filter(t => t.ret > 0).length;

  return {
    equity, trades, metrics,
    nTrades: trades.length,
    winRate: trades.length ? +(wins / trades.length * 100).toFixed(1) : 0,
    avgRet: trades.length ? +mean(trades.map(t => t.ret)).toFixed(2) : 0,
    avgWin: wins ? +mean(trades.filter(t => t.ret > 0).map(t => t.ret)).toFixed(2) : 0,
    avgLoss: (trades.length - wins) ? +mean(trades.filter(t => t.ret <= 0).map(t => t.ret)).toFixed(2) : 0,
    profitFactor: (() => {
      const gp = trades.filter(t => t.pnl > 0).reduce((s, t) => s + t.pnl, 0);
      const gl = Math.abs(trades.filter(t => t.pnl <= 0).reduce((s, t) => s + t.pnl, 0));
      return gl > 0 ? +(gp / gl).toFixed(2) : 0;
    })(),
    exitBreakdown: {
      TP: trades.filter(t => t.reason === 'TP').length,
      SL: trades.filter(t => t.reason === 'SL').length,
      TIME: trades.filter(t => t.reason === 'TIME').length,
    },
  };
}

// ══════════════════════════════════════════════════════════════
// 9. ABLACIÓN DE FEATURES
//    Quita una feature por vez y mide cuánto cae el AUC.
//    Revela cuáles indicadores aportan y cuáles son ruido.
// ══════════════════════════════════════════════════════════════
export function featureAblation(X, y, baseAuc) {
  const d = X[0]?.length || 0;
  const out = [];
  for (let j = 0; j < d; j++) {
    const Xr = X.map(row => row.map((v, k) => k === j ? 0 : v));
    const m = trainLogistic(Xr, y, { epochs: 150 });
    const p = Xr.map(r => predictProba(m, r));
    const auc = aucRoc(p, y);
    out.push({
      feature: FEATURE_NAMES[j],
      aucSin: +auc.toFixed(4),
      delta: +(baseAuc - auc).toFixed(4),
    });
  }
  return out.sort((a, b) => b.delta - a.delta);
}

// ══════════════════════════════════════════════════════════════
// 10. ENTRENAMIENTO COMPLETO DE UN TICKER
//     Junta todo: features → etiquetas → CV purgado → calibración
// ══════════════════════════════════════════════════════════════
export function trainTicker(daily, { pt = 2.0, sl = 1.5, maxH = 20, cost = 0.018 } = {}) {
  if (!daily || daily.length < 150) return null;

  const X = [], y = [], rets = [];
  for (let i = 60; i < daily.length - maxH; i++) {
    const f = extractFeatures(daily, i);
    if (!f) continue;
    const lab = tripleBarrier(daily, i, f._atr, { pt, sl, maxH, cost });
    if (!lab) continue;
    X.push(FEATURE_NAMES.map(k => f[k]));
    y.push(lab.label);
    rets.push(lab.ret);
  }
  if (X.length < 60) return null;

  // CV purgado: probabilidades fuera de muestra
  const folds = purgedKFold(X.length, 5, maxH, 0.01);
  const oosP = new Array(X.length).fill(null);
  for (const fold of folds) {
    const Xtr = fold.train.map(i => X[i]), ytr = fold.train.map(i => y[i]);
    const m = trainLogistic(Xtr, ytr, { epochs: 250 });
    fold.test.forEach(i => { oosP[i] = predictProba(m, X[i]); });
  }
  const validIdx = oosP.map((p, i) => p !== null ? i : -1).filter(i => i >= 0);
  if (validIdx.length < 40) return null;

  const pOos = validIdx.map(i => oosP[i]);
  const yOos = validIdx.map(i => y[i]);

  const cal = fitPlatt(pOos, yOos);
  const pCal = pOos.map(p => applyPlatt(cal, p));

  // Modelo final entrenado con todo
  const model = trainLogistic(X, y, { epochs: 400 });

  const auc = aucRoc(pOos, yOos);

  return {
    model, cal,
    n: X.length,
    baseRate: +mean(y).toFixed(3),
    auc,
    brier: brierScore(pCal, yOos),
    brierSkill: brierSkill(pCal, yOos),
    reliability: reliabilityCurve(pCal, yOos, 8),
    avgRet: +(mean(rets) * 100).toFixed(3),
    weights: FEATURE_NAMES.map((name, j) => ({ name, w: +model.w[j].toFixed(3) }))
      .sort((a, b) => Math.abs(b.w) - Math.abs(a.w)),
    X, y,
  };
}
