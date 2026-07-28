// ══════════════════════════════════════════════════════════════════════
//  FXCA16 — MOTOR DE ALFA CROSS-SECTIONAL
//
//  El cambio de paradigma: el sistema anterior predecía RETORNO ABSOLUTO
//  ("¿subirá AAPL?"). Eso está dominado por el mercado — por eso el edge
//  aparecía solo en el mes del rally del 25%.
//
//  Este motor predice RETORNO RELATIVO ("¿AAPL rendirá más que el resto
//  del universo?"). El beta de mercado se cancela por construcción, que
//  es como operan casi todos los fondos cuantitativos de acciones.
//
//  Contenido:
//   1. Features cross-sectionales (z-score y rango dentro de cada fecha)
//   2. Momentum residual — momentum limpio de beta
//   3. Information Coefficient (IC) — la métrica estándar de alfa
//   4. Decay del IC — cuántos días dura la señal
//   5. Análisis por quintiles — spread Q5−Q1
//   6. Gradient boosting (árboles) — supera a la regresión lineal
//   7. Combinación de señales ponderada por IC
// ══════════════════════════════════════════════════════════════════════

const mean = a => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;
const std = a => {
  if (a.length < 2) return 0;
  const m = mean(a);
  return Math.sqrt(a.reduce((s, v) => s + (v - m) ** 2, 0) / (a.length - 1));
};
const clip = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

// ══════════════════════════════════════════════════════════════
// 1. FEATURES CROSS-SECTIONALES
//    Cada feature se calcula en crudo y después se normaliza
//    DENTRO de cada fecha. Así "momentum alto" significa
//    "alto comparado con los demás hoy", no "alto en absoluto".
// ══════════════════════════════════════════════════════════════
export const ALPHA_FEATURES = [
  'mom_12_1',      // momentum 12 meses excluyendo el último (anomalía clásica)
  'mom_1m',        // reversión de corto plazo (signo invertido: efecto reversal)
  'mom_resid',     // momentum residual, limpio de beta
  'vol_idio',      // volatilidad idiosincrática (baja vol → mejor retorno ajustado)
  'vol_shock',     // cambio de volumen vs su base
  'dist_max',      // distancia al máximo de 52 semanas
  'aceleracion',   // segunda derivada del precio
  'consistencia',  // % de días positivos (calidad de la tendencia)
  'amihud',        // iliquidez de Amihud: |ret| / volumen
  'skew_ret',      // asimetría de los retornos recientes
];

// Calcula las features CRUDAS de un ticker en el índice i (sin lookahead)
export function rawFeatures(daily, i, benchRets) {
  if (i < 130 || i >= daily.length) return null;
  const px = daily[i].close;
  if (!px) return null;

  const ret = (a, b) => {
    const p0 = daily[b]?.close, p1 = daily[a]?.close;
    return (p0 && p1) ? (p1 - p0) / p0 : 0;
  };

  // Retornos diarios recientes
  const rets = [];
  for (let k = Math.max(1, i - 62); k <= i; k++) {
    const p0 = daily[k - 1]?.close, p1 = daily[k]?.close;
    if (p0 && p1) rets.push((p1 - p0) / p0);
  }
  if (rets.length < 30) return null;

  // ── Momentum 12-1: sube en 12 meses pero excluyendo el último mes ──
  const mom_12_1 = ret(i - 21, Math.max(0, i - 252));
  // ── Reversión de corto plazo (se usa con signo negativo) ──
  const mom_1m = ret(i, i - 21);

  // ── Momentum residual: quita la parte explicada por el mercado ──
  let mom_resid = mom_12_1;
  if (benchRets && benchRets.length >= rets.length) {
    const b = benchRets.slice(-rets.length);
    const mb = mean(b), mr = mean(rets);
    let cov = 0, varb = 0;
    for (let k = 0; k < rets.length; k++) {
      cov += (rets[k] - mr) * (b[k] - mb);
      varb += (b[k] - mb) ** 2;
    }
    const beta = varb > 0 ? cov / varb : 1;
    const benchMom = b.reduce((a, x) => a + x, 0);
    mom_resid = mom_12_1 - beta * benchMom;
  }

  // ── Volatilidad idiosincrática ──
  const vol_idio = std(rets) * Math.sqrt(252);

  // ── Shock de volumen ──
  const vols = [];
  for (let k = Math.max(0, i - 62); k <= i; k++) vols.push(daily[k]?.volume || 0);
  const vRec = mean(vols.slice(-5)), vBase = mean(vols) || 1;
  const vol_shock = vBase > 0 ? vRec / vBase : 1;

  // ── Distancia al máximo de 52 semanas ──
  const win = daily.slice(Math.max(0, i - 252), i + 1);
  const hi52 = Math.max(...win.map(d => d.high ?? d.close));
  const dist_max = hi52 > 0 ? (px / hi52 - 1) : 0;

  // ── Aceleración: momentum reciente vs momentum previo ──
  const m_rec = ret(i, i - 21);
  const m_prev = ret(i - 21, i - 42);
  const aceleracion = m_rec - m_prev;

  // ── Consistencia: proporción de días positivos ──
  const consistencia = rets.filter(r => r > 0).length / rets.length;

  // ── Iliquidez de Amihud ──
  let amihudSum = 0, cnt = 0;
  for (let k = Math.max(1, i - 21); k <= i; k++) {
    const p0 = daily[k - 1]?.close, p1 = daily[k]?.close, v = daily[k]?.volume || 0;
    if (p0 && p1 && v > 0) { amihudSum += Math.abs((p1 - p0) / p0) / (v * p1); cnt++; }
  }
  const amihud = cnt ? Math.log(1 + amihudSum / cnt * 1e12) : 0;

  // ── Asimetría de retornos ──
  const sd = std(rets), mr2 = mean(rets);
  const skew_ret = sd > 0 ? mean(rets.map(r => ((r - mr2) / sd) ** 3)) : 0;

  return {
    mom_12_1, mom_1m, mom_resid, vol_idio, vol_shock,
    dist_max, aceleracion, consistencia, amihud, skew_ret,
  };
}

// ══════════════════════════════════════════════════════════════
// 2. NORMALIZACIÓN CROSS-SECTIONAL
//    Convierte cada feature a rango percentil dentro de la fecha.
//    Robusto a outliers, que es crítico en datos financieros.
// ══════════════════════════════════════════════════════════════
export function normalizarCrossSection(porFecha) {
  // porFecha: { fecha: [ {ticker, raw:{...}, fwd} ] }
  const salida = [];
  for (const [fecha, filas] of Object.entries(porFecha)) {
    if (filas.length < 10) continue;   // sin universo suficiente no hay corte transversal

    const norm = {};
    for (const f of ALPHA_FEATURES) {
      const vals = filas.map(r => r.raw?.[f]).filter(v => v != null && isFinite(v));
      if (vals.length < 10) continue;
      const orden = [...vals].sort((a, b) => a - b);
      norm[f] = filas.map(r => {
        const v = r.raw?.[f];
        if (v == null || !isFinite(v)) return 0;
        // rango percentil centrado en 0, rango [-1, 1]
        const pos = orden.findIndex(x => x >= v);
        return (pos / Math.max(1, orden.length - 1)) * 2 - 1;
      });
    }

    // Target: retorno relativo al universo de esa fecha
    const fwdMedio = mean(filas.map(r => r.fwd).filter(isFinite));
    filas.forEach((r, idx) => {
      const x = ALPHA_FEATURES.map(f => norm[f] ? norm[f][idx] : 0);
      salida.push({
        ticker: r.ticker, fecha,
        x,
        fwdAbs: r.fwd,
        fwdRel: r.fwd - fwdMedio,          // ← el target que importa
        nUniverso: filas.length,
      });
    });
  }
  return salida;
}

// ══════════════════════════════════════════════════════════════
// 3. INFORMATION COEFFICIENT
//    Correlación de Spearman entre predicción y retorno realizado,
//    calculada FECHA POR FECHA y después promediada.
//    Es la métrica estándar de calidad de un alfa.
//
//    IC > 0.03 ya es explotable a escala.
//    IC > 0.05 es bueno.  IR = IC medio / desvío del IC.
// ══════════════════════════════════════════════════════════════
function spearman(a, b) {
  const n = a.length;
  if (n < 5) return 0;
  const rank = arr => {
    const idx = arr.map((v, i) => [v, i]).sort((x, y) => x[0] - y[0]);
    const r = new Array(n);
    idx.forEach(([, i], k) => r[i] = k);
    return r;
  };
  const ra = rank(a), rb = rank(b);
  const ma = mean(ra), mb = mean(rb);
  let num = 0, da = 0, db = 0;
  for (let i = 0; i < n; i++) {
    const x = ra[i] - ma, y = rb[i] - mb;
    num += x * y; da += x * x; db += y * y;
  }
  return (da * db) > 0 ? num / Math.sqrt(da * db) : 0;
}

export function informationCoefficient(datos, predictor) {
  // datos: [{fecha, x, fwdRel}]
  const porFecha = {};
  datos.forEach(d => (porFecha[d.fecha] ||= []).push(d));

  const ics = [];
  for (const filas of Object.values(porFecha)) {
    if (filas.length < 10) continue;
    const pred = filas.map(predictor);
    const real = filas.map(f => f.fwdRel);
    ics.push(spearman(pred, real));
  }
  if (ics.length < 5) return null;

  const icMedio = mean(ics), icSd = std(ics);
  const ir = icSd > 0 ? icMedio / icSd : 0;
  const t = icSd > 0 ? icMedio / (icSd / Math.sqrt(ics.length)) : 0;

  return {
    ic: +icMedio.toFixed(4),
    icSd: +icSd.toFixed(4),
    ir: +ir.toFixed(3),               // Information Ratio
    t: +t.toFixed(2),
    nFechas: ics.length,
    pctPositivo: +(ics.filter(x => x > 0).length / ics.length * 100).toFixed(0),
    serie: ics.map(x => +x.toFixed(4)),
    significativo: Math.abs(t) > 2,
  };
}

// ══════════════════════════════════════════════════════════════
// 4. DECAY DEL IC — ¿cuánto dura la señal?
//    Si el IC cae a cero en 3 días, no sirve para un horizonte
//    de 14. Determina el período de tenencia óptimo.
// ══════════════════════════════════════════════════════════════
export function icDecay(porFechaConHorizontes, predictor, horizontes = [1, 3, 5, 10, 20, 40]) {
  const out = [];
  for (const h of horizontes) {
    const datos = [];
    for (const [fecha, filas] of Object.entries(porFechaConHorizontes)) {
      if (filas.length < 10) continue;
      const key = `fwd${h}`;
      const valid = filas.filter(f => f[key] != null && isFinite(f[key]));
      if (valid.length < 10) continue;
      const medio = mean(valid.map(f => f[key]));
      valid.forEach(f => datos.push({ fecha, x: f.x, fwdRel: f[key] - medio }));
    }
    const r = informationCoefficient(datos, predictor);
    if (r) out.push({ horizonte: h, ic: r.ic, ir: r.ir, t: r.t, nFechas: r.nFechas });
  }
  return out;
}

// ══════════════════════════════════════════════════════════════
// 5. ANÁLISIS POR QUINTILES
//    Ordena por predicción, parte en 5 grupos y mide el retorno
//    de cada uno. Un alfa real muestra progresión monótona
//    Q1 < Q2 < Q3 < Q4 < Q5.
// ══════════════════════════════════════════════════════════════
export function analisisQuintiles(datos, predictor, nQ = 5) {
  const porFecha = {};
  datos.forEach(d => (porFecha[d.fecha] ||= []).push(d));

  const acum = Array.from({ length: nQ }, () => []);
  const spreads = [];

  for (const filas of Object.values(porFecha)) {
    if (filas.length < nQ * 3) continue;
    const orden = [...filas].sort((a, b) => predictor(a) - predictor(b));
    const tam = Math.floor(orden.length / nQ);
    const medias = [];
    for (let q = 0; q < nQ; q++) {
      const grupo = orden.slice(q * tam, q === nQ - 1 ? orden.length : (q + 1) * tam);
      const m = mean(grupo.map(g => g.fwdRel));
      acum[q].push(m);
      medias.push(m);
    }
    spreads.push(medias[nQ - 1] - medias[0]);
  }
  if (spreads.length < 5) return null;

  const quintiles = acum.map((a, i) => ({
    q: i + 1,
    retMedio: +mean(a).toFixed(3),
    nPeriodos: a.length,
  }));

  const spreadMedio = mean(spreads), spreadSd = std(spreads);
  // ¿La progresión es monótona? (correlación entre nº de quintil y retorno)
  const monot = spearman(quintiles.map(q => q.q), quintiles.map(q => q.retMedio));

  return {
    quintiles,
    spread: +spreadMedio.toFixed(3),
    spreadT: spreadSd > 0 ? +(spreadMedio / (spreadSd / Math.sqrt(spreads.length))).toFixed(2) : 0,
    monotonicidad: +monot.toFixed(3),
    pctSpreadPositivo: +(spreads.filter(x => x > 0).length / spreads.length * 100).toFixed(0),
    nPeriodos: spreads.length,
  };
}

// ══════════════════════════════════════════════════════════════
// 6. GRADIENT BOOSTING (árboles de decisión poco profundos)
//    Supera a la regresión lineal porque captura interacciones
//    y no linealidades sin que haya que especificarlas a mano.
// ══════════════════════════════════════════════════════════════
function mejorCorte(X, resid, idx, feat) {
  let best = null;
  const vals = idx.map(i => X[i][feat]).sort((a, b) => a - b);
  if (vals.length < 10) return null;
  // probar cortes en los deciles (rápido y robusto)
  for (let p = 1; p < 10; p++) {
    const umbral = vals[Math.floor(vals.length * p / 10)];
    const izq = [], der = [];
    for (const i of idx) (X[i][feat] <= umbral ? izq : der).push(resid[i]);
    if (izq.length < 5 || der.length < 5) continue;
    const mi = mean(izq), md = mean(der);
    // reducción de suma de cuadrados
    const gan = izq.length * mi * mi + der.length * md * md;
    if (!best || gan > best.gan) best = { feat, umbral, gan, valIzq: mi, valDer: md };
  }
  return best;
}

function construirArbol(X, resid, idx, prof, maxProf) {
  if (prof >= maxProf || idx.length < 20) {
    return { hoja: true, valor: mean(idx.map(i => resid[i])) };
  }
  let best = null;
  const nFeat = X[0].length;
  for (let f = 0; f < nFeat; f++) {
    const c = mejorCorte(X, resid, idx, f);
    if (c && (!best || c.gan > best.gan)) best = c;
  }
  if (!best) return { hoja: true, valor: mean(idx.map(i => resid[i])) };

  const izq = idx.filter(i => X[i][best.feat] <= best.umbral);
  const der = idx.filter(i => X[i][best.feat] > best.umbral);
  if (izq.length < 5 || der.length < 5) {
    return { hoja: true, valor: mean(idx.map(i => resid[i])) };
  }
  return {
    hoja: false, feat: best.feat, umbral: best.umbral,
    izq: construirArbol(X, resid, izq, prof + 1, maxProf),
    der: construirArbol(X, resid, der, prof + 1, maxProf),
  };
}

function predecirArbol(arbol, x) {
  let nodo = arbol;
  while (!nodo.hoja) nodo = x[nodo.feat] <= nodo.umbral ? nodo.izq : nodo.der;
  return nodo.valor;
}

export function entrenarGBM(X, y, { nArboles = 40, lr = 0.08, maxProf = 3 } = {}) {
  if (!X.length) return null;
  const base = mean(y);
  const arboles = [];
  const pred = new Array(X.length).fill(base);
  const idx = X.map((_, i) => i);

  for (let t = 0; t < nArboles; t++) {
    const resid = y.map((v, i) => v - pred[i]);
    const arbol = construirArbol(X, resid, idx, 0, maxProf);
    arboles.push(arbol);
    for (let i = 0; i < X.length; i++) pred[i] += lr * predecirArbol(arbol, X[i]);
  }
  return { base, lr, arboles };
}

export function predecirGBM(modelo, x) {
  if (!modelo) return 0;
  let p = modelo.base;
  for (const a of modelo.arboles) p += modelo.lr * predecirArbol(a, x);
  return p;
}

// Importancia: cuántas veces se usa cada feature y con qué ganancia
export function importanciaGBM(modelo) {
  if (!modelo) return [];
  const cuenta = new Array(ALPHA_FEATURES.length).fill(0);
  const recorrer = nodo => {
    if (!nodo || nodo.hoja) return;
    cuenta[nodo.feat] = (cuenta[nodo.feat] || 0) + 1;
    recorrer(nodo.izq); recorrer(nodo.der);
  };
  modelo.arboles.forEach(recorrer);
  const total = cuenta.reduce((a, b) => a + b, 0) || 1;
  return ALPHA_FEATURES.map((n, i) => ({
    feature: n, usos: cuenta[i], peso: +(cuenta[i] / total * 100).toFixed(1),
  })).sort((a, b) => b.usos - a.usos);
}

// ══════════════════════════════════════════════════════════════
// 7. IC POR FEATURE INDIVIDUAL
//    Antes de combinar, hay que saber cuáles aportan por sí solas.
// ══════════════════════════════════════════════════════════════
export function icPorFeature(datos) {
  return ALPHA_FEATURES.map((f, j) => {
    const r = informationCoefficient(datos, d => d.x[j]);
    return r ? { feature: f, ...r, serie: undefined } : { feature: f, ic: 0, ir: 0, t: 0, significativo: false };
  }).sort((a, b) => Math.abs(b.ic) - Math.abs(a.ic));
}

// Combinación ponderada por IC (señal compuesta simple pero efectiva)
export function señalCompuesta(datos, pesosIC) {
  return d => {
    let s = 0;
    ALPHA_FEATURES.forEach((f, j) => {
      const w = pesosIC[f] || 0;
      s += w * d.x[j];
    });
    return s;
  };
}

// ══════════════════════════════════════════════════════════════
// 8. SEÑAL ALFA VALIDADA
//
//  De todas las combinaciones probadas, esta es la única que
//  sobrevivió validación fuera de muestra y por sub-períodos:
//
//    alpha = rango(vol_shock) − rango(mom_1m)
//
//  Interpretación económica:
//   · vol_shock alto  → hay interés institucional entrando
//   · mom_1m bajo     → el papel viene castigado a corto plazo
//   La combinación busca acumulación silenciosa sobre debilidad
//   reciente, que es la anomalía de reversión con confirmación
//   de volumen.
//
//  Métricas medidas (102 tickers USD, 82 fechas, hold 10d):
//   IC +0.054 · IR +0.37 · t=3.38 · 70% de fechas con IC>0
//   Spread Q5−Q1 +1.12% (t=3.16) · monotonicidad 0.90
//
//  IMPORTANTE: los modelos complejos (GBM, compuesta ponderada
//  por IC) daban IC de 0.18–0.38 EN MUESTRA y −0.12 fuera.
//  La simplicidad acá no es pereza: es lo que generaliza.
// ══════════════════════════════════════════════════════════════
export const ALPHA_VALIDADA = {
  nombre: 'Acumulación sobre debilidad',
  formula: 'rango(vol_shock) − rango(mom_1m)',
  features: ['vol_shock', 'mom_1m'],
  metricas: { ic: 0.054, ir: 0.37, t: 3.38, pctFechas: 70, spreadQ5Q1: 1.12, monotonicidad: 0.90 },
};

export function alphaScore(d) {
  const iVS = ALPHA_FEATURES.indexOf('vol_shock');
  const iM1 = ALPHA_FEATURES.indexOf('mom_1m');
  return d.x[iVS] - d.x[iM1];
}

/**
 * Calcula el ranking alfa del universo en la fecha más reciente.
 * Devuelve percentil 0-100 por ticker: 100 = mejor candidato relativo.
 */
export function rankearUniverso(dataPorTicker, { minBarras = 200 } = {}) {
  // Resamplear a diario
  const uni = {};
  for (const [tk, bars] of Object.entries(dataPorTicker)) {
    if (!bars?.length) continue;
    const by = {}, out = [];
    for (const x of bars) {
      const dia = x.date || x.d;
      if (!dia) continue;
      if (!by[dia]) { by[dia] = { date: dia, open: x.open ?? x.o, high: x.high ?? x.hi, low: x.low ?? x.lo, close: x.close ?? x.c, volume: 0 }; out.push(by[dia]); }
      by[dia].high = Math.max(by[dia].high, x.high ?? x.hi);
      by[dia].low = Math.min(by[dia].low, x.low ?? x.lo);
      by[dia].close = x.close ?? x.c;
      by[dia].volume += (x.volume ?? x.v) || 0;
    }
    if (out.length >= minBarras) uni[tk] = out;
  }
  const tickers = Object.keys(uni);
  if (tickers.length < 15) return null;   // sin universo no hay corte transversal

  // Retornos del universo (proxy de benchmark)
  const cnt = {};
  Object.values(uni).forEach(dl => dl.forEach(x => cnt[x.date] = (cnt[x.date] || 0) + 1));
  const fechas = Object.keys(cnt).filter(f => cnt[f] >= tickers.length * 0.7).sort();
  if (fechas.length < 150) return null;

  const pos = {};
  tickers.forEach(tk => { pos[tk] = {}; uni[tk].forEach((x, i) => pos[tk][x.date] = i); });

  const benchRets = [];
  for (let i = 1; i < fechas.length; i++) {
    const rs = [];
    for (const tk of tickers) {
      const a = pos[tk][fechas[i - 1]], b = pos[tk][fechas[i]];
      if (a != null && b != null) {
        const p0 = uni[tk][a].close, p1 = uni[tk][b].close;
        if (p0 && p1) rs.push((p1 - p0) / p0);
      }
    }
    benchRets.push(rs.length ? mean(rs) : 0);
  }

  // Features en la última fecha común
  const fUlt = fechas[fechas.length - 1];
  const filas = [];
  for (const tk of tickers) {
    const i = pos[tk][fUlt];
    if (i == null || i < 140) continue;
    const rf = rawFeatures(uni[tk], i, benchRets);
    if (!rf) continue;
    filas.push({ ticker: tk, raw: rf });
  }
  if (filas.length < 15) return null;

  // Normalizar cross-section y calcular alfa
  const rank = (vals, v) => {
    const orden = [...vals].sort((a, b) => a - b);
    const p = orden.findIndex(x => x >= v);
    return (p / Math.max(1, orden.length - 1)) * 2 - 1;
  };
  const vsVals = filas.map(r => r.raw.vol_shock).filter(isFinite);
  const m1Vals = filas.map(r => r.raw.mom_1m).filter(isFinite);

  const conAlpha = filas.map(r => {
    const vs = rank(vsVals, r.raw.vol_shock);
    const m1 = rank(m1Vals, r.raw.mom_1m);
    return { ticker: r.ticker, alpha: vs - m1, vol_shock: vs, mom_1m: m1, raw: r.raw };
  });

  // Percentil final 0-100
  const alphas = conAlpha.map(r => r.alpha).sort((a, b) => a - b);
  conAlpha.forEach(r => {
    const p = alphas.findIndex(x => x >= r.alpha);
    r.percentil = Math.round(p / Math.max(1, alphas.length - 1) * 100);
    r.quintil = Math.min(5, Math.floor(r.percentil / 20) + 1);
  });

  return {
    fecha: fUlt,
    nUniverso: conAlpha.length,
    ranking: conAlpha.sort((a, b) => b.alpha - a.alpha),
    porTicker: Object.fromEntries(conAlpha.map(r => [r.ticker, r])),
  };
}
