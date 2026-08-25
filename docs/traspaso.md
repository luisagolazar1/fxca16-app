# FXCA16 — Resumen de traspaso

> Actualizado: 2026-08-25. Reemplaza la versión anterior.
> Los cambios de esta sesión están en el commit `4cb3362` (+ merge `368c298`).

## Repositorio
- **GitHub:** https://github.com/luisagolazar1/fxca16-app
- **App en vivo:** https://fxca16-app.vercel.app
- **Deploy:** automático vía Vercel al hacer push a `main`
- **Rama:** `main` (única)

## Credenciales
- Token de GitHub: **no se replica acá**. Pedirlo al usuario al empezar la sesión.
- ⚠️ El token con permiso de push se compartió por chat en al menos dos sesiones.
  **Recomendado rotarlo** (regenerar en GitHub → Settings → Developer settings).
- Sin otras claves externas — todo corre con `yfinance` (gratuito, sin API key).
- El token no tiene permiso `workflow`, por eso no se puede ampliar el timeout de 15 min
  del workflow. No hace falta: el presupuesto de tiempo (11 min) guarda progreso parcial.

## Cómo retomar
```bash
git clone https://<usuario>:<TOKEN>@github.com/luisagolazar1/fxca16-app.git
cd fxca16-app && npm install
npx vite build          # verificar que compila antes de tocar nada
```
Push (si rechaza por conflicto en `src/data.js`, que lo pisa el workflow):
```bash
git pull --no-rebase -X ours https://<usuario>:<TOKEN>@github.com/.../fxca16-app.git main
git push https://<usuario>:<TOKEN>@github.com/.../fxca16-app.git main
```

### Cómo correr backtests fuera del navegador (montado esta sesión)
`App.jsx` es un archivo React, pero sus funciones puras se pueden correr en Node:
```bash
cp src/App.jsx src/AppLab.jsx
printf '\nexport { combinedSignal, volVsMedia, volMediaMovil, evoFeatures };\n' >> src/AppLab.jsx
npx esbuild src/AppLab.jsx --bundle --format=esm --platform=node \
  --outfile=lab/applab.mjs --external:react --external:./data.js --loader:.png=dataurl
```
Hace falta un stub mínimo de `react` en `lab/node_modules/react` (useState/useMemo/etc. como
no-ops). Mantener `data.js` **externo** al bundle: si se incluye, duplica 57 MB y el proceso
muere por falta de memoria. Borrar `src/AppLab.jsx` antes de commitear.

---

## Qué es el proyecto
App de trading (React + Vite) para acciones USA y Merval Argentina. Señales técnicas (FXCA16)
+ motor de alfa cross-sectional + validación estadística. Corre en el navegador, datos
embebidos en `src/data.js` (no hay backend).

## Estructura
```
src/
  App.jsx          ~6.500 líneas — UI + lógica de señales (combinedSignal, applyP80Threshold…)
  alpha.js         685 líneas — alfa cross-sectional (features, IC, quintiles, ranking)
  quant.js         541 líneas — triple-barrera, regresión logística, backtest de cartera
  quant2.js        605 líneas — meta-labeling, Deflated Sharpe, PBO, consistencia temporal
  data.js          57 MB — generado por el script, NO editar a mano
scripts/update_data.py        734 líneas — descarga de Yahoo, genera data.js
.github/workflows/update-data.yml  — timeout 15 min
```

Exports de `data.js`: `CSV_DATA_DAILY_RAW` (10 años, 158 tickers, ~2.531 barras c/u),
`FXCA16_DYN_PARAMS`, `FXCA16_EARNINGS`, `FXCA16_FUNDAMENTALES`, `FXCA16_NOTICIAS`,
`default` (horario ~1 año, ~1.600 barras), `expandEmbedded`, `expandDaily`.

---

# ⚠️ HALLAZGOS DE ESTA SESIÓN

Ordenados por importancia. Los tres primeros son **bugs de metodología en el propio sistema**,
no resultados de mercado, y son lo más valioso de la sesión.

## 1. `VOL_MEDIA_ANUAL` estaba rota — CORREGIDO ✅

`vol_24h` es un cociente (volumen de la barra ÷ media de las 24 barras previas). Por
construcción su promedio de largo plazo da ≈1 para cualquier activo. Medido sobre 152 tickers:

| | rango entre tickers |
|---|---|
| media móvil real de `vol_24h` | 0,952 – 1,086 |
| constante `VOL_MEDIA_ANUAL` | 0,663 – 1,939 |

La constante era **10× más dispersa que la cantidad que decía medir**. No era la media de
`vol_24h`; se desconoce de dónde salió. El efecto era un **offset fijo por ticker**:

- TSLA: constante 0,663 vs real 0,997 → leía +0,33 de más, siempre. Casi nunca podía marcar
  volumen bajo.
- CECO2: constante 1,939 vs real 1,054 → leía −0,89 de menos, siempre.
- **53% de los tickers (80/152) desviados más de 0,30.**

Consecuencia: al filtrar por "volumen bajo" se estaba seleccionando en buena medida *los
tickers con la constante más inflada*, no días de volumen bajo. Cualquier validación previa
que usara este indicador (incluido el comentario en el código que lo cita como "validado,
t=3,14") está contaminada.

**Corregido:** `volMediaMovil(data, ruedas=126)` — ventana móvil de 6 meses que termina en la
última barra de la **rueda anterior** (la barra evaluada nunca entra en su propia media).
Mínimo 60 ruedas, si no cae al fallback. Cobertura medida: **157/158 tickers sin fallback**.
`combinedSignal` expone `vol_media_mov` y `vol_mm_ruedas`. La UI etiqueta
"Vol vs media 6m" vs "Vol vs media (anual)" según la fuente.

Efecto del cambio: correlación 0,972 con la constante vieja, pero **el signo cambia en 24,1%
de los casos**. El corte del tercil inferior pasó de −0,33 a −0,11.

## 2. `combinedSignal()` es recursiva — NO CORREGIDO ⚠️

En `App.jsx` (~línea 1590), para calcular `scoreTrend`:
```js
const prevSlice = data.slice(0, -7);
const prevSig = prevSlice.length>=60 ? combinedSignal(prevSlice, W, allData) : null;
```
Esa llamada **vuelve a hacer lo mismo**, recursivamente, hasta llegar a 60 barras. Con n
barras son ~n/7 llamadas anidadas, cada una recalculando `findStructuralLevels()` completo.

Costo medido:
- serie horaria (1.600 barras): ~35 ms/llamada
- serie diaria (2.400 barras): **4.000–10.000 ms/llamada**

Sospecha fuerte: **es la causa raíz real del bug "Quant Lab y Validación congelaban la UI"**,
que se parcheó con yields, límite de 45 modelos y backtest acotado a 750 días — o sea, se
trató el síntoma.

Fix probado en laboratorio (no commiteado): agregar un 4º parámetro `_noTrend` y pasarlo
en `true` en la llamada recursiva.
```js
function combinedSignal(data, W=7, allData=null, _noTrend=false) { …
  const prevSig = (!_noTrend && prevSlice.length>=60)
    ? combinedSignal(prevSlice, W, allData, true) : null;
```
**Verificado:** con y sin el corte, `rr` y `sig` dan idénticos (1,96 / VENTA FUERTE en AAPL a
2.400 barras). Baja el costo 10×. Sólo se pierde el `scoreTrend` de niveles profundos, que ya
no se usaba de esos niveles. **Arreglar esto primero: mejora la app entera, no sólo backtests.**

## 3. `dynParams` es sobreajuste y sigue activo ⚠️

`update_data.py` (línea ~855) prueba W ∈ {5,7,10,14} sobre la **serie completa** de cada
ticker y se queda con el de mejor win rate. Después `adaptiveW()` usa ese W en producción
(`App.jsx` línea ~4000: `optW ? optW : adaptiveW(tk.ticker, W)`).

Es el mismo error que:
- `adaptiveScoreAdj` — **neutralizado**, con este comentario en el código: *"modificaba el
  score usando resultados de simulaciones previas sobre los MISMOS datos. Eso es sobreajuste"*
- `TICKER_CONFIDENCE` — **eliminado**, por lo mismo

La diferencia es que éste **sigue activo y se regenera en cada corrida del workflow**. Elegir
el mejor de 4 W por ticker infla el win rate por construcción, y usa toda la historia
incluyendo el futuro de cualquier fecha evaluada. Además deriva `conf = (wr-0.5)*0.4` y
`p80adj = ±3` de ese win rate inflado.

**No medido todavía** si el W por ticker le gana al W global fuera de muestra. Es la
medición que decide si se neutraliza.

**Nota:** `update_data.py` NO regenera `VOL_MEDIA_ANUAL` (era hardcodeada a mano), así que
el hallazgo 1 no puede reaparecer por el workflow.

**Sospecha pendiente:** `RSI_TASAS_BASE` y las tasas por patrón de vela se generaron con la
misma metodología (constantes precalculadas sobre el histórico completo). No revisadas.

---

# HALLAZGOS DE MERCADO

## Método usado (mantenerlo)
Todo lo de abajo se midió así:
- Panel de 158 tickers × ~188 ruedas = ~31.700 observaciones, con `combinedSignal()` **real**
  (no aproximación), señal recalculada punto a punto sin fuga de información.
- **Exceso** = retorno menos el promedio de los papeles de la **misma fecha, misma moneda y
  mismo tercil de volatilidad (ATR%)**. El control por volatilidad es imprescindible: mató
  tres hipótesis que sin él parecían buenas.
- **t clusterizado por fecha**: las observaciones del mismo día no son independientes. Sin
  esto los t se inflan 3–4× y todo parece señal.
- **Holdout temporal 60/40** dentro de la propia búsqueda: se busca sólo en las fechas viejas
  y se reporta el resultado en las que el buscador nunca vio.
- Costos: `COSTO_MERVAL = 1,2%` y `COSTO_CEDEAR = 1,8%` round-trip.

## 4. `USD R/R > 1,97` — ÚNICO CANDIDATO SERIO ⭐

`rr` es el ratio riesgo/beneficio que ya calcula `findStructuralLevels()` (el fix que subió el
R/R mediano de 0,23 a ~1,8). **Resultó ser predictivo por sí solo.**

| horizonte | exceso (todo) | exceso OOS | t OOS |
|---|---|---|---|
| 1d | +0,020% | +0,024% | 0,70 |
| 5d | +0,246% | +0,247% | 2,74 |
| 10d | +0,447% | +0,493% | 3,65 |
| 20d | +1,001% | **+1,237%** | **5,69** |
| 30d | +1,050% | +1,453% | 5,29 |
| 45d | +1,235% | **+1,607%** | 4,02 |

Por qué es el mejor candidato del proyecto:
- **Monótono en el horizonte** — el efecto se acumula en vez de aparecer en un punto. Un
  artefacto de búsqueda no tiene por qué crecer ordenado en seis horizontes.
- **Rinde MÁS fuera de muestra que dentro** (+1,607% vs +1,235% a 45d). Lo contrario del
  sobreajuste.
- Pasa el control de volatilidad (+0,493% con control vs +0,543% sin él a 10d).
- Estable ante cambio de fecha de corte (probado con 15-ene y 06-feb).
- Consistencia: **8/10 meses positivos** (umbral del proyecto: 65%).
- A 45d roza el costo: +1,607% OOS contra 1,8% round-trip = −0,19%. La curva sigue subiendo.

**Limitación que impide declararlo válido:** corregido por solapamiento, a 45 días hay **3
ventanas independientes**, no 3.870 observaciones (`t_corregido = 0,57`). No es un resultado
negativo — es que la serie horaria dura un año y no entran más ventanas. **Se resuelve con la
corrida de 10 años.**

**Uso hoy, sin más validación:** como criterio de **ranking** (no paga comisión). Cuando el
sistema ya va a mostrar 20 oportunidades, ordenarlas por R/R en vez de por score pone arriba
las que rinden +0,49% más a 10 días.

## 5. `ARS bajo SMA200` — sirve hasta 20 días, se INVIERTE a 45

`mon=ARS & p200 < −2,7%`, exceso OOS con control de volatilidad:

| 5d | 10d | 20d | 30d | 45d |
|---|---|---|---|---|
| +0,450% | +0,514% | **+0,679%** | −0,500% | **−2,429%** (t=−5,93) |

No se apaga: **se da vuelta**. La ganancia hasta 20 días es un rebote que se devuelve entero.
Peligroso porque a 20d parecía neto positivo (+0,43%). Consistencia 10/11 meses positivos a
10d. `ARS RSI<46,6` es la misma idea con la misma forma (+0,668% a 20d, −0,930% a 45d).

Sirve como tilt de ranking a corto, **no** como señal de entrada.

## 6. El volumen NO predice más allá de 1 día — línea cerrada

Con el indicador corregido (hallazgo 1) se rebarrieron 4.716 combinaciones. **Ni una sola
condición que use `vdif`, `vpct`, `vol24` o `volDiv` sobrevivió fuera de muestra.**

El efecto a 1 día existe pero es de 0,10–0,26% contra costos de 1,2–1,8%:

| celda (móvil 6m) | exceso 1d | t |
|---|---|---|
| COMPRA + vol⁺ | +0,120% | 2,70 |
| COMPRA + vol⁻ | −0,257% | −2,73 |
| VENTA + vol⁺ (5d) | +0,286% | 2,24 |
| VENTA + vol⁻ | −0,062% | −1,61 |

**Patrón consistente en toda la sesión:** cada vez que apareció algo, el signo del volumen fue
el **opuesto** a la intuición inicial — lo que precede caídas es volumen *muerto*, no volumen
alto. Pasó tres veces.

## 7. La hipótesis original (señal × signo de volumen a 1 día) — DESCARTADA

Hipótesis probada: compra+vol⁺→baja, venta+vol⁺→sube, venta+vol⁻→baja.
Medida sobre 80 tickers × 381 fechas = 15.561 obs.

- **Acción por acción: 51 aciertos de signo sobre 110 = 46,4%.** Azar es 50%.
- Global: 2 de 3 celdas con el signo correcto, ninguna distinguible de cero (|t| < 1,96
  clusterizando por fecha). La de mayor magnitud (COMPRA + vol⁺, +0,30%) va **al revés**.
- El grupo de control se comportó igual o mejor que los 40 seleccionados → no hay nada
  específico de los extremos del ranking.

## 8. "COMPRA + volumen muerto → cae" — FALSO POSITIVO, era el sesgo

Llegó a t=−3,92 con drop-one, 17/22 tickers negativos y 12/15 meses consistentes. Parecía el
mejor hallazgo de la sesión. Al corregir `VOL_MEDIA_ANUAL` (hallazgo 1) **se dio vuelta**:

| | constante vieja | media móvil 6m |
|---|---|---|
| 1d | −0,165% (t=−2,31) | −0,166% (t=−1,76) |
| 5d | −0,222% | **+0,035%** |
| 10d | −0,317% | **+0,424%** |

Vivía del lookahead de la constante. **Dejar anotado: no volver a probarlo.**

## 9. `trend` discrimina las señales de venta — SIN CONFIRMAR, prometedor

Hasta ahora todas las `VENTA` se trataban igual sin mirar `trend`. Separadas, a 3 días:

**Universo completo (control), exceso vs. fecha+moneda+tercil de volatilidad:**

| trend | vol⁺ | vol⁻ |
|---|---|---|
| ALCISTA FUERTE | −0,077 (t=−0,36) | −0,099 (t=−0,43) |
| **ALCISTA** | +0,134 (t=0,48) | **−0,649 (t=−2,59)** ⭐ |
| LATERAL | −0,012 (t=−0,07) | −0,123 (t=−1,08) |
| BAJISTA | −0,023 (t=−0,11) | −0,047 (t=−0,23) |
| BAJISTA FUERTE | −0,168 (t=−1,02) | −0,105 (t=−0,66) |

Dos cosas para seguir:

**(a) `VENTA + ALCISTA + vol⁻`** → −0,649%, t=−2,59, n=594, WR 44,4%. Primera celda de la
familia que cruza 1,96, y **mantiene el signo en los top-20 de momentum** (−0,831%, WR 40,0%).
Lectura natural: tendencia alcista moderada + señal de venta + volumen que no acompaña = suba
sin nafta.

*Cautela:* mirar la columna vol⁻ completa — −0,099 / **−0,649** / −0,123 / −0,047 / −0,105.
Si `trend` modulara de verdad, habría un **gradiente**. En cambio una celda sobresale 6× sobre
sus vecinas, que están todas pegadas a −0,1%. Eso parece una celda con suerte más que un
mecanismo. Un efecto real por tendencia raramente saltea el nivel de al lado.

**(b) `ALCISTA FUERTE` se comporta al revés.** En los top-20 de momentum es la única con
exceso **positivo** en las dos columnas (+0,671 con n=33, y +0,621 con n=251, t=1,40): ahí la
señal de venta **falla**, el papel sigue subiendo. Si se confirma, es accionable como **veto y
sin costo**: no mostrar señales de venta en papeles en tendencia alcista fuerte que vienen
liderando el momentum. Hoy se muestran igual que cualquier otra.

## 10. La búsqueda a ciegas encuentra el período, no señales

Barrido de 3.411 combinaciones (pares de condiciones) con holdout: 69 pasaron t>2,5 en muestra,
**4 sobrevivieron fuera**. En el rebarrido con el volumen corregido (4.716 tests): 338 pasaron
en muestra, **0 sobrevivieron**.

Diagnóstico limpio del segundo barrido: las 4 mejores en muestra eran
`p50>1,40 & atrp<0,87` (+4,68%, t=6,21), `p200>2,81 & atrp<0,87` (+6,13%, t=5,89),
`rsi>53,40 & atrp<0,87` (+5,35%, t=5,69)… **todas con `atrp<0,87`**. Y el átomo solo:

`atrp<0,87` → en muestra +1,008% (t=2,47), fuera **+0,040% (t=0,93)**

O sea: el período de muestra fue uno donde los papeles de baja volatilidad con momentum
rindieron muy bien. Cualquier combinación que los seleccione daba t>5. **No es una señal —
es una descripción del período.** Mismo error que el rally de octubre 2025 ya anotado, con
otra cara.

---

## Arquitectura del sistema (sin cambios respecto al traspaso anterior)

### Score técnico — `combinedSignal()`
Momentum (ROC, MACD, medias) + Reversión (Bollinger, RSI), combinados según régimen.
Normalización con `tanh`. Ventanas 7/14/30/60. Niveles desde soportes/resistencias reales
(`findStructuralLevels`), R/R mediano ~1,8-1,9. Selección P80 por convicción absoluta
(`|score-50| >= umbral`), control "EXIGENCIA" en la UI.

### Alfa cross-sectional — `alpha.js`
Predice retorno **relativo** al universo.
- **USA (validado):** `rango(vol_shock) − rango(mom_1m)`. IC +0,127, IR 1,06, t=8,38.
  OOS t=2,58–3,51. `rankearUniverso()`.
- **Merval (⚗ preliminar):** 20 papeles más líquidos, `−amihud − skew_ret`. IC +0,236 OOS,
  t=7,86, pero 20 tickers × 50 fechas. `rankearUniversoMerval()`.
- Horizonte óptimo 30–45 días. A 7/14d el alfa (~1%) no cubre el 1,8% de comisión.

### Validación — `quant.js` + `quant2.js` (tab 🔬)
Triple-barrera, K-fold purgado, Platt. Deflated Sharpe, PBO (CSCV), test de consistencia
temporal (el más importante), meta-labeling.

### Calidad fundamental — filtro de riesgo, NO señal
Deliberadamente no alimenta score ni alfa (sería sesgo de anticipación: son datos de HOY).
Sólo excluye frágiles. Factores QMJ.

### Tracker (tab 📌)
Congela lo que el sistema decía al marcar. Única evidencia que ninguna validación
retrospectiva reemplaza. `localStorage('fxca16_tracker')`. Guarda `alphaPreliminar`.

---

## Estado de los datos
```
Histórico horario (~1 año):    158 tickers, ~250.000 barras
Histórico diario (10 años):    158 tickers, mediana 2.531 días  ✅
Fundamentales:                 159 tickers, 34 frágiles         ✅
Calendario de earnings:        0 tickers  ⚠️ VACÍO
```
`descargar_earnings()` devuelve vacío. Probablemente cambió la API de yfinance
(`get_earnings_dates`). Revisar el log del workflow, sección "📅 Earnings". El fallback
hardcodeado en `getUpcomingEvents()` sigue funcionando.

---

## PRÓXIMOS PASOS (orden sugerido)

1. **Arreglar la recursión de `combinedSignal`** (hallazgo 2). Mejora la app entera y abarata
   todo lo demás. El fix está probado y verificado.
2. **Medir `dynParams`** (hallazgo 3): ¿el W por ticker le gana al W global fuera de muestra?
   Si no, neutralizarlo como se hizo con `adaptiveScoreAdj`.
3. **Validación de 10 años sobre diarias** para R/R (hallazgo 4) y `trend` (hallazgo 9).
   Es lo que decide si R/R entra al sistema o va a descartados. Salvedad honesta:
   `combinedSignal` sobre diarias no da el mismo número que sobre horarias (el lookback de
   estructura pasa de 1.680 horas a 1.680 días), así que testea si **el concepto** se sostiene,
   no reproduce el número exacto de producción. Con el fix del punto 1, viable en ~1-2 h.
4. **Correr el Tracker 3 meses** — evidencia hacia adelante, insustituible.
5. Arreglar el calendario de earnings.
6. Revalidar el alfa del Merval con más historia.
7. Vista de cartera agregada (riesgo total, correlación entre posiciones).
8. Dólar CCL/MEP — decisión CEDEAR vs papel local.
9. Dividendos — no se descuentan en ningún cálculo.

---

## Filosofía (mantener)

Cada mejora se valida **empíricamente contra los datos reales**, no por intuición. Lo que no
sobrevive fuera de muestra se descarta o se marca explícitamente como preliminar.

Esta sesión agrega tres reglas concretas, aprendidas a los golpes:

1. **Controlar por volatilidad siempre.** Sin ese control, tres hipótesis distintas parecieron
   buenas y las tres eran exposición a beta.
2. **Clusterizar por fecha y contar ventanas independientes.** 15.000 observaciones de 380
   fechas son 380 datos, no 15.000. A 45 días son 3.
3. **Buscar sólo en el 60% viejo y reportar el 40% que el buscador no vio.** No cuesta tiempo
   extra y es lo único que separa señal de descripción del período. De 28 candidatos con
   t>2,5 en muestra, sobrevivieron 4 en un barrido y 0 en el otro.

Y una advertencia: **las constantes precalculadas del proyecto no son confiables.**
`VOL_MEDIA_ANUAL` resultó estar rota, `adaptiveScoreAdj` y `TICKER_CONFIDENCE` ya se habían
eliminado por sobreajuste, y `dynParams` tiene el mismo defecto y sigue activo. Antes de
confiar en cualquier constante embebida, verificar que reproduce la cantidad que dice medir.
