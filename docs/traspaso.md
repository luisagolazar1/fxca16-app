# FXCA16 — Resumen de traspaso

> Actualizado: 2026-08-26. Reemplaza la versión anterior.
> Commits de la sesión: `4cb3362` (vol media móvil), `c497c04` (recursión + noticias),
> `610452e` (persistencia direccional), `c6d2645` (norma por activo), `c09ea95`
> (neutralizar adaptiveW), y los de registro.

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

# NORMA DE ESTIMACIÓN DEL PROYECTO (nueva, validada)

> **Estimar por activo con encogimiento hacia el global. Ventana dinámica de 400 ruedas
> que termina en la rueda anterior. Nunca en pool global puro.**

Surgió de una observación del usuario: *"un error que estamos teniendo es evaluar de manera
global y no por casos particulares"*. Se validó empíricamente en vez de adoptarla por
criterio.

**EXPERIMENTO** (279.735 obs diarias, 156 tickers, 10 años, ~750 fechas fuera de muestra,
modelo LMSW como banco de pruebas). Spread long-short a 1 día en ARS:

| método | spread | t | IR | OOS spread | OOS t |
|---|---|---|---|---|---|
| Global (pool único) | +0,081% | 2,65 | 1,00 | +0,257% | 4,61 |
| **Por activo** | **+0,240%** | **9,75** | **3,60** | +0,346% | **7,64** |
| Por activo + encogimiento | +0,225% | 8,39 | 3,11 | **+0,367%** | 7,36 |

**Por activo triplica al global.** Evaluar en pool estaba tirando dos tercios de la señal.

**Por qué encogimiento y no por activo puro:** el peso propio que el dato asigna es 0,29 con
250 ruedas y **0,48 con 400**. Es decir, ni con 400 ruedas la estimación individual merece más
de la mitad del peso. Sin encogimiento, "por activo" degenera en `TICKER_CONFIDENCE`.

**Por qué 400 ruedas:** 250/300/400 dan resultados casi idénticos (t=9,75 / 10,36 / 9,84), así
que el hallazgo no depende de elegir bien el número. Se toma 400 porque sube el peso propio de
0,29 a 0,48 — le da más voz al activo, que es el objetivo.

**La norma se autorregula.** Aplicada a patrones de vela, el peso propio dio 0,647 para
"3 Cuervos" y 0,613 para "3 Velas Alcistas" (que sí tienen heterogeneidad real), pero
**exactamente 0,000** para Doji, Engulfing Alcista, Estrella Fugaz y Marubozu, donde τ²=0 y
toda la varianza entre activos es ruido de muestreo. Y aplicada al W de `dynParams`, el peso
correcto es 0 — que es justo lo que se midió (ver hallazgo 11).

Esa es la diferencia con los sobreajustes históricos: **el peso lo decide el dato, no el
programador.**

---

# INDICADOR NUEVO: PERSISTENCIA DIRECCIONAL (LMSW)

Basado en Llorente, Michaely, Saar y Wang (*Review of Financial Studies*, 2002): los retornos
generados por reparto de riesgo revierten, los generados por trading informado continúan. El
coeficiente C2 mide de qué lado está el papel **hoy**, y cambia con el tiempo dentro del mismo
activo (verificado: GGAL C2=−0,091 en 2024, +0,019 en 2025).

**Especificación** (`persistenciaDireccional(ticker, hastaFecha)` en App.jsx):

```
z(r[t+1]) = c0 + c1·z(r[t]) + c2·z(r[t])·z(V[t])
V = log(volumen) − media móvil 200 ruedas   (detrendado, adaptativo)
ventana 400 ruedas, r y V estandarizados con la propia ventana,
coeficientes encogidos hacia el promedio del universo de esa fecha
```

**VALIDACIÓN** (303.096 obs, 156 tickers, 10 años, OOS por construcción):

| | spread 1d | t |
|---|---|---|
| **ARS** | **+0,248%** | **10,29** |
| USD | −0,002% | −0,16 |

- **Drop-one: 156/156** corridas siguen con t>2,58 (rango 3,41–3,95)
- **8 de 9 años positivos**
- La normalización fue decisiva: mejoró ARS de t=6,21 a 10,29 y hundió USD de 0,48 a −0,16

**USD no funciona y se probaron 6 cortes** (iliquidez de Amihud, volumen en dólares, ETF vs
acción, magnitud del movimiento, descomposición C1/C2, versión normalizada). Ninguno pasa de
|t|=1,05. El diagnóstico: C1 medio en USD es −0,006 (cero) contra +0,070 en Merval — no hay
autocorrelación diaria que capturar. Badge punteado en USD.

⚠ **NO es señal de entrada:** +0,248% diario contra 1,2% de comisión Merval. Es criterio de
**ranking**, que no paga comisión.

⚠ **MATIZ IMPORTANTE:** el efecto **no es de volumen**. El término C1 solo da t=10,38 en ARS
(más que el modelo completo) y el término del volumen C2 aporta t=1,75. Lo que se descubrió es
que **Merval tiene momentum diario persistente** y el marco LMSW lo capta de rebote. Por eso se
llama "persistencia" y no "volumen".

**En la UI:** badge en Detalle y en Replay. Sólido en ARS, punteado con tooltip "sin validar"
en USD. En Replay se corta por fecha para no mirar al futuro.

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

## 4. `R/R estructural` — CERRADO: efecto real pero débil, no pasa a aplicada ⭐→⏳

> **Actualizado 2026-08-25 (misma sesión, corrida de cierre).** La medición original sobre 1
> año de horaria (abajo, tachada) estaba inflada por escasez de historia. Revalidado sobre
> 10 años de diarias — con la recursión de `combinedSignal` ya cortada, la corrida bajó de
> ~15h estimadas a unos 5 minutos.

`rr` es el ratio riesgo/beneficio que ya calcula `findStructuralLevels()` (el fix que subió el
R/R mediano de 0,23 a ~1,8). Sí tiene relación con el retorno — pero mucho más chica de lo
que parecía.

**Resultado final (103 tickers USD, 10 años, 42.123 obs), por quintil de R/R a 20d:**

| quintil | rango R/R | n | exceso 20d | t | WR |
|---|---|---|---|---|---|
| Q1 (peor) | 1,07–1,80 | 8.050 | −0,215% | −2,27 | 56,7% |
| Q2 | 1,80–1,89 | 8.528 | −0,177% | −1,75 | 57,1% |
| Q3 | 1,89–2,00 | 8.341 | +0,016% | 0,14 | 57,5% |
| Q4+Q5 | 2,00–5,00 | 17.204 | **+0,182%** | **2,54** | 57,1% |

**El gradiente es monótono y real** (Q1 a Q4+Q5: −0,22 → −0,18 → +0,02 → +0,18), la misma
firma cualitativa que se vio con 1 año. Pero la magnitud es **3 a 6 veces menor**
(+0,182% contra el +1,237% que había dado la medición inicial a 20d).

**Por qué se infló la medición original:** el umbral `>1,97` no era un extremo — capturaba
**43% del universo** (mediana real de R/R: 1,93), y **40% del panel tiene RR=2,00 exacto**,
que es un **tope de diseño** del cálculo de niveles, no un valor que emerja naturalmente del
mercado. En un año particular ese corte agarraba algo específico de ese régimen; en 10 años
agarra casi la mitad del universo en cualquier momento.

**La limitación de ventanas independientes, resuelta:** con 10 años hay 84 ventanas
independientes a 20 días (contra 3 que había con 1 año). Corregido por eso, el t cae de 5,69
a **1,25** — no alcanza para declarar el efecto sólido incluso con toda la historia
disponible.

**Veredicto: no pasa a "aplicada".** Sigue en observación. Único uso justificado: **tilt de
ranking** dentro de las oportunidades ya filtradas (ordenar por R/R como criterio secundario,
no como filtro) — ahí no compite contra costos y la dirección es correcta, aunque modesta.

<details>
<summary>Medición original sobre 1 año de horaria (superada, se deja por trazabilidad)</summary>

| horizonte | exceso (todo) | exceso OOS | t OOS |
|---|---|---|---|
| 1d | +0,020% | +0,024% | 0,70 |
| 5d | +0,246% | +0,247% | 2,74 |
| 10d | +0,447% | +0,493% | 3,65 |
| 20d | +1,001% | +1,237% | 5,69 |
| 30d | +1,050% | +1,453% | 5,29 |
| 45d | +1,235% | +1,607% | 4,02 |

Con 1 año de datos, monótono en el horizonte y rendía más fuera de muestra que dentro — la
firma que hace confiar en un hallazgo. Pero corregido por solapamiento, a 45 días había sólo
**3 ventanas independientes**, insuficiente para declarar significancia. Esa era la limitación
que la corrida de 10 años debía resolver, y la resolvió: no confirmando la magnitud original.

</details>

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

## 9. `trend` en señales de venta — CERRADO con 10 años: parcial, y el signo era al revés

Se cierra la versión preliminar que había con 1 año de horarias.

**156 tickers × 10 años, 34.617 señales de venta.** Exceso a 3 días, control por fecha,
moneda y tercil de volatilidad:

| trend | n | exceso 3d | t | OOS exc | OOS t |
|---|---|---|---|---|---|
| **ALCISTA FUERTE** | 3.346 | **−0,287%** | −1,99 | **−0,557%** | **−3,65** |
| ALCISTA | 1.134 | +0,205% | 1,20 | +0,093% | 0,46 |
| LATERAL | 16.374 | +0,097% | 1,01 | −0,005% | −0,04 |
| BAJISTA | 7.104 | −0,044% | −0,49 | −0,018% | −0,12 |
| BAJISTA FUERTE | 6.659 | +0,120% | 1,35 | +0,013% | 0,09 |

**La hipótesis original no se sostiene:** no hay gradiente ordenado por tendencia. El test
entre extremos (ALCISTA FUERTE vs BAJISTA FUERTE) da t=−1,62 a 3 días y t=−0,75 a 20 días.

Lo que sí queda es **un caso puntual**: VENTA sobre papeles en tendencia alcista fuerte,
con el efecto reforzándose fuera de muestra (t=−3,65).

⚠ **CORRECCIÓN DE INTERPRETACIÓN.** Durante la sesión se dijo que ahí "la señal de venta
falla". Es al revés. Exceso negativo en una VENTA significa que el papel rinde **peor** que
sus pares, o sea que la venta **acierta**. Verificado: retorno crudo +0,159%, exceso
−0,155%, baja el 45,4% de las veces — el papel sube en absoluto pero menos que sus pares.

Consecuencia: **no es candidato a veto.** Es el subgrupo donde la venta funciona mejor. Si se
usara, sería para priorizar, no para filtrar. Y sólo sirve en términos relativos (ranking),
porque el retorno crudo es positivo.

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
es una descripción del período.**

## 11. `dynParams` / `adaptiveW` — NEUTRALIZADO, y anulaba el selector de W ⚠

`update_data.py` probaba W ∈ {5,7,10,14} sobre la serie completa de cada ticker y se quedaba
con el de mejor win rate. `adaptiveW()` lo aplicaba en producción.

**Medido** (157 tickers, 10 años, split 60/40, replicando `backtest_w()` exacto):

| | win rate fuera de muestra |
|---|---|
| W elegido por activo | 47,48% |
| **W global fijo = 5** | **47,71%** |

**Diferencia −0,23pp con t=−2,62: no es que no ayude, perjudica.** El W propio le gana al
global en **8 de 157 activos (5%)**, exactamente lo esperable por azar. El orden de los W es
idéntico dentro y fuera de muestra (5 > 7 > 10 > 14): W=5 es mejor para todos.

**BUG ADICIONAL, más grave.** Los 158 tickers tenían `sims >= 5`, así que `adaptiveW` siempre
devolvía el W aprendido y **nunca llegaba al `globalW`**. El selector de ventana de la UI
(7/14/30/60) estaba siendo **ignorado por completo** en Oportunidades: 138 tickers calculaban
con W=5, 13 con W=7, 5 con W=10, 2 con W=14, sin importar qué eligiera el usuario.

Con la neutralización el selector vuelve a funcionar. Verificado: GGAL pasa de VENTA (51) en
W=7 a COMPRA (63) en W=30 — antes era imposible.

⚠ Nota aparte: el win rate ronda 47,5% en **todos** los W. La estrategia que `backtest_w()`
usa para elegir (cruce SMA20/50, stop 1,5×ATR, TP 2,5×ATR) pierde más veces de las que gana.

## 12. VETO DE FIBONACCI — el candidato más sólido sin implementar ⭐

`COMPRA + trend=ALCISTA + distancia al nivel Fib más cercano > 3,92% + ese nivel clasificado
'soporte' débil`. Es decir: el sistema dice comprar, pero el precio está en tierra de nadie,
sin ningún nivel técnico que lo sostenga.

**Exceso a 20 días** (156 tickers × 10 años, control por fecha/moneda/volatilidad):

| | n | exceso | t |
|---|---|---|---|
| Total | 424 | **−2,632%** | **−3,80** |
| Fuera de muestra | 110 | **−3,066%** | −2,50 |
| ARS | 190 | −3,461% | −3,46 |
| USD | 234 | −1,930% | −2,10 |

- Win rate 41,5%
- **Drop-one:** sin el ticker más favorable (INTC) sigue en t=−2,99
- **Consistencia: 8 de 8 años con exceso negativo**
- Más fuerte fuera de muestra que dentro
- **Funciona en los dos mercados** — a diferencia de la persistencia direccional, que es sólo Merval

**Cautela:** 424 observaciones sobre 93 tickers, y sólo 45/93 tienen exceso negativo. Se apoya
en pocos casos por activo.

**Por qué el umbral de evidencia es más bajo acá:** como *veto* no paga comisión — filtrar una
compra es gratis. No hay que superar el 1,2-1,8% de costo, sólo hay que no equivocarse.

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

1. **Probar el selector de W en Oportunidades.** Es el cambio más visible de la sesión:
   hasta ahora no hacía nada (ver hallazgo 11). Verificar que el comportamiento por ventana
   tiene sentido antes de confiar en las señales.
2. **Implementar el veto de Fibonacci** (hallazgo 12). Es el candidato más sólido que quedó
   sin aplicar: 8/8 años negativos, aguanta drop-one, funciona en ARS y USD, y como veto no
   paga comisión.
3. **Correr el Tracker 3 meses** — evidencia hacia adelante, insustituible.
4. Arreglar el calendario de earnings (`descargar_earnings()` devuelve vacío).
5. Revalidar el alfa del Merval con más historia.
6. Vista de cartera agregada (riesgo total, correlación entre posiciones).
7. Dólar CCL/MEP — decisión CEDEAR vs papel local.
8. Dividendos — no se descuentan en ningún cálculo.

### Ya cerrado en esta sesión
- ~~Recursión de `combinedSignal`~~ → arreglado, 35x más rápido
- ~~Validación de 10 años de R/R~~ → efecto real pero débil, queda en observación
- ~~Medir `dynParams`~~ → neutralizado, perjudicaba (t=-2.62)
- ~~Fibonacci y `trend` en ventas~~ → ambos cerrados con 10 años
- ~~Patrones de vela por activo~~ → medido, no justifica cambiar la tabla

## Filosofía (mantener)

Cada mejora se valida **empíricamente contra los datos reales**, no por intuición. Lo que no
sobrevive fuera de muestra se descarta o se marca explícitamente como preliminar.

Reglas concretas, aprendidas a los golpes:

1. **Controlar por volatilidad siempre.** Sin ese control, tres hipótesis distintas parecieron
   buenas y las tres eran exposición a beta.
2. **Clusterizar por fecha y contar ventanas independientes.** 15.000 observaciones de 380
   fechas son 380 datos, no 15.000. A 45 días son 3.
3. **Buscar sólo en el 60% viejo y reportar el 40% que el buscador no vio.** De 28 candidatos
   con t>2,5 en muestra, sobrevivieron 4 en un barrido y 0 en el otro.
4. **Estimar por activo con encogimiento** (ver norma arriba). El peso lo decide el dato.
5. **Un año de datos no alcanza para declarar nada a horizontes largos.** R/R parecía t=5,69 a
   20 días con 1 año; con 10 años el efecto es 3-6x más chico. "Sobrevive holdout con 1 año" y
   "sobrevive con historia suficiente" son dos chequeos distintos y hacen falta los dos.

**Las constantes precalculadas del proyecto no son confiables.** Historial completo:

| constante | destino |
|---|---|
| `VOL_MEDIA_ANUAL` | rota (10x más dispersa que la cantidad que medía) → reemplazada |
| `adaptiveScoreAdj` | sobreajuste → neutralizada |
| `TICKER_CONFIDENCE` | sobreajuste → eliminada |
| `dynParams` / `adaptiveW` | perjudicaba (t=−2,62) → neutralizada |
| `VELAS_TASAS_BASE` | medida con la norma nueva → se deja, es honesta |
| `RSI_TASAS_BASE` | **sin revisar** |

Antes de confiar en cualquier constante embebida, **verificar que reproduce la cantidad que
dice medir**. Ese chequeo simple fue el que destapó `VOL_MEDIA_ANUAL`.

**Y un patrón que se repitió toda la sesión:** cada vez que apareció algo prometedor en el
volumen, el signo terminó siendo el **opuesto** a la intuición. Pasó tres veces. También pasó
con `trend` en ventas, donde la interpretación del signo estuvo invertida hasta que se verificó
con 10 años. Verificar la dirección del efecto antes de construir sobre él.
