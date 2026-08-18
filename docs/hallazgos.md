# Hallazgos — hipótesis probadas fuera de la app

Registro de patrones investigados manualmente (fuera del pipeline normal
de `quant.js`/`quant2.js`) para no re-testear lo mismo más adelante
pensando que es una idea nueva. Solo entran acá hipótesis con
metodología y resultado explícito — no intuiciones sueltas.

---

## 2026-08-03 — "MACD subiendo + volumen bajo predice rally a 3-5 días"

**Origen:** BA, GLOB, ORCL, AAL, BABA subieron juntas la semana del 27/7
al 31/7. Un vistazo a los datos de esos días sugería que las 5 tenían
el MACD virando al alza mientras el precio todavía estaba plano/cayendo,
con volumen por debajo del promedio — el patrón clásico de "divergencia
alcista silenciosa antes del rebote".

**Primer test (n chico):** 9 rallies detectados en esas 5 acciones a lo
largo de julio (retorno 3D > 4%). Solo 1 de 9 (11%) cumplía el patrón
completo (MACD subiendo 3 días + volumen < 1x). Además, de los 9
rallies, solo UNO involucró a las 5 acciones simultáneamente — la
semana de la Fed (29/7). El resto fueron rallies aislados de 1-2
tickers en fechas distintas, sin sincronía.

**Contexto macro encontrado:** el 29/7 la Fed mantuvo tasas sin cambios
con votación dividida 9-3 (hawkish), el mercado cayó ese día, y hubo
rebote fuerte jueves 30/7 y viernes 31/7 (Nasdaq +2.8%, liderado por
MSFT +16% en resultados). Que 5 acciones de sectores sin relación
(aeroespacial, IT, cloud, aerolíneas, e-commerce chino) subieran juntas
es la firma típica de un movimiento de mercado amplio (beta), no de una
señal específica por acción.

**Test ampliado (n grande, todo el universo, julio 2026):**

| Variante | n señal / sin señal | Ret. CON señal | Ret. SIN señal | Win rate CON | t-stat |
|---|---|---|---|---|---|
| MACD↑3d + Vol<1x, fwd 3d | 605 / 2449 | +0.18% | +0.09% | 48.8% | 0.51 |
| ídem, fwd 5d | 576 / 2317 | +0.43% | **+1.17%** | 51.9% | -0.67 |
| + RSI<50, fwd 3d | 295 / 2914 | +0.33% | **+1.00%** | 46.8% | -0.75 |
| + RSI<50, fwd 5d | 274 / 2619 | +0.71% | **+1.06%** | 51.8% | -0.34 |
| MACD↑**2d** (sin vol), fwd 3d | 1194 / 2015 | -0.12% | **+1.57%** | 43.8% | -1.35 |
| MACD↑2d + Vol<1x, fwd 3d | 841 / 2368 | +0.06% | +1.25% | 47.1% | -1.12 |
| MACD↑2d + Vol<1x, fwd 5d | 777 / 2116 | +0.15% | +1.35% | 49.2% | -1.00 |
| MACD↑2d + Vol<1x + RSI<50, fwd 3d | 378 / 2831 | +0.26% | +1.03% | 47.9% | -0.84 |
| MACD↑2d + Vol<1x + RSI<50, fwd 5d | 350 / 2543 | +0.57% | +1.09% | 52.0% | -0.50 |

**Conclusión: DESCARTADA.** Ninguna variante alcanza significancia
(|t| < 1.96 en las 9 pruebas; el máximo fue 1.35). En 7 de 9 variantes
el grupo SIN la señal rindió *mejor* que el grupo CON la señal —
dirección opuesta a la hipótesis. El win rate ronda 44-52% en todas,
indistinguible de azar.

**Por qué no sorprende, visto contra la validación ya existente:** el
ablation de indicadores (tab Validación) ya tenía a `macdN` cerca del
último lugar en importancia (delta AUC -14.33, de los más bajos de la
lista). Este test es consistente con eso, no lo contradice.

**Regla para el futuro:** si alguien (yo incluido) vuelve a notar "estas
acciones subieron juntas, ¿había una señal técnica común?" — la
respuesta por default, salvo evidencia nueva y grande, es que
probablemente fue un catalizador de mercado compartido (Fed, earnings
season, dato macro), no una señal técnica reusable. Antes de programar
cualquier regla basada en esto, repetir este mismo test (universo
completo, ventana de fechas distinta, idealmente varios meses) y exigir
significancia real, no solo "funcionó en 5 casos".

---

## 2026-08-03 — "El retorno de la semana siguiente a una reunión FOMC es distinto al de una semana normal"

**Origen:** derivado del hallazgo anterior. Si la Fed fue el catalizador
real de la suba de fines de julio, ¿el patrón "semana post-FOMC ≠
semana normal" se repite en las demás reuniones de 2026?

**Método:** las 4 reuniones FOMC de 2026 ya completas al momento del
test (28/1, 18/3, 29/4, 17/6 — la del 29/7 quedó fuera por no tener
todavía 7 días completos posteriores), retorno del universo completo
(158 activos) 7 días antes y 7 días después de cada una, comparado
contra un baseline de ventanas de 7 días "normales" (lejos de cualquier
fecha FOMC) a lo largo de 2026.

**Resultado crudo:**

| Ventana | Retorno medio | Win rate | Volatilidad vs. normal |
|---|---|---|---|
| 7d antes de la Fed | +0.15% | 45.7% | 0.91x |
| 7d después de la Fed | -1.08% | 38.6% | 1.07x |
| Baseline (normal) | +0.52% | 48.9% | 1.00x |

t-stat "después vs. baseline" = -4.66 (aparentemente muy significativo).

**Por qué se descarta igual, a pesar del t-stat alto:** el n nominal
(632-2319 observaciones) está inflado. Cada fecha FOMC aporta 158
observaciones (una por ticker), pero esas 158 no son independientes —
comparten el mismo shock macro del día. La muestra *efectiva* real son
**4 eventos**, no cientos de observaciones. Con 4 eventos ningún test
estadístico da margen para confiar en el resultado, sin importar qué
tan grande se vea el t-stat calculado ingenuamente. Es el mismo
problema que corrige el panel "Unicidad de muestras" del tab de
Validación para el backtest de cartera (ahí 3151 observaciones nominales
eran ~245 efectivas por solapamiento temporal — acá el problema es peor).

**Dato honesto que sí queda:** de las 4 reuniones completas de 2026,
3 tuvieron retorno negativo o chato la semana después (28/1: -2.65%,
18/3: -1.30%, 17/6: -0.99%) y solo 1 fue positiva (29/4: +0.61%,
modesto). La suba fuerte de fines de julio fue la excepción del año, no
la regla — ni dentro del propio 2026.

**Veredicto: sin conclusión — no se puede afirmar ni descartar la
relación con esta muestra.** No es un "descartada" tan limpio como el
de MACD (ahí n grande y resultado claramente nulo); acá directamente no
hay muestra suficiente para decir nada. No se implementa ninguna regla
basada en esto.

**Para hacerlo bien:** hay 10 años de historia diaria embebida
(`CSV_DATA_DAILY_RAW`, ~2500 barras/ticker) — con el calendario FOMC
real de esos 10 años (~80 reuniones) la muestra efectiva pasaría de 4 a
80 eventos, ahí sí sería un test con poder estadístico real. Pendiente,
no se hizo por decisión explícita (bajo prioridad por ahora).


---

## 2026-08-03 — "Reversión: caído >8% del máximo de 20 días + RSI<45"

**Origen:** buscando qué precedía a las subas fuertes de las 20 acciones
que más subieron en 3 meses. A diferencia de la hipótesis de MACD, esta
sí replicó en el universo completo en el primer test: los días previos a
subas de +8% en 7 días mostraban precio *debajo* del máximo de 20 días,
RSI bajo y semana previa negativa — firma de reversión, no de momentum.

**Primer resultado (prometedor):** universo completo, mayo-ago 2026,
+1.70% a 7 días vs +0.32% del resto, t=7.27, win rate 60%. Además la
condición discriminaba dirección (los días previos a *caídas* de -8%
tenían el perfil opuesto: RSI 54, semana previa +3.09%), así que no era
solo "volatilidad".

**Batería de validación:**

| Test | Resultado |
|---|---|
| Ventana original (3 meses) | +1.38pp, t=7.27 |
| Extendido a 10 años (65.047 señales) | +0.32pp, t=6.90 — el edge se achica 4x |
| Neto de costos (H=7) | **-0.27% por operación** |
| H=60 para amortizar costo, retorno absoluto | +12.33% (engañoso — ver abajo) |
| H=60, exceso sobre el mercado mismo día | +2.59% |
| H=60, **contra papeles de volatilidad similar** | **+0.24%, t=0.26 — no significativo** |
| Neto de costos, ajustado por volatilidad | **-1.30%/op ≈ -5.47% anual** |
| Consistencia anual (exceso ajustado) | 6/11 años (55%) — umbral >65% |

**Conclusión: DESCARTADA.** Tres correcciones sucesivas fueron
desarmando el hallazgo:

1. **Retorno absoluto vs exceso:** a 60 días el "+12.33%" era casi todo
   mercado subiendo (el baseline sin señal daba +8.42%). El exceso real
   era 2.59%.
2. **Ventanas solapadas:** con retornos forward de 60 días calculados a
   diario, el solapamiento es 59/60. El t=20.88 nominal cae a 2.70 con
   la muestra efectiva (n 63.385 → 1.056).
3. **Control de volatilidad (decisivo):** la señal selecciona papeles
   1.36x más volátiles que el promedio. Comparando contra papeles de
   volatilidad *similar* el mismo día, el exceso cae de 2.59% a 0.24%
   y el t a 0.26. El desglose por quintil no es monótono (Q0 +2.47%,
   Q2 -1.05%, Q4 +0.99%) — incoherente, ruido.

**Lo que era en realidad:** exposición a beta/volatilidad, no alfa.
Comprar papeles golpeados y volátiles en un mercado que subió 10 años
paga — pero paga por el riesgo tomado, no por capacidad predictiva. Es
el mismo diagnóstico que el propio tab de Validación ya había hecho
sobre el score técnico ("amplificación de beta en un rally, no
capacidad predictiva").

**Lección metodológica reutilizable:** cuando una señal seleccione
activos con volatilidad sistemáticamente distinta al promedio, el
control correcto no es contra "el resto del universo" sino contra
activos de volatilidad comparable en la misma fecha. Sin ese control,
casi cualquier filtro de "papeles golpeados" va a parecer que funciona
en un mercado alcista.

---

## 2026-08-03 — Tasas base de RSI y de patrones de vela (agregados como CONTEXTO, no como señal)

A diferencia de las tres entradas anteriores, estos dos no se descartan:
se incorporaron a la app como estadística descriptiva, con la advertencia
explícita de que ninguno alcanza como regla de entrada.

### RSI por bandas (10 años, 363.746 obs)

La relación es una **U, no una rampa**: los dos extremos (RSI<30 y
RSI>70) preceden movimientos más grandes en *ambas* direcciones, y el
medio (45-55) es la zona más quieta. Eso es volatilidad, no dirección.

| Banda | P(+4% en 1-4d) | P(-4%) | fwd 4d |
|---|---|---|---|
| 0-30 | 33.0% | 25.0% | +1.18% |
| 45-55 | ~21% | ~19% | +0.35% |
| 70+ | 23.1% | 17.1% | +1.01% |

- Pasa el test de consistencia: RSI bajo le gana a RSI alto en **81 de
  118 meses (69%)**, sobre el umbral de 65%. Es lo primero de toda esta
  investigación que lo pasa.
- Pero el spread medio es **0.82 pp**, contra 1.2-1.8% de costo de
  operar. No alcanza como regla autónoma.
- Últimos 12 meses flojos: 5 de 12 (42%).
- **Julio 2026 fue atípico** (puesto 21 de 118, top 18%): ahí el patrón
  se veía monótono y fuerte (RSI<30 → +3.21%, RSI>70 → -1.30%), lo
  opuesto a la forma en U de la historia larga. Codificar una regla
  mirando solo julio habría dado una señal invertida para RSI>70.

### Patrones de vela (mismos datos)

De 15 patrones medidos, con corrección de Bonferroni (15 comparaciones)
más control de volatilidad:

- **Solo 2 sobreviven**: Marubozu Alcista (exceso +0.319 pp, t=2.80) y
  3 Cuervos (+0.166 pp, t=3.40).
- **Solo 1 pasa además consistencia mensual**: 3 Cuervos (70%).
  Marubozu Alcista falla con 60%.
- **Las etiquetas tradicionales no se sostienen.** "3 Cuervos" es un
  patrón *bajista* de manual y predice retornos *positivos*. "Martillo"
  (la reversión alcista clásica) es el **peor** de los 15 (-0.153 pp).
  "3 Velas Alcistas" y "Engulfing Bajista" dan esencialmente cero.
- Aun el mejor caso mueve 0.2-0.3 pp, **muy por debajo del costo**.

**Lo que se implementó:** dos paneles en la sección "⊕ AGREGADOS
RECIENTES" al final del tab Detalle, mostrando la tasa base medida de la
banda de RSI y del patrón de vela actual, cada uno con su advertencia.
La idea es que el usuario vea *dónde está parado según la historia* sin
que eso se lea como un pronóstico. También se agregó detección de
"3 Cuervos" a detectCandlePattern(), con un `desc` que refleja lo medido
en vez de repetir la interpretación clásica.

---

## 2026-08-03 — El alfa cross-sectional NO sobrevive fuera de su ventana de desarrollo

**Contexto:** el documento de traspaso describía a `alpha.js` como "lo
único que sobrevivió validación fuera de muestra en toda la sesión",
con IC +0.127, IR 1.06, t=8.38, validado con split temporal 60/40.

**Test:** reproducir la fórmula documentada
(`rango(vol_shock) − rango(mom_1m)`, con suavizado de 10 días) y
evaluarla sobre los **10 años de serie diaria** — datos que el modelo
nunca vio, porque se desarrolló sobre la serie horaria de ~1 año.

**Resultado por período** (H=30d; "mono" = correlación entre número de
quintil y exceso; debería ser cercana a +1 si el ranking funciona):

| Período | Monotonía | t(Q5) |
|---|---|---|
| 2016-2019 | -0.17 | 0.16 |
| 2020-2022 | **-0.94** | -0.11 |
| 2023-2024 | -0.48 | 0.01 |
| 2025 | -0.78 | -0.61 |
| **2026** | **+0.76** | 0.66 |
| **Todo 2016-2026** | **-0.66** | **0.00** |

**Conclusión: el alfa solo "funciona" en 2026 — el período sobre el que
se construyó.** En el agregado de 10 años la monotonía es -0.66 (ranking
invertido) y el t de Q5 es exactamente 0.00. Es el patrón clásico de
sobreajuste a la ventana de desarrollo.

**Lo que sí es cierto y vale rescatar:** el alfa efectivamente dispara
*antes* que el score técnico. Q5 compra papeles que vienen -6.9% en 20
días, mientras el score técnico marca COMPRA FUERTE después de +13.7%.
El mecanismo es el correcto — el problema es que el ranking no predice.

**Salvedad:** la reproducción usa la fórmula documentada, pero puede
diferir en detalles de la implementación real de `alpha.js`.

---

## 2026-08-03 — Diagnóstico del timing del score técnico

Motivado por la observación de que "el sistema marca compra cuando el
activo ya subió". Medido corriendo `combinedSignal()` real sobre 39
tickers, último año, ~2.300 señales:

| Señal | Ya subió (20d antes) | Queda (20d después) |
|---|---|---|
| COMPRA FUERTE | **+13.7%** | **+0.42%** |
| COMPRA | +7.1% | +1.1% |
| VENTA | -0.8% | +1.46% |
| VENTA FUERTE | -9.2% | +0.79% |
| *baseline* | — | *+0.97%* |

**COMPRA FUERTE captura +0.42% después de un movimiento de +13.7%** —
ratio 34:1 entre lo ya ocurrido y lo disponible. Y rinde por debajo del
baseline (0.97%).

No se puede afirmar que la señal esté invertida (t(VENTA vs COMPRA
FUERTE) = 1.48, no significativo), pero **sí que COMPRA FUERTE no aporta
nada sobre comprar al azar**.

**No existe "el paso antes" en estos indicadores.** Se probaron 5 reglas
de entrada basadas en el perfil precursor (el estado 7 días antes de que
dispare la señal) y **ninguna supera al baseline**; la mejor da 0.83% vs
0.89%, y "RSI 50-65" da -0.32% (t=-3.15, peor de forma significativa).
Correlaciones con el retorno a 20 días: RSI **0.004**, ya-subió-10d
0.095, vs-SMA20 0.085.

**Razón estructural:** los indicadores del score se calculan *a partir*
del movimiento de precio. No lo anticipan, lo describen. Adelantar la
señal solo agrega falsos positivos.

---

## 2026-08-03 — Búsqueda exhaustiva: 43 indicadores + 66 combinaciones de a dos

**Método:** biblioteca de 43 indicadores técnicos (RSI 7/14/21, MACD
línea e histograma, estocástico K/D, Williams %R, CCI, ROC 5/10/20/60,
Bollinger posición y ancho, ATR y ratio, ADX/DMI, OBV, MFI, CMF, Aroon,
TRIX, CMO, Force Index, Stochastic RSI, Vortex, Donchian, Ultimate
Oscillator, distancias a máximos, gap, cuerpo de vela, volumen relativo
y shock). IC de Spearman cross-seccional por fecha, horizonte 10 días,
primero en 3 meses y después validado sobre 10 años (110.806 obs).

**Hallazgo principal — el momentum está invertido a 10 días:**
24 indicadores pasan Bonferroni sobre 10 años, y **casi todos con IC
negativo**. Más RSI, más ROC, más por encima de la SMA20, más cerca del
techo de Donchian → *menor* retorno futuro. El signo coincide entre la
ventana de 3 meses y los 10 años en 23 de 24 casos.

| Indicador | IC 10a | t | IC 3m | mismo signo |
|---|---|---|---|---|
| vsSma20 | -0.0366 | -5.80 | -0.0625 | sí |
| roc20 | -0.0337 | -5.51 | -0.0967 | sí |
| roc5 | -0.0328 | -5.34 | -0.0521 | sí |
| pctDesdeMax20 | -0.0320 | -5.27 | -0.0531 | sí |
| cci | -0.0294 | -5.20 | -0.0294 | sí |
| adx | **+0.0141** | 3.24 | +0.0709 | sí |
| volRatio | +0.0172 | 4.02 | -0.0044 | no |

**Mejores combinaciones de a dos** (ambos invertidos): roc20+roc5
(IC +0.0387, t=6.26, IR 0.18), vsSma20+roc20 (+0.0370, t=5.95),
vsSma20+roc5 (+0.0372, t=5.93). La ganancia sobre el mejor indicador
individual es marginal — los indicadores están muy correlacionados
entre sí, combinarlos no agrega información nueva.

**Test operativo (quintil más castigado por vsSma20):**

| Etapa | Exceso | t |
|---|---|---|
| Sin control | +0.41% | 2.26 |
| **Con control de volatilidad** | **+0.279%** | **1.47 (no significativo)** |
| Neto de costos (1.61%) | **-1.327%** | — |
| Anualizado | **-33.45%** | — |

**Conclusión: existe señal detectable pero NO operable.** El efecto es
real y consistente (10 años, signo estable, Bonferroni), pero es 5-6x
más chico que el costo de transacción. Y el IR de la mejor combinación
es 0.18 — muy lejos del 1.06 que reportaba el traspaso para el alfa, lo
que refuerza que aquel número estaba sobreajustado.

**Implicación importante para el sistema:** la dirección del efecto es
*contraria* a cómo el score técnico usa estos indicadores. El score
marca COMPRA FUERTE con momentum alto; los datos dicen que el momentum
alto precede retornos menores. Eso explica por qué COMPRA FUERTE rinde
por debajo del baseline.

---

## 2026-08-03 — CONCLUSIÓN CAUSAL: por qué el score marca COMPRA después de la suba

Cierre del diagnóstico. La pregunta ya no era *si* llega tarde (medido:
COMPRA FUERTE aparece tras +13.7% y captura +0.42%) sino **qué** lo hace
llegar tarde. Análisis forense del código de `combinedSignal()` más
medición sobre 13.380 arranques de suba (+15% en ≤40 días).

### La causa: cada regla exige que el movimiento ya haya ocurrido

Las reglas del Motor A (momentum) no son predicciones, son
**verificaciones de que la suba pasó**. Literalmente: `roc10 > 3.0`
significa "ya subió más de 3% en 10 días".

| Regla | Puntos | Día mediano en que se enciende |
|---|---|---|
| `roc5 > 1%` | +8 | 5 |
| `m5 > 3%` | +8 | 5 |
| MACD hist > umbral | +10/+20 | 7 |
| `roc10 > 3%` | **+20** | 9 |
| `SMA20 > SMA50` | +12 | **14** |

El cruce de medias es el más lento por razón matemática: una SMA de 20
ruedas necesita que los precios nuevos pesen lo suficiente para arrastrar
el promedio sobre otra de 50. Ese retraso es inherente al promedio móvil.

### Resultado agregado (motor real sobre 315 arranques)

- Marca COMPRA en el día 8, **COMPRA FUERTE en el día 12**
- El tramo completo dura 19 días
- Al marcar COMPRA FUERTE ya subió **9.0%** de un total de 16.7%
- Queda **7.3%** por delante
- **→ el score se enciende con el 54% de la suba ya consumida**

### Hipótesis que se probó y resultó FALSA

Se sospechaba que la selección de motor amplificaba el retraso: como
`wMom = 0.35 + 0.5·min(1, |SMA20−SMA50|/SMA50 / 0.03)`, al arranque las
medias están juntas → menos peso al momentum. **Medido, no es así**: el
wMom es plano a lo largo del tramo (0.770 en día 0 → 0.747 en día 30) e
incluso baja levemente. La ponderación no es la culpable; el retraso está
enteramente en los componentes.

### Implicación: no se arregla bajando umbrales

Bajar `roc10 > 3%` a `roc10 > 1%` adelanta el encendido pero también lo
dispara en cientos de movimientos que no van a ningún lado — se cambia
retraso por falsos positivos. Es exactamente lo que mostró el test de
reglas precursoras: la que se encendía más temprano ("RSI 50-65") rendía
−0.32%, peor que el azar.

**Un sistema que se encienda antes necesita información que no derive del
precio.** Todo indicador calculado a partir del precio va a llegar, por
construcción, después de que el precio se movió. Ver la sección de
disparadores no-precio en `docs/disparadores.md`.

---

## 2026-08-17 — Patrón EVO+reversión+RSI+MACD: tres corridas, mismo veredicto

Continuación del hallazgo del 2026-08-03 (mismo patrón, primera vez).
Se probó en tres muestras de tamaño y método crecientes; las tres
descartan la hipótesis, lo que la deja mucho más firme que un solo test.

### Corrida 1 — top 20 ganadores de 30 días

Encontró 27 casos con la condición (EVO>50, reversión>50, RSI<50,
MACD<0) dentro del top 20 de mejor rendimiento a 30 días: WR 91-100% a
10-20 días. **Con control de sesgo de selección** (misma condición en
el resto del universo, no solo en los ganadores ya conocidos): WR
51-60%, indistinguible de azar.

### Corrida 2 — reconstrucción diaria sobre 10 años

Como `combinedSignal()` (de donde salen EVO y reversión) está calibrada
para la serie horaria (~1 año de historia), se reconstruyó una versión
compatible con series diarias para poder testear 10 años. **Validación
de fidelidad: solo 34% de coincidencia de señal** contra la función
real, con diferencias de 13-18 puntos en EVO/reversión — los períodos
de la fórmula (24 barras, 6 barras) fueron calibrados para horas, no
para días, y no trasladan su significado. Aun así, usando solo los
componentes que sí traducen limpio a diario (RSI-14, MACD, Bollinger-20
estándar, sin el componente EVO): **sin edge en ningún test** — 79.019
casos sobre 10 años, retorno CON la condición prácticamente idéntico a
SIN ella en los 5 horizontes (máx t=1.67), consistencia anual 55%
(debajo del umbral 65%), exceso ajustado por volatilidad 0.12% (t=0.77).

### Corrida 3 — año completo con la función REAL (sin aproximación)

Como el año calza con la cobertura horaria, se pudo usar
`combinedSignal()` real (no la reconstrucción). 1.337 casos encontrados
sobre 153 tickers. **Confirma la corrida 1**: dentro del top 20 (sesgado)
WR 66% a 20d; en el resto del universo (control real), WR 50% — moneda
al aire.

### Extensión: quintil de alfa + Fibonacci

Se cruzó el patrón con el quintil de alfa cross-sectional (calculado
punto-en-el-tiempo para cada fecha) y el nivel de Fibonacci más cercano
(swing de 60 días). De 35 combinaciones posibles (5 quintiles × 7
niveles), la mejor (Q5 + retroceso profundo 78.6%/100%) daba +4.83% a
20d, WR 60% — pero:
- t corregido por solapamiento = 0.57 (con Bonferroni por 35
  comparaciones, haría falta |t|>2.9)
- muestra efectiva real: **8**, no 164
- dentro del top20 (sesgado): +15.32%, WR 80% — fuera (control real):
  +3.37%, WR 57%, ya sin significancia

Se amplió además la ventana a día-por-día (1 a 15 días) para esa misma
combinación: **el día 7 específicamente es el punto de la curva donde
el combo MENOS se distingue del control** (combo +0.63% vs. control
+0.66%, t=-0.02). Ningún día de los 15 alcanza significancia (máx
t=0.46). Sin sesgo, el día 7 da +0.07% con 46% de aciertos.

**Veredicto: descartada en las tres escalas probadas.** No es un
patrón chico que necesite más ajuste — es sistemáticamente ausente,
independientemente de cómo se lo mida.

---

## 2026-08-17 — Discrepancia entre los 20 alcistas y los 20 bajistas del último año: solo un indicador se distingue, y no sobrevive validación

**Origen:** comparar los 9 indicadores técnicos y fundamentales, medidos
**al inicio** del período (sin lookahead), entre el top 20 de mejor y
peor rendimiento del último año.

**Primer resultado:** de RSI, MACD, distancia a SMA50/200, volatilidad,
calidad fundamental, margen, deuda y **distancia al máximo de 52
semanas** — solo el último cruzó significancia (t=2.57). Los activos
que estaban cerca de sus máximos al arrancar el período rindieron mejor
(+29.0%, WR 84%) que los muy castigados (-5.2%, WR 30%), efecto
contrario a la intuición de "está barato, tiene que rebotar" y más
cercano al momentum académico (Jegadeesh-Titman).

**Validación con ventanas móviles sobre 10 años (13.739 observaciones,
cada 21 días, forward 252d):** el efecto **desaparece por completo**.
Los quintiles quedan planos (+36.7% a +42.5%, sin orden), t corregido
por solapamiento = 0.10. Consistencia anual 5/9 (56%, debajo del umbral
65%), con variación enorme entre años (2020: -35pp de diferencia; 2022:
+82pp). Lo único donde aparece significancia es en los quintiles de
volatilidad más alta (44-71% anual) — ahí es exposición a riesgo en un
mercado que subió, no señal predictiva; en volatilidad normal (la
mayoría del universo) el efecto es nulo (t entre -0.20 y 0.47).

**Por qué el primer test se veía tan bien:** comparar dos grupos
definidos por su resultado ya conocido (top 20 ganadores vs. top 20
perdedores) es la misma trampa de sesgo de selección de siempre, en otra
forma. Con ventanas móviles sobre todo el universo se disuelve.

**Veredicto: descartado como indicador.** No se agregó a la app.

---

## 2026-08-17 — Reacción del mercado a los anuncios FOMC: 78 eventos, 2017-2026

**Método:** calendario histórico completo de anuncios FOMC (fuente:
federalreserve.gov), trayectoria del universo día -5 a +20 alrededor de
cada anuncio, clasificando por si la reacción del día 0 fue positiva o
negativa. A diferencia del test de agosto (solo 4 reuniones de 2026,
"sin conclusión" por falta de muestra), acá la muestra es de 76 eventos
con datos suficientes — mucho más sólida.

**Resultado:**
- **Antes del anuncio no hay nada:** días -5 a -1 planos, sin diferencia
  entre lo que después resultó positivo o negativo. El mercado no
  anticipa la decisión.
- **El salto ocurre en el día 0**, de golpe: +1.27% tras reacción
  positiva vs. -0.94% tras negativa (t=11.34).
- **Después, la brecha se mantiene y crece** hasta +4.47% vs +0.01% al
  día 20 (t significativo todos los días) — PERO al descontar el salto
  del día 0 y medir solo el recorrido posterior (día 0→20): +3.15% vs
  +0.95%, t=1.29, **no significativo**. La mayor parte de la brecha es
  el salto inicial que nunca se revierte, no una tendencia posterior
  capturable.
- Consistencia anual: 5/7 años con muestra suficiente (71%), pero con
  dispersión grande entre años (2019/2020 con +15pp de diferencia,
  2018 con -3pp).

**Impacto sectorial:** se probó si algún sector concentra el impacto
(18 sectores, comparando volatilidad y retorno medio en días FOMC vs.
días normales). Ningún sector muestra volatilidad desproporcionada
(ratios 0.56x-1.37x, todos cerca de 1). Solo "Consumo" cruza
significancia en retorno medio (t=-2.02), pero con 18 comparaciones
es exactamente el tipo de falso positivo esperable por azar — no
sobrevive sin corrección. El movimiento post-Fed parece ser de mercado
amplio, no de rotación sectorial.

**Salvedad metodológica:** la clasificación positiva/negativa se hizo
por la reacción del mercado ese día, no por el contenido del
comunicado — es circular por diseño. No se puede afirmar "un anuncio
dovish produce X", solo "cuando el mercado ya reaccionó bien, después
pasó Y" (y ese "después" no es significativo).

**No se implementó nada en la app**: el hallazgo es informativo
(contexto de por qué se mueve el mercado en ventanas FOMC — ver el
panel `estadoFOMC()` ya existente), no una señal operable.
