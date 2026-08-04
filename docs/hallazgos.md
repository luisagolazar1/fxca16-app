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
