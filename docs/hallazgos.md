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
