# Disparadores de movimiento — información que NO deriva del precio

Motivación: el diagnóstico de `hallazgos.md` concluyó que **todo indicador
calculado a partir del precio llega, por construcción, después de que el
precio se movió**. El score técnico se enciende con el 54% de la suba ya
consumida, y eso no se arregla bajando umbrales.

Un sistema que anticipe necesita información de otra naturaleza. Este es
el catálogo de qué mueve realmente a una acción, ordenado por qué tan
anticipable es y por si el proyecto puede acceder al dato hoy.

Marcas de accesibilidad:
- 🟢 **Fecha conocida de antemano** — se puede tener en el calendario
- 🟡 **Accesible pero requiere fuente** — hay API/scraping posible
- 🔴 **No anticipable** — solo reaccionar, no predecir

---

## 1. Resultados corporativos (earnings)

**El disparador individual más potente.** Referencia medida en esta misma
sesión: MSFT saltó ~8% en un día tras reportar (29/7/2026), y el CEDEAR
en pesos +15.18% sumando el componente cambiario.

| Evento | Accesibilidad | Notas |
|---|---|---|
| Fecha del reporte | 🟢 | Se publica con semanas de anticipación. Ya implementado en `estadoEarnings()` — pendiente que el workflow llene `FXCA16_EARNINGS` |
| EPS y ventas vs consenso | 🔴 | El número sale en el momento |
| **Guidance** (proyección) | 🔴 | Suele mover más que el resultado del trimestre en sí |
| Revisiones de analistas previas | 🟡 | Si el consenso se mueve en los días previos, es señal anticipada |
| Earnings whisper | 🟡 | Expectativa informal, distinta del consenso publicado |

**Lo accionable hoy:** saber que faltan N días para el reporte. No
predice dirección, pero permite decidir no abrir posición técnica justo
antes de un evento binario. Ya está en la app.

---

## 2. Anuncios corporativos

| Evento | Accesibilidad | Impacto típico |
|---|---|---|
| Fusiones y adquisiciones (M&A) | 🔴 | El adquirido salta fuerte; el adquirente suele caer |
| Recompra de acciones (buyback) | 🔴 | Alcista, demanda estructural |
| Cambio de dividendo | 🟡 | Aumentos alcistas, recortes muy bajistas |
| Split / reverse split | 🟢 | Fecha se anuncia antes; el reverse split suele ser señal de debilidad |
| Cambio de CEO/CFO | 🔴 | Alta volatilidad, dirección ambigua |
| Emisión de acciones o deuda | 🔴 | Dilución → bajista |
| Spin-off | 🟢 | Fecha conocida |
| Profit warning | 🔴 | Muy bajista, suele venir fuera de calendario |

---

## 3. Calendario macro

**Fechas conocidas con meses de anticipación** — la categoría más
sistematizable.

| Evento | Accesibilidad | Notas |
|---|---|---|
| **Reuniones FOMC** | 🟢 | Ya implementado (`FOMC_2026`). 8 por año |
| CPI / inflación EEUU | 🟢 | Mensual, fecha fija |
| Empleo (Non-Farm Payrolls) | 🟢 | Primer viernes de cada mes |
| PBI trimestral | 🟢 | Fecha fija |
| PMI / ISM | 🟢 | Mensual |
| Actas del FOMC (minutes) | 🟢 | 3 semanas después de cada reunión |
| Discursos de miembros de la Fed | 🟢 | Agenda pública |

⚠️ **Advertencia medida en esta sesión:** se testeó si el retorno
post-FOMC difiere del de una semana normal y el resultado fue
*inconcluso* — solo 4 reuniones completas en 2026, muestra insuficiente
(ver `hallazgos.md`). Saber que el evento viene sirve como gestión de
riesgo, no como señal direccional.

### Macro específico argentino (para el panel Merval)

| Evento | Accesibilidad |
|---|---|
| Dato de inflación INDEC | 🟢 mensual, fecha fija |
| Decisiones de tasa del BCRA | 🟡 |
| Vencimientos de deuda / licitaciones del Tesoro | 🟢 |
| Movimientos del dólar CCL/MEP | 🟡 **pendiente #5 del traspaso** — clave para decidir CEDEAR vs papel local |
| Riesgo país | 🟡 |
| Elecciones y anuncios de política económica | 🟢 cuando hay calendario electoral |

---

## 4. Regulatorio y legal

| Evento | Accesibilidad | Impacto |
|---|---|---|
| Aprobaciones regulatorias (FDA, antimonopolio) | 🟡 | Binario y brutal en biotech |
| Demandas y fallos judiciales | 🟡 | Fechas de audiencia son públicas |
| Cambios impositivos sectoriales | 🟡 | |
| Sanciones / controles de exportación | 🔴 | Muy relevante en semis y tech |
| Investigaciones abiertas (SEC, DOJ) | 🟡 | |

---

## 5. Sector e industria

| Señal | Accesibilidad | Notas |
|---|---|---|
| Resultados de un competidor | 🟢 | **Muy útil**: si un par reporta antes, arrastra al sector. Fecha conocida |
| Precio de materias primas | 🟡 | Petróleo→YPF/PBR, cobre→mineras, granos→agro |
| Datos de demanda sectorial | 🟡 | Ventas de autos, tráfico aéreo, inventarios de chips |
| Ciclo de inventarios | 🟡 | |
| Movimiento del índice o ETF sectorial | 🟡 | Deriva de precio pero de *otro* activo, así que puede anticipar |

**Nota:** el caso BA/GLOB/ORCL/AAL/BABA analizado en esta sesión fue
exactamente esto — cinco sectores sin relación subiendo juntos por un
catalizador macro común (la Fed), no por señal técnica individual.

---

## 6. Flujos y posicionamiento

| Señal | Accesibilidad | Notas |
|---|---|---|
| Inclusión/exclusión de un índice | 🟢 | Fecha de rebalanceo conocida; genera compra forzada de fondos indexados |
| Short interest | 🟡 | Alto short + catalizador = short squeeze |
| Operaciones de insiders | 🟡 | Se reportan con rezago pero antes del movimiento |
| Cambios en tenencias institucionales (13F) | 🟡 | Rezago de 45 días |
| Volumen y open interest de opciones | 🟡 | Actividad inusual en calls a veces precede noticias |
| Vencimiento de opciones | 🟢 | Tercer viernes de cada mes |
| Lock-up expiration post-IPO | 🟢 | Fecha conocida, presión vendedora |

---

## 7. Revisiones de analistas

| Señal | Accesibilidad | Notas |
|---|---|---|
| Upgrade/downgrade de rating | 🔴 | Impacto inmediato |
| Cambio de precio objetivo | 🟡 | |
| **Deriva del consenso de EPS** | 🟡 | La tendencia de revisiones *antes* del reporte es de lo más anticipatorio que existe |
| Inicio de cobertura | 🔴 | |

---

## 8. Lo que NO ayuda (verificado en esta sesión)

Registrado para no volver a intentarlo:

- ❌ **Indicadores de precio para anticipar** — 43 probados, 24 pasan
  significancia pero con IC negativo y efecto 5-6x menor al costo
- ❌ **Patrones de vela con su interpretación de manual** — de 15, solo
  "3 Cuervos" pasa todos los tests, y con el signo invertido respecto al
  folklore
- ❌ **Combinar dos indicadores técnicos** — están muy correlacionados,
  la ganancia es marginal
- ❌ **Alargar el horizonte** — el efecto muere antes de que el costo se
  amortice
- ❌ **Elegir los activos donde el efecto es más fuerte** — la
  correlación entre períodos es 0.090, no se pueden seleccionar

---

## Prioridad sugerida para implementar

Ordenado por relación entre valor y esfuerzo, considerando lo que el
proyecto ya tiene:

1. **Terminar el calendario de earnings** — el fix de `descargar_earnings()`
   ya está pusheado; falta confirmar en el log del workflow que la API
   nueva de yfinance trae datos. Es el disparador de mayor impacto y la
   fecha es 🟢.
2. **Calendario de resultados de competidores del sector** — si YPF
   reporta el martes, es información sobre PAMP y TGSU2 antes de que se
   muevan. Se construye con el mismo dato del punto 1.
3. **Dólar CCL/MEP** — pendiente #5 del traspaso original, imprescindible
   para decidir CEDEAR vs papel local en el panel Merval.
4. **Calendario macro ampliado** — CPI, empleo, PBI. El FOMC ya está.
5. **Deriva del consenso de analistas** — lo más anticipatorio de la
   lista, pero requiere fuente de datos paga o scraping.

**Advertencia metodológica:** cualquiera de estos, antes de convertirse en
señal, tiene que pasar la misma batería que aplicamos a todo lo demás —
muestra grande, control de volatilidad, corrección por comparaciones
múltiples, consistencia temporal y descuento de costos. Que un disparador
sea real no implica que sea operable con costos de 1.2-1.8%.
