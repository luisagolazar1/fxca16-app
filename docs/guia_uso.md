# FXCA16 — Guía de uso eficiente

La app tiene mucha información porque cada pieza salió de medir algo,
no de agregar por agregar. Esta guía es el recorrido corto: qué mirar
primero, qué ignorar, y cómo no perder tiempo con datos que ya sabemos
que no sirven para decidir.

---

## El recorrido de 60 segundos, para cualquier acción

Si solo tenés un minuto antes de decidir algo, en este orden:

1. **🔍 Detalle → Calidad fundamental.** Si dice FRÁGIL, ya tenés la
   mitad de la decisión. No hace falta mirar nada técnico si la empresa
   está podrida por dentro.
2. **🔍 Detalle → Calendario de balance.** Si faltan pocos días, lo que
   veas en el precio puede ser ruido de expectativa, no señal.
3. **🔍 Detalle → alertas de calidad de serie.** Si dice "serie
   degradada" o "dudosa", el papel casi no opera — el precio quedó
   quieto por falta de compradores, no por estabilidad real. Ignorá
   cualquier indicador técnico de esa acción.
4. Recién ahí mirá la **señal técnica**. Y hacelo sabiendo lo que dice
   el punto siguiente.

---

## Lo que hay que saber ANTES de mirar cualquier señal técnica

Esto no es una opinión, es lo que se midió en `docs/hallazgos.md`:

- **La señal técnica llega tarde por diseño.** COMPRA FUERTE aparece
  después de que el papel ya subió, en promedio, más de la mitad del
  movimiento. No es un bug de calibración — cada indicador (RSI, MACD,
  cruce de medias) *verifica* que algo ya subió, no lo anticipa.
- **Ningún indicador técnico solo, ni combinado de a dos, cubre el
  costo de operar** (1.2% Merval / 1.8% CEDEAR ida y vuelta) de forma
  confiable. El mejor caso encontrado da una ventaja de 5-6 veces menor
  al costo.
- **El alfa de USA está sobreajustado** a la ventana en que se
  construyó — no se sostiene sobre 10 años de historia. El de Merval es
  **preliminar**, muestra chica, no confirmado.

Conclusión práctica: usá la señal técnica como **descripción del estado
actual**, no como pronóstico. Es útil para saber "¿qué está pasando acá
ahora?", no para saber "¿qué va a pasar?".

---

## Qué SÍ mirar, de mayor a menor confiabilidad

| Prioridad | Qué mirar | Por qué |
|---|---|---|
| 1 | Fragilidad fundamental | Es un filtro de riesgo real, no una predicción — margen negativo o deuda alta no se arregla solo |
| 2 | Calendario de balance / reunión Fed | Fechas conocidas de antemano, el único tipo de catalizador que se puede anticipar de verdad |
| 3 | Calidad de la serie de precios | Evita operar sobre ruido de un papel ilíquido |
| 4 | Noticias del activo | Información que no deriva del precio — pero es agenda sin filtrar, hay que leerla con criterio propio |
| 5 | Contexto de RSI / patrones de vela | Solo como descripción estadística de dónde está parado el precio, nunca como señal de entrada — el propio panel te lo advierte |
| 6 | Score técnico (COMPRA/VENTA) | El menos confiable de todos — llega tarde y el costo se come cualquier ventaja |

---

## Recorrido por tab

### 🎯 Oportunidades
Vista general del universo con el filtro P80. Sirve para explorar, no
para decidir — cualquier cosa que te llame la atención, andá a su
Detalle antes de sacar una conclusión.

### 🔍 Detalle
El más completo. Todo lo nuevo que se agrega va al final, en
**"⊕ Agregados recientes"** — así siempre sabés qué es lo último que se
sumó sin tener que memorizar cambios.

### ⏪ Replay
La herramienta más honesta de la app. Tocás cualquier barra de
cualquier fecha pasada y el sistema recalcula la señal **cortando la
serie ahí** — sin ver nada posterior. Es la forma de comprobar con tus
propios ojos si la señal llegó a tiempo o tarde en un caso concreto.

**Cómo usarlo bien:** no saques conclusiones de un solo click. Tocá 4 o
5 fechas distintas del mismo activo, o mirá el ranking de "suben/caen"
y usá el botón de cruce automático para ver el patrón en varios activos
a la vez.

### ⚖️ Comparar / ⭐ Listas / 📌 Tracker
Organización personal. El Tracker es lo único que congela una
predicción en el momento en que se hizo — es la evidencia más valiosa
que tenés, porque a diferencia de todo lo demás, nadie la pudo ajustar
después de ver el resultado.

### 🔬 Validación
Acá vive la letra chica: qué se probó, qué se descartó, y por qué. Si
alguna vez pensás "che, ¿y si probamos X patrón técnico?" — mirá primero
la sección **"Hipótesis probadas y descartadas"**. Es información
tan valiosa como una señal positiva: te ahorra repetir un camino que ya
se probó que no lleva a ningún lado.

---

## Errores comunes que esta guía te evita

- **Leer "COMPRA FUERTE" como recomendación** en vez de como descripción
  de que el papel ya viene subiendo.
- **Operar un papel marcado como serie dudosa/degradada** — el precio
  ahí es ruido, no información.
- **Ignorar la fragilidad fundamental** porque la señal técnica se ve
  bien. Son independientes a propósito: la fundamental es la única que
  no se contamina de que "el precio ya subió".
- **Armar una regla de trading a partir de un solo caso llamativo** (una
  suba de 5 acciones juntas, un patrón de vela que "funcionó" una vez).
  Todo lo que sobrevivió esa prueba a gran escala está en
  `docs/hallazgos.md` — si no está ahí, probablemente no se sostiene.

---

## Dónde profundizar si querés el detalle completo

- `docs/hallazgos.md` — cada hipótesis probada, con metodología y
  resultado, incluidas las que fallaron
- `docs/disparadores.md` — catálogo de qué información puede anticipar
  un movimiento, y qué tan accesible es cada una

Esta guía es el resumen ejecutivo de esos dos documentos. Si algo acá
te genera dudas de "¿por qué es así?", la respuesta con números está
en `hallazgos.md`.
