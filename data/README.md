# data/balances_manual.json

Calendario de balances cargado a mano, porque el fetch automático
contra Yahoo (`descargar_earnings()` en `scripts/update_data.py`) lleva
**tres intentos fallidos** y `FXCA16_EARNINGS` seguía vacío en
producción sin forma de ver el log del Action para diagnosticar más.

## Origen

Cargado desde `Calendario_Master_Balances_2026.xlsx` el 2026-08-18.
54 tickers, cruzados 1:1 contra el universo de la app sin errores de
nomenclatura (verificado — ningún ticker del Excel quedó fuera del
match). Los 5 ETFs del archivo (SPY, QQQ, IWM, GLD, TLT) no tienen
fecha porque no reportan balance — correcto, no es un dato faltante.

## Cómo se usa

`scripts/update_data.py` lo carga como **piso confiable**: se fusiona
con lo que ya está en `data.js` (que puede tener actualizaciones más
frescas si el fetch automático alguna vez empieza a traer datos), sin
pisar nada que ya esté cargado. Si el fetch sigue vacío, el calendario
manual queda intacto igual — no depende de que la API funcione.

## Cómo actualizarlo

Cuando haya un calendario más nuevo (después de que pase la próxima
tanda de balances, o si aparecen fechas nuevas confirmadas):

1. Conseguir un Excel con las mismas columnas: `Ticker`, `Nombre de la
   Empresa`, `Mercado / Categoría`, `Trimestre`, `Fecha de Presentación`
   (formato "30 de Julio de 2026"), `Horario (EST)`, `Estado Actual`.
2. Correr `python3 scripts/convertir_balances_excel.py archivo.xlsx`
   — regenera este JSON.
3. Commitear. El próximo `update_data.py` lo recoge solo.

## Limitación conocida

Es una foto fija a la fecha de carga — no se actualiza sola entre
subidas de Excel. Los balances "Estimada" pueden moverse unos días en
la práctica; verificar cerca de la fecha si es una decisión importante.
