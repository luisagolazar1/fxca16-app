# Generar el checklist de activos

`scripts/checklist_plantilla.html` es la plantilla. Para regenerar el
checklist con datos frescos hay que leer `src/data.js`, calcular por
activo (precio, retornos 5/20/60/90d, señal con `combinedSignal`, RSI,
volatilidad, distancia a máximos, `calidadSerie`, fundamentales) y
reemplazar el marcador `/*__DATA__*/[]` de la plantilla por el JSON del
arreglo de activos.

Campos esperados por la plantilla (nombres cortos para achicar el HTML):

    t   ticker            n   nombre           s   sector
    m   USA | MERVAL      p   precio
    r5 r20 r60 r90        retornos %
    g   señal             c   confianza %      rs  RSI
    v   volatilidad %     dm  % desde máx 20   d52 % desde máx 52s
    q   ok|dudosa|degradada                    qm  motivo
    cf  calidad fund.     fr  frágil (bool)    bn  banderas
    mn  margen neto %     roe ROE %            dp  deuda/patr %
    lq  liquidez corriente                     sg  en seguimiento (bool)

El checklist marca en gris los campos que el sistema no tiene (earnings,
noticias, guidance). Eso es deliberado: gris significa "sin dato", no
"sin problema".
