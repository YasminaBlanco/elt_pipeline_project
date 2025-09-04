SELECT
    codigoMoneda AS codigo_moneda,
    descripcion AS descripcion_moneda,
    tipoCotizacion AS cotizacion
FROM {{ source('pipeline', 'bcra_raw') }}