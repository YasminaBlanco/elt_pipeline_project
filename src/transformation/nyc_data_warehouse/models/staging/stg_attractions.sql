SELECT
    nombre AS nombre_atraccion,
    direccion AS direccion_atraccion,
    url AS url_atraccion,
    latitude,
    longitude
FROM {{ source('pipeline', 'atracciones_raw') }}