SELECT
    nombre AS nombre_museo,
    direccion AS direccion_museo,
    url AS url_museo,
    latitude,
    longitude
FROM {{ source('pipeline', 'museos_raw') }}