/*
Este modelo unifica los datos de los museos y atracciones en una sola tabla.
Esto evita la duplicación de código en modelos posteriores y facilita el cálculo
de distancias a cualquier punto de interés.
*/

-- depends_on: {{ ref('stg_attractions') }}
-- depends_on: {{ ref('stg_museums') }}

WITH all_locations AS (

    SELECT
        nombre_atraccion AS nombre_lugar,
        direccion_atraccion AS direccion_lugar,
        url_atraccion AS url_lugar,
        latitude AS latitud,
        longitude AS longitud,
        'atraccion' AS tipo_lugar
    FROM {{ ref('stg_attractions') }}

    UNION ALL

    SELECT
        nombre_museo AS nombre_lugar,
        direccion_museo AS direccion_lugar,
        url_museo AS url_lugar,
        latitude AS latitud,
        longitude AS longitud,
        'museo' AS tipo_lugar
    FROM {{ ref('stg_museums') }}

)

SELECT * FROM all_locations