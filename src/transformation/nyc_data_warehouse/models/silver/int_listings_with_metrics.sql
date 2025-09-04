/*
Este modelo es el corazón de la capa intermedia. Combina los datos de los listados
con las cotizaciones y las atracciones unificadas. Se calculan métricas clave como
el precio en pesos, el revenue estimado y se enriquece con la información de la
atracción más cercana.
*/

WITH listings AS (
    SELECT
        listing_id,
        listing_name,
        host_id,
        host_name,
        district,
        neighborhood,
        room_type,
        price AS price_usd,
        latitude AS listing_lat,
        longitude AS listing_lon,
        minimum_nights,
        number_of_reviews,
        reviews_per_month,
        host_listing_count,
        availability,
        last_review_date
    FROM {{ ref('stg_listings') }}
),

currencies AS (
    SELECT cotizacion FROM {{ ref('stg_currencies') }} WHERE codigo_moneda = 'USD'
),

all_locations AS (
    SELECT * FROM {{ ref('int_all_attractions') }}
),

-- Encontramos la atracción más cercana para cada listado
listings_with_closest_attraction AS (
    SELECT
        l.listing_id,
        l.listing_name,
        l.listing_lat,
        l.listing_lon,
        a.nombre_lugar AS closest_attraction_name,
        a.tipo_lugar AS closest_attraction_type,
        SQRT(
            POWER(l.listing_lat - a.latitud, 2) +
            POWER(l.listing_lon - a.longitud, 2)
        ) AS distance_to_attraction
    FROM listings AS l
    CROSS JOIN all_locations AS a
),

-- Usamos una CTE para encontrar la menor distancia y la información de la atracción
closest_attraction AS (
    SELECT
        listing_id,
        MIN(distance_to_attraction) AS min_distance
    FROM listings_with_closest_attraction
    GROUP BY 1
),

final_join AS (
    SELECT
        lwa.listing_id,
        lwa.closest_attraction_name,
        lwa.closest_attraction_type,
        lwa.distance_to_attraction
    FROM listings_with_closest_attraction AS lwa
    JOIN closest_attraction AS ca
        ON lwa.listing_id = ca.listing_id AND lwa.distance_to_attraction = ca.min_distance
)

-- El SELECT final que crea la tabla enriquecida.
SELECT
    l.listing_id,
    l.listing_name,
    l.host_id,
    l.host_name,
    l.district,
    l.neighborhood,
    l.room_type,
    l.price_usd,
    -- Cálculo del precio en pesos argentinos.
    l.price_usd * m.cotizacion AS price_ars,
    l.minimum_nights,
    l.number_of_reviews,
    l.reviews_per_month,
    l.host_listing_count,
    l.availability,
    l.last_review_date,
    -- Métrica: Revenue anual estimado (asumiendo que reviews_per_month son consistentes).
    (l.reviews_per_month * 12) AS estimated_annual_reviews,
    l.price_usd * (l.reviews_per_month * 12) AS estimated_annual_revenue,
    -- Información de la atracción más cercana.
    fj.closest_attraction_name,
    fj.closest_attraction_type,
    fj.distance_to_attraction
FROM
    listings AS l
CROSS JOIN
    currencies AS m
JOIN
    final_join AS fj ON l.listing_id = fj.listing_id