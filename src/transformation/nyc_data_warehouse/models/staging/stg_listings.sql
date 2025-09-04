SELECT
    id AS listing_id,
    name AS listing_name,
    host_id,
    host_name,
    -- Normalizamos los nombres de distritos y barrios
    LOWER(TRIM(neighbourhood_group)) AS district,
    LOWER(TRIM(neighbourhood)) AS neighborhood,
    LOWER(TRIM(room_type)) AS room_type,
    -- Verificamos y limpiamos valores numéricos
    CAST(price AS DECIMAL) AS price,
    CAST(minimum_nights AS SIGNED) AS minimum_nights,
    CAST(number_of_reviews AS SIGNED) AS number_of_reviews,
    CAST(reviews_per_month AS DECIMAL) AS reviews_per_month,
    CAST(calculated_host_listings_count AS SIGNED) AS host_listing_count,
    CAST(availability_365 AS SIGNED) AS availability,
    -- Convertimos la fecha a un formato estándar
    STR_TO_DATE(last_review, '%Y-%m-%d') AS last_review_date,
    -- latitud y longitud de la ubicación
    latitude,
    longitude
FROM 
    {{ source('pipeline', 'nyc_raw') }}
WHERE
    price > 0 AND price IS NOT NULL
    AND calculated_host_listings_count > 0