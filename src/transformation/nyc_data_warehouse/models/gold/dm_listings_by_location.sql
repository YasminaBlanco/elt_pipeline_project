-- models/datamart/dm_listings_by_location.sql
/*
Este modelo consolida métricas clave a nivel de ubicación (distrito, barrio y tipo de habitación).
Es una vista ideal para dashboards y análisis de alto nivel.

¿Cuál es el precio promedio de los alojamientos por barrio y distrito?
¿Qué barrios tienen la mayor concentración de alojamientos activos?

¿Existen diferencias significativas en la disponibilidad anual entre barrios o tipos de alojamiento? L


*/
SELECT
    district,
    neighborhood,
    COUNT(listing_id) AS total_listings,
    AVG(price_usd) AS avg_price_usd,
    AVG(availability) AS avg_availability,
    AVG(host_listing_count) AS avg_host_listings
FROM {{ ref('int_listings_with_metrics') }}
GROUP BY
    district,
    neighborhood