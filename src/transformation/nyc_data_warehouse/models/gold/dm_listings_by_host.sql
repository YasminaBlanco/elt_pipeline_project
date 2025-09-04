-- models/datamart/dm_listings_by_host.sql
/*
Este modelo resume las métricas clave para cada anfitrión, incluyendo
el número de propiedades y la variación de sus precios.

¿Cuáles son los anfitriones con más propiedades listadas y cómo varían sus precios?
 Puedes ordenar la tabla por total_listings y examinar avg_price_usd, max_price_usd y min_price_usd para cada anfitrión.


*/
SELECT
    host_id,
    host_name,
    COUNT(listing_id) AS total_listings,
    AVG(price_usd) AS avg_price_usd,
    MAX(price_usd) AS max_price_usd,
    MIN(price_usd) AS min_price_usd,
    SUM(number_of_reviews) AS total_reviews
FROM {{ ref('int_listings_with_metrics') }}
GROUP BY
    host_id,
    host_name