-- models/datamart/dm_listings_by_room_type.sql
/*
Este modelo agrega métricas de negocio por tipo de habitación.

¿Cuáles son los anfitriones con más propiedades listadas y cómo varían sus precios? Puedes ordenar la tabla por total_listings y examinar avg_price_usd, max_price_usd y min_price_usd para cada anfitrión.


*/
SELECT
    room_type,
    COUNT(listing_id) AS total_listings,
    SUM(estimated_annual_revenue) AS total_estimated_annual_revenue
FROM {{ ref('int_listings_with_metrics') }}
GROUP BY
    room_type