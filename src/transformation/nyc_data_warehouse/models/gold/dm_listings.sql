-- models/datamart/dm_listings.sql
/*
Este modelo es la tabla central de hechos. Presenta cada listado
con todas sus métricas enriquecidas, sin agregaciones, lista para
el análisis exploratorio, la identificación de outliers y la visualización.

¿Cómo se distribuyen los precios y qué outliers existen? Puedes graficar el campo price_usd para encontrar valores atípicos.

¿Qué relación hay entre la disponibilidad anual y la cantidad de reseñas? Puedes crear un gráfico de dispersión con availability y number_of_reviews para identificar patrones de ocupación.

¿Cómo evoluciona el número de reseñas por mes en los diferentes distritos de la ciudad? Al tener last_review_date, puedes usar este campo para analizar la evolución de las reseñas a lo largo del tiempo.

¿Cuál es la distancia promedio de los listados a las atracciones? Puedes consultar la columna distance_to_attraction para obtener esta métrica.
*/
SELECT
    listing_id,
    listing_name,
    host_id,
    host_name,
    district,
    neighborhood,
    room_type,
    price_usd,
    price_ars,
    reviews_per_month,
    estimated_annual_revenue,
    availability,
    closest_attraction_name,
    distance_to_attraction,
    last_review_date
FROM {{ ref('int_listings_with_metrics') }}