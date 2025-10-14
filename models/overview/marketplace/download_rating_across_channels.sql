WITH LatestRatings AS (
    SELECT
        scraped_date,
        relaxo_sku,
        source,
        product_id,
        avg_rating,
        ROW_NUMBER() OVER(PARTITION BY relaxo_sku, source, product_id ORDER BY scraped_date DESC) as rn
   from 
{{ ref('int_buybox_rating_and_reviews') }}
)
SELECT
 scraped_date,
    relaxo_sku,
     source,
    product_id,
   avg_rating
FROM LatestRatings
WHERE rn = 1;
