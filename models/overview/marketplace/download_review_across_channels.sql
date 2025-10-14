WITH LatestReviews AS (
    SELECT
        relaxo_sku,
        scraped_date,
        source,
        product_id,
        no_of_reviews,
        ROW_NUMBER() OVER(PARTITION BY relaxo_sku, source, product_id ORDER BY scraped_date DESC) as rn
   from 
{{ ref('int_buybox_rating_and_reviews') }}
)
SELECT
    scraped_date,
    relaxo_sku,
    source,
    product_id,
    no_of_reviews
FROM LatestReviews
WHERE rn = 1;
