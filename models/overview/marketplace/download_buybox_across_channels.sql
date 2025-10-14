WITH LatestDateWiseSellerName AS (
    SELECT
    scraped_date,
        relaxo_sku,
        source,
        product_id,
        seller_name,
        ROW_NUMBER() OVER(PARTITION BY relaxo_sku, source, product_id ORDER BY scraped_date DESC) as rn
   from 
{{ ref('int_buybox_rating_and_reviews') }}
)
SELECT
    relaxo_sku,
    scraped_date,
    source,
    product_id,
    seller_name
FROM LatestDateWiseSellerName
WHERE rn = 1;
