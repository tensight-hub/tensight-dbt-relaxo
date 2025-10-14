WITH LatestPrices AS (
    SELECT
    scraped_date,
        relaxo_sku,
        source,
        product_id,
        mrp,
        product_price,
        ROW_NUMBER() OVER(PARTITION BY relaxo_sku, source, product_id ORDER BY scraped_date DESC) as rn
   from 
{{ ref('int_buybox_rating_and_reviews') }}
)
SELECT
    relaxo_sku,
    scraped_date,
    source,
    product_id,
    mrp,
    product_price
FROM LatestPrices
WHERE rn = 1;
