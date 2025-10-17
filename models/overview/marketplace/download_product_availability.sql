WITH LatestDateWiseSoldOutStatus AS (
    SELECT
    scraped_date,
        relaxo_sku,
        source,
        product_id,
        is_sold_out, 
        ROW_NUMBER() OVER(PARTITION BY relaxo_sku, source, product_id ORDER BY scraped_date DESC) as rn
   from
{{ ref('int_buybox_rating_and_reviews') }}
)
SELECT
    scraped_date,
    relaxo_sku,
    source,
    product_id,
    CASE
        WHEN LOWER(is_sold_out) = 'true' THEN 'Out of Stock'
        WHEN LOWER(is_sold_out) = 'false' THEN 'In Stock'
        ELSE 'Unknown' 
    END AS status 
FROM LatestDateWiseSoldOutStatus
WHERE rn = 1;