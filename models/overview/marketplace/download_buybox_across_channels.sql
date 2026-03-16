WITH LatestDateWiseSellerName AS (
    SELECT
    scraped_date,
        relaxo_sku,
        source,
        product_id,
        seller_name,
        tagging,
        case 
            when lower(is_sold_out) = 'true' then 'out of stock'
            when lower(is_sold_out) = 'false' then 'in stock'
            else 'unknown'
        end as stock_status,
        ROW_NUMBER() OVER(PARTITION BY relaxo_sku, source, product_id ORDER BY scraped_date DESC) as rn
   from 
{{ ref('int_buybox_rating_and_reviews') }}
)
SELECT
    relaxo_sku,
    scraped_date,
    source,
    product_id,
    seller_name,
    tagging,
    stock_status
FROM LatestDateWiseSellerName
WHERE rn = 1;
