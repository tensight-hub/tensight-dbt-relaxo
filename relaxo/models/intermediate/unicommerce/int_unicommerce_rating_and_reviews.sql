

SELECT
    pp."ean /relaxo sku (which ever is common across the channels)" AS relaxo_sku,
    pm."relaxo sku" AS product_master_sku,
    pm."channel id (asin/fsn)" AS product_id,
    lower(pm.category) AS category,
    lower(pm."sub category") AS sub_category,
    pm.size,
    lower(pm.gender) AS gender,
    rr.source,

    CAST(
        CASE WHEN lower(rr.product_price) = 'nan' THEN '0' ELSE rr.product_price END AS DECIMAL(10,2)
    ) AS clean_price,
    try_cast(rr.scraped_date AS timestamp) AS scraped_date
FROM {{ ref('stg_price_parity_master') }} pp
LEFT JOIN {{ ref('stg_product_master') }} pm
    ON pp."ean /relaxo sku (which ever is common across the channels)" = pm."relaxo sku"
LEFT JOIN {{ ref('stg_unicommerce_rating_and_reviews') }} rr
    ON pm."channel id (asin/fsn)" = rr.product_id
WHERE rr.scraped_date IS NOT NULL;
