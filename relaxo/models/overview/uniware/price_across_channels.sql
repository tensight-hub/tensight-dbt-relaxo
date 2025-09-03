SELECT
    rr.scraped_date,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.sku_size,
    pm.sku_gender,
    
    MAX(CASE
        WHEN rr.source = 'amazon'
        THEN
            CAST(CASE WHEN lower(rr.product_price) = 'nan' THEN '0' ELSE rr.product_price END AS DECIMAL(10, 2))
    END) AS amazon_price,
     MAX(CASE
        WHEN rr.source = 'flipkart'
        THEN CAST(CASE WHEN lower(rr.product_price) = 'nan' THEN '0' ELSE rr.product_price END AS DECIMAL(10, 2))
    END) AS flipkart_price,
   MAX(CASE
        WHEN rr.source = 'myntra'
        THEN CAST(CASE WHEN lower(rr.product_price) = 'nan' THEN '0' ELSE rr.product_price END AS DECIMAL(10, 2))
    END) AS myntra_price,
   MAX(CASE
        WHEN rr.source = 'Ajio'
        THEN CAST(CASE WHEN lower(rr.product_price) = 'nan' THEN '0' ELSE rr.product_price END AS DECIMAL(10, 2))
    END) AS ajio_price

FROM
    {{ ref('stg_price_parity_master') }} pp
LEFT JOIN
    {{ ref('stg_product_master') }} pm 
       ON pp.relaxo_sku = pm.sku_relaxo
LEFT JOIN
    {{ ref('stg_unicommerce_rating_and_reviews') }} rr 
    ON pm.channel_sku_id = rr.product_id
WHERE
    rr.scraped_date IS NOT NULL
GROUP BY
    rr.scraped_date,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.sku_size,
    pm.sku_gender
ORDER BY
    pp.relaxo_sku;

    
   