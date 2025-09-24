/*

SELECT
    rr.scraped_date,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.name,
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
     {{ ref('stg_buybox_rating_and_reviews') }} rr 
    ON pm.channel_sku_id = rr.product_id
WHERE
    rr.scraped_date IS NOT NULL
GROUP BY
    pm.name,
    rr.scraped_date,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.sku_size,
    pm.sku_gender
ORDER BY
    pp.relaxo_sku;

    */





SELECT
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    name,
    sku_size,
    sku_gender,
    max(image_url) AS image_url,

    {{ channel_price('source', 'product_price', 'amazon', 'amazon_price') }},
    {{ channel_price('source', 'product_price', 'flipkart', 'flipkart_price') }},
    {{ channel_price('source', 'product_price', 'myntra', 'myntra_price') }},
    {{ channel_price('source', 'product_price', 'ajio', 'ajio_price') }}

from 
{{ ref('int_buybox_rating_and_reviews') }}

GROUP BY
    name,
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    sku_size,
    sku_gender;
