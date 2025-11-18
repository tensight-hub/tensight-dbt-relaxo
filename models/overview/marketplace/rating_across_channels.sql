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
        THEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN '0' ELSE rr.avg_rating END AS DECIMAL(10, 2))
    END) AS amazon_avg_rating,

    MAX(CASE
        WHEN rr.source = 'flipkart'
        THEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN '0' ELSE rr.avg_rating END AS DECIMAL(10, 2))
    END) AS flipkart_avg_rating,

    MAX(CASE
        WHEN rr.source = 'myntra'
        THEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN '0' ELSE rr.avg_rating END AS DECIMAL(10, 2))
    END) AS myntra_avg_rating,

    MAX(CASE
        WHEN rr.source = 'Ajio'
        THEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN '0' ELSE rr.avg_rating END AS DECIMAL(10, 2))
    END) AS ajio_avg_rating
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
    tagging,
    sku_category,
    sku_sub_category,
    name,
    sku_size,
    sku_gender,
    max(image_url) AS image_url,
    
    {{ channel_avg_rating('source', 'avg_rating', 'amazon', 'amazon_avg_rating') }},
    {{ channel_avg_rating('source', 'avg_rating', 'flipkart', 'flipkart_avg_rating') }},
    {{ channel_avg_rating('source', 'avg_rating', 'myntra', 'myntra_avg_rating') }},
    {{ channel_avg_rating('source', 'avg_rating', 'Ajio', 'ajio_avg_rating') }},
        MAX(CASE WHEN source = 'amazon' THEN product_url END) AS amazon_url,
    MAX(CASE WHEN source = 'flipkart' THEN product_url END) AS flipkart_url,
    MAX(CASE WHEN source = 'myntra' THEN product_url END) AS myntra_url,
    MAX(CASE WHEN source = 'Ajio' THEN product_url END) AS ajio_url


    

from 
{{ ref('int_buybox_rating_and_reviews') }}

GROUP BY
    name,
    scraped_date,
    relaxo_sku,
    tagging,
    sku_category,
    sku_sub_category,
    sku_size,
    sku_gender

ORDER BY
    relaxo_sku; 
   