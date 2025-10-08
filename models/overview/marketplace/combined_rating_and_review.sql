
SELECT
    rr.scraped_date,
    pp.relaxo_sku,
    pm.name,
    pm.sku_category,
    pm.sku_sub_category,
    'Ratings' AS filter,
    pm.sku_size,
    pm.sku_gender,
    max(image_url) AS image_url,
    
     CAST(MAX(CASE
        WHEN rr.source = 'amazon'
        THEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN '0' ELSE rr.avg_rating END AS DECIMAL(10,2))
    END) AS VARCHAR) AS amazon,
    CAST(MAX(CASE
        WHEN rr.source = 'flipkart'
        THEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN '0' ELSE rr.avg_rating END AS DECIMAL(10,2))
    END) AS VARCHAR) AS flipkart,
    CAST(MAX(CASE
        WHEN rr.source = 'myntra'
        THEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN '0' ELSE rr.avg_rating END AS DECIMAL(10,2))
    END) AS VARCHAR) AS myntra,
    CAST(MAX(CASE
        WHEN rr.source = 'Ajio'
        THEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN '0' ELSE rr.avg_rating END AS DECIMAL(10,2))
    END) AS VARCHAR) AS ajio,
    MAX(CASE WHEN rr.source = 'amazon' THEN rr.product_url END) AS amazon_url,
    MAX(CASE WHEN rr.source = 'flipkart' THEN rr.product_url END) AS flipkart_url,
    MAX(CASE WHEN rr.source = 'myntra' THEN rr.product_url END) AS myntra_url,
    MAX(CASE WHEN rr.source = 'Ajio' THEN rr.product_url END) AS ajio_url

    from 

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
    'Ratings',
    rr.scraped_date,
    pm.name,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.sku_size,
    pm.sku_gender

UNION ALL 

SELECT
    rr.scraped_date,
    pp.relaxo_sku,
    pm.name,
    pm.sku_category,
    pm.sku_sub_category,
    'Reviews' AS filter,
    pm.sku_size,
    pm.sku_gender,
    max(image_url) AS image_url,
    CAST(MAX(CASE
        WHEN rr.source = 'amazon'
        THEN CAST(CAST(CASE WHEN lower(rr.no_of_reviews) = 'nan' THEN '0' ELSE rr.no_of_reviews END AS DECIMAL) AS INT)
    END) AS VARCHAR) AS amazon,
    CAST(MAX(CASE
        WHEN rr.source = 'flipkart'
        THEN CAST(CAST(CASE WHEN lower(rr.no_of_reviews) = 'nan' THEN '0' ELSE rr.no_of_reviews END AS DECIMAL) AS INT)
    END) AS VARCHAR) AS flipkart,
    CAST(MAX(CASE
        WHEN rr.source = 'myntra'
        THEN CAST(CAST(CASE WHEN lower(rr.no_of_reviews) = 'nan' THEN '0' ELSE rr.no_of_reviews END AS DECIMAL) AS INT)
    END) AS VARCHAR) AS myntra,
    CAST(MAX(CASE
        WHEN rr.source = 'Ajio'
        THEN CAST(CAST(CASE WHEN lower(rr.no_of_reviews) = 'nan' THEN '0' ELSE rr.no_of_reviews END AS DECIMAL) AS INT)
    END) AS VARCHAR) AS ajio,
    MAX(CASE WHEN rr.source = 'amazon' THEN rr.product_url END) AS amazon_url,
    MAX(CASE WHEN rr.source = 'flipkart' THEN rr.product_url END) AS flipkart_url,
    MAX(CASE WHEN rr.source = 'myntra' THEN rr.product_url END) AS myntra_url,
    MAX(CASE WHEN rr.source = 'Ajio' THEN rr.product_url END) AS ajio_url

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
    'Reviews',
    pm.name,
    rr.scraped_date,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.sku_size,
    pm.sku_gender

order by 
 relaxo_sku;
 


/*
SELECT
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    name,
    'Ratings' AS filter,
    sku_size,
    sku_gender,
    max(image_url) AS image_url,
    
    {{ channel_avg_rating('source', 'avg_rating', 'amazon', 'amazon') }},
    {{ channel_avg_rating('source', 'avg_rating', 'flipkart', 'flipkart') }},
    {{ channel_avg_rating('source', 'avg_rating', 'myntra', 'myntra') }},
    {{ channel_avg_rating('source', 'avg_rating', 'Ajio', 'ajio') }}

from 
{{ ref('int_buybox_rating_and_reviews') }}

GROUP BY
'Ratings',
    name,
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    sku_size,
    sku_gender

UNION ALL 


    SELECT
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    'Reviews' AS filter,
    name,
    sku_size,
    sku_gender,
    max(image_url) AS image_url,

    {{ channel_reviews('source', 'no_of_reviews', 'amazon', 'amazon') }},
    {{ channel_reviews('source', 'no_of_reviews', 'flipkart', 'flipkart') }},
    {{ channel_reviews('source', 'no_of_reviews', 'myntra', 'myntra') }},
    {{ channel_reviews('source', 'no_of_reviews', 'Ajio', 'ajio') }}

from 
{{ ref('int_buybox_rating_and_reviews') }}

GROUP BY
   'Reviews',
    name,
    scraped_date,
    relaxo_sku,
    sku_category,
    sku_sub_category,
    sku_size,
    sku_gender

    ORDER BY
 relaxo_sku;
 */






    
    
    
   



    
   



    
   



    
   