SELECT
    rr.scraped_date,
    pp.relaxo_sku,
    pm.name,
    pm.sku_category,
    pm.sku_sub_category,
    'Ratings' AS filter,
    pm.sku_size,
    pm.sku_gender,
    
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
    END) AS VARCHAR) AS ajio

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
    END) AS VARCHAR) AS ajio
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

    
    
    
   



    
   



    
   



    
   