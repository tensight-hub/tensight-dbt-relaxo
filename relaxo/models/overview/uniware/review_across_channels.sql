SELECT
    rr.scraped_date,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.sku_size,
    pm.sku_gender,
    
     MAX(CASE
        WHEN rr.source = 'amazon'
        THEN CAST(CAST(CASE WHEN lower(rr.no_of_reviews) = 'nan' THEN '0' ELSE rr.no_of_reviews END AS DECIMAL) AS INT)
    END) AS amazon_review_count,

    MAX(CASE
        WHEN rr.source = 'flipkart'
        THEN CAST(CAST(CASE WHEN lower(rr.no_of_reviews) = 'nan' THEN '0' ELSE rr.no_of_reviews END AS DECIMAL) AS INT)
    END) AS flipkart_review_count,

    MAX(CASE
        WHEN rr.source = 'myntra'
        THEN CAST(CAST(CASE WHEN lower(rr.no_of_reviews) = 'nan' THEN '0' ELSE rr.no_of_reviews END AS DECIMAL) AS INT)
    END) AS myntra_review_count,

    MAX(CASE
        WHEN rr.source = 'Ajio'
        THEN CAST(CAST(CASE WHEN lower(rr.no_of_reviews) = 'nan' THEN '0' ELSE rr.no_of_reviews END AS DECIMAL) AS INT)
    END) AS ajio_review_count

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

    
   