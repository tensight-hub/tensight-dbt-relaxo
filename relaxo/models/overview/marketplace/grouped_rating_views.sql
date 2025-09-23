SELECT distinct
    pp.relaxo_sku,
    pm.channel_sku_id,
    rr.scraped_date,
    pm.sku_category,
    pm.sku_sub_category, 
    pm.sku_size,
    pm.sku_gender,
    rr.source AS channel,
    rr.avg_rating,
    
     (CASE
        WHEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN NULL ELSE rr.avg_rating END AS DECIMAL(10,2)) > 4
        THEN 1
        else 0 END) AS "rating>4",
    (CASE
        WHEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN NULL ELSE rr.avg_rating END AS DECIMAL(10,2)) BETWEEN 2 AND 4
        THEN 1
        else 0 END) AS "rating>2and<4",
    (CASE
        WHEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN NULL ELSE rr.avg_rating END AS DECIMAL(10,2)) < 2
        THEN 1
        else 0 END) AS "rating<2"
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



   








    