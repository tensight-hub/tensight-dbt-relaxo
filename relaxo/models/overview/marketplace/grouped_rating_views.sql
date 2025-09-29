SELECT distinct
    scraped_date,
    relaxo_sku,
    channel_sku_id,
    sku_category,
    sku_sub_category, 
    sku_size,
    sku_gender,
    lower(source) AS channel,
    avg_rating,
    
     (CASE
        WHEN CAST(CASE WHEN lower(avg_rating) = 'nan' THEN NULL ELSE avg_rating END AS DECIMAL(10,2)) > 4
        THEN 1
        else 0 END) AS "rating>4",
    (CASE
        WHEN CAST(CASE WHEN lower(avg_rating) = 'nan' THEN NULL ELSE avg_rating END AS DECIMAL(10,2)) BETWEEN 2 AND 4
        THEN 1
        else 0 END) AS "rating>2and<4",
    (CASE
        WHEN CAST(CASE WHEN lower(avg_rating) = 'nan' THEN NULL ELSE avg_rating END AS DECIMAL(10,2)) < 2
        THEN 1
        else 0 END) AS "rating<2"
        from 
{{ ref('int_buybox_rating_and_reviews') }}




--FROM
 --   {{ ref('stg_price_parity_master') }} pp
--LEFT JOIN
   -- {{ ref('stg_product_master') }} pm 
   --    ON pp.relaxo_sku = pm.sku_relaxo
--LEFT JOIN
   --  {{ ref('stg_buybox_rating_and_reviews') }} rr 
   -- ON pm.channel_sku_id = rr.product_id
--WHERE
    --rr.scraped_date IS NOT NULL



   








    