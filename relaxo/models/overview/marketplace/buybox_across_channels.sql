
SELECT * FROM (
    SELECT
    rr.scraped_date,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.name,
    pm.sku_size,
    pm.sku_gender,
    max(image_url) AS image_url,
        MAX(CASE
            WHEN rr.source = 'amazon'
            THEN CASE WHEN lower(rr.seller_name) = 'nan' THEN '' ELSE rr.seller_name END
        END) AS amazon,
        MAX(CASE
            WHEN rr.source = 'flipkart'
            THEN CASE WHEN lower(rr.seller_name) = 'nan' THEN '' ELSE rr.seller_name END
        END) AS flipkart,
        MAX(CASE
            WHEN rr.source = 'myntra'
            THEN CASE WHEN lower(rr.seller_name) = 'nan' THEN '' ELSE rr.seller_name END
        END) AS myntra,
        MAX(CASE
            WHEN rr.source = 'Ajio'
            THEN CASE WHEN lower(rr.seller_name) = 'nan' THEN '' ELSE rr.seller_name END
        END) AS ajio
    FROM
    {{ ref('stg_price_parity_master') }} pp
LEFT JOIN
    {{ ref('stg_product_master') }} pm 
       ON pp.relaxo_sku = pm.sku_relaxo
LEFT JOIN
     {{ ref('stg_buybox_rating_and_reviews') }} rr 
    ON pm.channel_sku_id = rr.product_id

    WHERE rr.scraped_date IS NOT NULL
    GROUP BY
    pm.name,
    rr.scraped_date,
    pp.relaxo_sku,
    pm.sku_category,
    pm.sku_sub_category,
    pm.sku_size,
    pm.sku_gender
    ORDER BY
        relaxo_sku
) AS buybox_results;
 






 