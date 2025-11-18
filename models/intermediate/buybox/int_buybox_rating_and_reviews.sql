

SELECT
    pp.relaxo_sku,
    pp.tagging,
    pm.sku_relaxo,
    pm.channel_sku_id,
    pm.sku_category,
    pm.sku_sub_category,
    pm.sku_size,
    pm.sku_gender,
    pm.name,
    rr.source,
    rr.scraped_date,
    rr.seller_name,
    rr.is_sold_out,
    rr.product_price,
    rr.no_of_reviews,
    rr.avg_rating,
    rr.product_id,
    rr.product_url,
    rr.mrp,
    max(rr.image_url) as image_url
FROM {{ ref('stg_price_parity_master') }} pp
LEFT JOIN {{ ref('stg_product_master') }} pm
    ON pp.relaxo_sku = pm.sku_relaxo
    and lower(pm.tagging) = lower(pp.tagging)
LEFT JOIN {{ ref('stg_buybox_rating_and_reviews') }} rr
    ON pm.channel_sku_id = rr.product_id
WHERE rr.scraped_date IS NOT NULL
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
ORDER BY pp.relaxo_sku
