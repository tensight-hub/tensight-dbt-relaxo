/* SELECT
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

    {{ channel_reviews('source', 'no_of_reviews', 'amazon', 'amazon_review_count') }},
    {{ channel_reviews('source', 'no_of_reviews', 'flipkart', 'flipkart_review_count') }},
    {{ channel_reviews('source', 'no_of_reviews', 'myntra', 'myntra_review_count') }},
    {{ channel_reviews('source', 'no_of_reviews', 'Ajio', 'ajio_review_count') }},
     MAX(CASE WHEN lower(source) = 'amazon' THEN product_url END) AS amazon_url,
    MAX(CASE WHEN lower(source) = 'flipkart' THEN product_url END) AS flipkart_url,
    MAX(CASE WHEN lower(source) = 'myntra' THEN product_url END) AS myntra_url,
    MAX(CASE WHEN lower(source) = 'ajio' THEN product_url END) AS ajio_url


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
    sku_gender;


    
   