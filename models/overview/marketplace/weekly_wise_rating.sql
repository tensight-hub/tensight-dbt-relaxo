WITH channel_data AS (
    SELECT
        scraped_date,
        relaxo_sku,
        sku_category,
        sku_sub_category,
        amazon_url AS product_url,
        'amazon' AS channel,
         week_start,
         max(image_url) image_url,
        max(amazon_avg_rating) AS rating
    FROM  {{ ref("stg_weekly_wise_rating") }}
    WHERE amazon_avg_rating IS NOT NULL
    group by 1,2,3,4,5,6,7
    
    UNION ALL
    SELECT
        scraped_date,
        relaxo_sku,
        sku_category,
        sku_sub_category,
        flipkart_url AS product_url,
        'flipkart' AS channel,
         week_start,
        max(image_url) image_url,
        max(flipkart_avg_rating) AS rating
    FROM {{ ref("stg_weekly_wise_rating") }}
    WHERE flipkart_avg_rating IS NOT NULL
    group by 1,2,3,4,5,6,7
    
    UNION ALL
    SELECT
        scraped_date,
        relaxo_sku,
        sku_category,
        sku_sub_category,
        myntra_url AS product_url,
        'myntra' AS channel,
         week_start,
         max(image_url) image_url,
        max(myntra_avg_rating) AS rating
    FROM {{ ref("stg_weekly_wise_rating") }}
    WHERE myntra_avg_rating IS NOT NULL
    group by 1,2,3,4,5,6,7
    
    UNION ALL
    SELECT
        scraped_date,
        relaxo_sku,
        sku_category,
        sku_sub_category,
        ajio_url AS product_url,
        'ajio' AS channel,
         week_start,
         max(image_url) image_url,
        max(ajio_avg_rating) AS rating
        FROM {{ ref("stg_weekly_wise_rating") }}
    WHERE ajio_avg_rating IS NOT NULL
    group by 1,2,3,4,5,6,7
    
),
base AS (
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        product_url,
        channel,
        scraped_date,
        week_start,
        rating,
        date_format(week_start, '%m-%Y') AS date_month,
        row_number() OVER (PARTITION BY relaxo_sku, channel ORDER BY scraped_date) AS week_num
    FROM channel_data
)
SELECT
     relaxo_sku,
     sku_category,
     sku_sub_category,
    image_url,
    channel,
    product_url,
    date_month,
    MAX(CASE WHEN week_num = 1 THEN rating END) AS week1,
    MAX(CASE WHEN week_num = 2 THEN rating END) AS week2,
    MAX(CASE WHEN week_num = 3 THEN rating END) AS week3,
    MAX(CASE WHEN week_num = 4 THEN rating END) AS week4
FROM base
GROUP BY
     relaxo_sku,
     sku_category,
     sku_sub_category,
        image_url,
        product_url,
    channel,
    date_month
ORDER BY
    relaxo_sku,
    channel;