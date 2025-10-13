WITH channel_data AS (
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        amazon_url AS product_url,
        'amazon' AS channel,
         week_start,
        amazon_avg_rating AS rating
    FROM  {{ ref("stg_weekly_wise_rating") }}
    UNION ALL
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        flipkart_url AS product_url,
        'flipkart' AS channel,
         week_start,
        flipkart_avg_rating AS rating
    FROM {{ ref("stg_weekly_wise_rating") }}
    UNION ALL
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        myntra_url AS product_url,
        'myntra' AS channel,
         week_start,
        myntra_avg_rating AS rating
    FROM {{ ref("stg_weekly_wise_rating") }}
    UNION ALL
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        ajio_url AS product_url,
        'ajio' AS channel,
         week_start,
        ajio_avg_rating AS rating
    FROM {{ ref("stg_weekly_wise_rating") }}
),
base AS (
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        product_url,
        channel,
        week_start,
        rating,
        date_format(week_start, '%m-%Y') AS date_month,
        row_number() OVER (PARTITION BY relaxo_sku, channel ORDER BY week_start) AS week_num
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