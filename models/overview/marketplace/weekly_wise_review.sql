WITH channel_data AS (
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        'amazon' AS channel,
         week_start,
        amazon_review_count AS review
    FROM  {{ ref("stg_weekly_wise_review") }}
    UNION ALL
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        'flipkart' AS channel,
         week_start,
        flipkart_review_count AS review
    FROM {{ ref("stg_weekly_wise_review") }}
    UNION ALL
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        'myntra' AS channel,
         week_start,
        myntra_review_count AS review
    FROM {{ ref("stg_weekly_wise_review") }}
    UNION ALL
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        'ajio' AS channel,
         week_start,
        ajio_review_count AS review
    FROM {{ ref("stg_weekly_wise_review") }}
),
base AS (
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        channel,
        week_start,
        review,
        date_format(week_start, '%m-%Y') AS date_month,
        row_number() OVER (PARTITION BY relaxo_sku, channel ORDER BY week_start) AS week_num
    FROM channel_data
)
SELECT
     relaxo_sku,
     sku_category,
     sku_sub_category,
    channel,
    date_month,
    MAX(CASE WHEN week_num = 1 THEN review END) AS week1,
    MAX(CASE WHEN week_num = 2 THEN review END) AS week2,
    MAX(CASE WHEN week_num = 3 THEN review END) AS week3,
    MAX(CASE WHEN week_num = 4 THEN review END) AS week4
FROM base
GROUP BY
     relaxo_sku,
     sku_category,
     sku_sub_category,
    channel,
    date_month
ORDER BY
    relaxo_sku,
    channel;