WITH channel_data AS (
    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        amazon_url AS product_url,   
        'amazon' AS channel,
        week_start,
        amazon_stock_availability AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}

    UNION ALL

    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        flipkart_url AS product_url, 
        'flipkart' AS channel,
        week_start,
        flipkart_stock_availability AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}

    UNION ALL

    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        myntra_url AS product_url,   
        'myntra' AS channel,
        week_start,
        myntra_stock_availability AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}

    UNION ALL

    SELECT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        ajio_url AS product_url,     
        'ajio' AS channel,
        week_start,
        ajio_stock_availability AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
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
        stock_availability,
        date_format(week_start, '%m-%Y') AS date_month,
        row_number() OVER (PARTITION BY relaxo_sku, channel ORDER BY week_start) AS week_num
    FROM channel_data
)

SELECT
    relaxo_sku,
    sku_category,
    sku_sub_category,
    image_url,
    product_url,                    
    channel,
    date_month,
    MAX(CASE WHEN week_num = 1 THEN stock_availability END) AS week1,
    MAX(CASE WHEN week_num = 2 THEN stock_availability END) AS week2,
    MAX(CASE WHEN week_num = 3 THEN stock_availability END) AS week3,
    MAX(CASE WHEN week_num = 4 THEN stock_availability END) AS week4
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
