/*WITH channel_data AS (
    SELECT
        scraped_date,
        relaxo_sku,
        sku_category,
        sku_sub_category,
        amazon_url AS product_url,   
        'amazon' AS channel,
        week_start,
        max(image_url) as image_url,
        max(amazon_stock_availability) AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
        where amazon_stock_availability is not null
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
        max(image_url) as image_url,
        max(flipkart_stock_availability) AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
        WHERE flipkart_stock_availability is not null
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
        max(image_url) as image_url,
        max(myntra_stock_availability) AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
        WHERE myntra_stock_availability is not null
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
        max(image_url) as image_url,
        max(ajio_stock_availability) AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
        WHERE ajio_stock_availability is not null
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
        stock_availability,
        date_format(week_start, '%m-%Y') AS date_month,
        row_number() OVER (PARTITION BY relaxo_sku, channel ORDER BY scraped_date) AS week_num
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
*/


WITH channel_data AS (
    SELECT
        scraped_date,
        relaxo_sku,
        tagging,
        sku_category,
        sku_sub_category,
        amazon_url AS product_url,   
        'amazon' AS channel,
        week_start,
        max(image_url) as image_url,
        max(amazon_stock_availability) AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
        where amazon_stock_availability is not null
    group by 1,2,3,4,5,6,7,8

    UNION ALL

    SELECT
        scraped_date,
        relaxo_sku,
        tagging,
        sku_category,
        sku_sub_category,
        flipkart_url AS product_url, 
        'flipkart' AS channel,
        week_start,
        max(image_url) as image_url,
        max(flipkart_stock_availability) AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
        WHERE flipkart_stock_availability is not null
    group by 1,2,3,4,5,6,7,8

    UNION ALL

    SELECT
        scraped_date,
        relaxo_sku,
        tagging,
        sku_category,
        sku_sub_category,
        myntra_url AS product_url,   
        'myntra' AS channel,
        week_start,
        max(image_url) as image_url,
        max(myntra_stock_availability) AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
        WHERE myntra_stock_availability is not null
    group by 1,2,3,4,5,6,7,8

    UNION ALL

    SELECT
        scraped_date,
        relaxo_sku,
        tagging,
        sku_category,
        sku_sub_category,
        ajio_url AS product_url,     
        'ajio' AS channel,
        week_start,
        max(image_url) as image_url,
        max(ajio_stock_availability) AS stock_availability
    FROM {{ ref("stg_week_wise_stock_availability") }}
        WHERE ajio_stock_availability is not null
    group by 1,2,3,4,5,6,7,8
),

base AS (
    SELECT
        relaxo_sku,
        tagging,
        sku_category,
        sku_sub_category,
        image_url,
        product_url,                  
        channel,
        scraped_date,   
        week_start,
        stock_availability,
        date_format(week_start, '%m-%Y') AS date_month,
         EXTRACT(week FROM week_start) - EXTRACT(week FROM DATE_TRUNC('month', week_start)) + 1 AS calendar_week_num
    FROM channel_data
)

SELECT
    relaxo_sku,
    tagging,
    sku_category,
    sku_sub_category,
    image_url,
    product_url,                    
    channel,
    date_month,
    MAX(CASE WHEN calendar_week_num = 1 THEN stock_availability END) AS week1,
    MAX(CASE WHEN calendar_week_num = 2 THEN stock_availability END) AS week2,
    MAX(CASE WHEN calendar_week_num = 3 THEN stock_availability END) AS week3,
    MAX(CASE WHEN calendar_week_num = 4 THEN stock_availability END) AS week4
FROM base
GROUP BY
    relaxo_sku,
    tagging,
    sku_category,
    sku_sub_category,
    image_url,
    product_url,                    
    channel,
    date_month
ORDER BY
    relaxo_sku,
    channel;
