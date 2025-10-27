WITH channel_data AS (
    SELECT
        scraped_date,
        relaxo_sku,
        sku_category,
        sku_sub_category,
        amazon_url AS product_url,   
        'amazon' AS channel,
        week_start,
        max(image_url) as image_url,
        max(amazon_price)  AS price
    FROM {{ ref("stg_weekly_wise_price") }}
    where amazon_price is not null
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
        max(flipkart_price) AS price
    FROM {{ ref("stg_weekly_wise_price") }}
        WHERE flipkart_price is not null
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
        max(myntra_price) AS price
    FROM {{ ref("stg_weekly_wise_price") }}
        WHERE myntra_price is not null
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
        max(ajio_price) AS price
    FROM {{ ref("stg_weekly_wise_price") }}
        WHERE ajio_price is not null
    group by 1,2,3,4,5,6,7

),

base AS (
    SELECT DISTINCT
        relaxo_sku,
        sku_category,
        sku_sub_category,
        image_url,
        product_url,                  
        channel,
        scraped_date,
        week_start,
        price,
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
    MAX(CASE WHEN week_num = 1 THEN price END) AS week1,
    MAX(CASE WHEN week_num = 2 THEN price END) AS week2,
    MAX(CASE WHEN week_num = 3 THEN price END) AS week3,
    MAX(CASE WHEN week_num = 4 THEN price END) AS week4
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
