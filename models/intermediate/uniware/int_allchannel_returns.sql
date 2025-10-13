SELECT
    channel_sku_code,
    channel_date,
    channel_name,
    order_id,
    channel_sku_order_id,
    SUM(CAST(CAST(channel_return_quantity AS double) AS bigint)) AS channel_return_quantity
FROM (
    SELECT
        channel_sku_code,
        channel_date,
        channel_return_quantity,
        channel_name,
        order_id,
        channel_sku_order_id 
    FROM {{ ref('stg_ajio_returns') }}

    UNION ALL

    SELECT
        channel_sku_code,
        channel_date,
        channel_return_quantity,
        channel_name,
        order_id,
        channel_sku_order_id 
    FROM {{ ref('stg_myntra_returns') }}

    UNION ALL

    SELECT
        channel_sku_code,
        channel_date,
        channel_return_quantity,
        channel_name,
        order_id,
        channel_sku_order_id 
    FROM {{ ref('stg_flipkart_returns') }}
)
GROUP BY 1,2,3,4,5
