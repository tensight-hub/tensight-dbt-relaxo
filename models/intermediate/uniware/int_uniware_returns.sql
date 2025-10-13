SELECT
    created_date,
    product_sku_code,
    master_mapping_channel_entry,
    facility_code,
    sale_order_number,
    channel_uniware_order_id,
    SUM(CAST(CAST(warehouse_return_quantity AS double) AS bigint)) AS Warehouse_Channel_Return_Quantity
FROM {{ ref('stg_unicommerce_returns') }}
where lower(master_mapping_channel_entry) in ('ajio','myntra','flipkart','amazon')
GROUP BY 1,2,3,4,5,6