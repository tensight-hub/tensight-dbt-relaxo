SELECT DISTINCT
    ur.created_date,
    ur.product_sku_code,
    ur.master_mapping_channel_entry,
    pm.sku_category,
    pm.sku_sub_category,
    pm.name,
    ur.Warehouse_channel_Return_Quantity,
    acr.channel_return_quantity
FROM {{ ref('int_uniware_returns') }} ur
LEFT JOIN {{ ref('int_allchannel_returns') }} acr
    ON ur.product_sku_code = acr.channel_sku_code
    AND LOWER(ur.master_mapping_channel_entry) = LOWER(acr.channel_name)
   AND ur.created_date = acr.channel_date
LEFT JOIN {{ ref('stg_product_master') }} pm
    ON pm.sku_relaxo = ur.product_sku_code
    AND LOWER(pm.channel) = LOWER(ur.master_mapping_channel_entry)
