SELECT DISTINCT
    ur.created_date,
    acr.channel_date,
    acr.channel_sku_code,
    acr.channel_name,
    acr.order_id,
    pm.sku_category,
    pm.sku_sub_category,
    pm.name,
    ur.channel_uniware_order_id,
    acr.channel_sku_order_id,
    ur.Warehouse_channel_Return_Quantity,
    acr.channel_return_quantity
    FROM {{ ref('int_allchannel_returns') }} acr
    left join {{ ref('int_uniware_returns') }} ur
    on LOWER(ur.master_mapping_channel_entry) = LOWER(acr.channel_name)

  AND  ur.channel_uniware_order_id = acr.channel_sku_order_id 

LEFT JOIN {{ ref('stg_product_master') }} pm
    ON pm.sku_relaxo = ur.product_sku_code
    AND LOWER(pm.channel) = LOWER(ur.master_mapping_channel_entry)
   


  