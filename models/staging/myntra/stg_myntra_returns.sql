with

source as (

    select * from {{ source('myntra', 'returns_myntra') }}

),

renamed as (

    select  
       
        seller_id,
       	warehouse_id,
        model,
        myntra_sku_code,
        CASE
        WHEN substr(seller_sku_code, 1, 3) IN ('A1Z', 'A1M')THEN substr(seller_sku_code, 1, 19)
        ELSE substr(seller_sku_code, 1, 18)
        END AS channel_sku_code,
        style_id,
        sku_id,
        brand,
    case 
        when cast(order_created_date as varchar) = 'nan' then null 
        else cast(date_parse(cast(order_created_date as varchar), '%Y-%m-%d') as date) 
    end as picked_up_date,
    case 
        when cast(inscanned_on as varchar) = 'nan' then null 
        else cast(date_parse(cast(inscanned_on as varchar), '%Y-%m-%d') as date) 
    end as inscanned_on,
    case 
        when cast(fmpu_date as varchar) = 'nan' then null 
        else cast(date_parse(cast(fmpu_date as varchar), '%Y-%m-%d') as date) 
    end as fmpu_date,
    case 
        when cast(order_delivered_date as varchar) = 'nan' then null 
        else cast(date_parse(cast(order_delivered_date as varchar), '%Y-%m-%d') as date) 
    end as order_delivered_date,
    case 
        when cast(return_created_date as varchar) = 'nan' then null 
        else cast(date_parse(cast(return_created_date as varchar), '%Y-%m-%d') as date) 
    end as channel_date,
    case 
        when cast(refunded_date as varchar) = 'nan' then null 
        else cast(date_parse(cast(refunded_date as varchar), '%Y-%m-%d') as date) 
    end as refunded_date,
    case 
        when cast(order_rto_date as varchar) = 'nan' then null 
        else cast(date_parse(cast(order_rto_date as varchar), '%Y-%m-%d') as date) 
    end as order_rto_date,
        is_refunded,
        exchange_id,
        order_id,
        order_group_id,
        order_line_id,
        seller_order_id,
        type,
        status,
        store_packet_id,
        seller_packet_id_fk,
        quantity as channel_return_quantity,
        return_id,
        return_mode,
        return_reason,
        return_status,
        forward_tracking_number,
        return_tracking_number,
        master_bag_id,
        lmdo_status,
        lmdo_last_modified_on,
        gatepass_id,
        gatepass_status,
        gatepass_type,
        gatepass_lastmodified,
         'Myntra' AS channel_name

        from source)

select * from renamed


