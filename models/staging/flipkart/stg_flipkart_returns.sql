with

source as (

    select * from {{ source('flipkart', 'returns_flipkart') }}

),

renamed as (

    select  
        location_id,
       	order_id,
        order_item_id,	
        return_id,	
        tracking_id,
        shipment_id,
        replacement_order_item_id,
        CASE
        WHEN substr(sku, 1, 3) IN ('A1Z', 'A1M')THEN substr(sku, 1, 19)
        ELSE substr(sku, 1, 18)
        END AS channel_sku_code,
        fsn,
        product,
        total_price,
        quantity as channel_return_quantity,
        ff_type,
        case when return_requested_date = 'nan' then null else cast(date_parse(return_requested_date, '%e %M, %Y') as date) end as channel_date,
        case when return_approval_date = 'nan' then null else cast(date_parse(return_approval_date, '%e %M, %Y') as date) end as return_approval_date,
        case when completed_date = 'nan' then null else cast(date_parse(completed_date, '%e %M, %Y') as date) end as completed_date,
        case when out_for_delivery_date = 'nan' then null else cast(date_parse(out_for_delivery_date, '%e %M, %Y') as date) end as out_for_delivery_date,
        case when return_delivery_promise_date = 'nan' then null else cast(date_parse(return_delivery_promise_date, '%e %M, %Y') as date) end as return_delivery_promise_date,
        case when picked_up_date = 'nan' then null else cast(date_parse(picked_up_date, '%e %M, %Y') as date) end as picked_up_date,
        shipment_type,
        return_status,
        completion_status,
        return_type,
        return_reason,
       "return_sub-reason" as return_sub_reason,
        comments,
        vendor_name,
        location_name,
        'Flipkart' AS channel_name


        from source)

select * from renamed


