




with source as (

    select * 
    from {{ source('unicommerce_facility_wise', 'uniware_sales_kolkata') }}

),

renamed as (

    select
        "sale order code" as sale_order_code,
        "invoice code" as invoice_code,
        "display order code" as display_order_code,
        "channel product id" as channel_product_id,
        "sale order item code" as sale_order_item_code,
        "seller sku code" as seller_sku_code,
        "parent sale order code" as parent_sale_order_code,
        "item code" as item_code,
        CASE
        WHEN substr("item sku code" , 1, 3) IN ('A1Z', 'A1M')
            THEN substr("item sku code" , 1, 19)
        ELSE substr("item sku code" , 1, 18)
        END AS item_sku_code,
        "reverse pickup code" as reverse_pickup_code,

        -- product info
        "sku name" as sku_name,
        "item type brand" as item_type_brand,
        "item type name" as item_type_name,
        "item type color" as item_type_color,
        "item type size" as item_type_size,
        "item details" as item_details,
        category,

        -- order status
        cod,
        "on hold" as on_hold,
        "sale order item status" as sale_order_item_status,
        "sale order status" as sale_order_status,
        "shipping provider" as shipping_provider,
        "shipping arranged by" as shipping_arranged_by,
        "shipping package status code" as shipping_package_status_code,
        "shipping courier status" as shipping_courier_status,
        "shipping tracking status" as shipping_tracking_status,
        "shipping address city" as buyer_city,
        "shipping address state" as buyer_state,
        "shipping address pincode" as buyer_pincode,
        "reverse pickup reason" as reverse_pickup_reason,
        "tracking number" as tracking_number,

        -- prices
        case when mrp = 'nan' then 0 else cast(mrp as double) end as mrp,
        case when "selling price" = 'nan' then 0 else cast("selling price" as double) end as selling_price,
        case when "total price" = 'nan' then 0 else cast("total price" as double) end as total_price,
        case when subtotal = 'nan' then 0 else cast(subtotal as double) end as subtotal,
        case when discount = 'nan' then 0 else cast(discount as double) end as discount,
        case when "shipping charges" = 'nan' then 0 else cast("shipping charges" as double) end as shipping_charges,

        -- dates
        case when created = 'nan' then null else cast(date_parse(created, '%Y-%m-%d %H:%i:%s') as date) end as created_date,
        updated,
        case when "dispatch date" = 'nan' then null else cast(date_parse("dispatch date", '%Y-%m-%d %H:%i:%s') as date) end as dispatch_date,
        case when "return date" = 'nan' then null else cast(date_parse("return date", '%Y-%m-%d %H:%i:%s') as date) end as return_date,
        case when "delivery time" = 'nan' then null else cast(date_parse("delivery time", '%Y-%m-%d %H:%i:%s') as date) end as delivery_time,
        case when "reverse pickup created date" = 'nan' then null else cast(date_parse("reverse pickup created date", '%Y-%m-%d %H:%i:%s') as date) end as reverse_pickup_created_date,
        cast(date_parse("order date as dd/mm/yyyy hh:mm:ss", '%Y-%m-%d %H:%i:%s') as date) as order_date,

        case when "fulfillment tat" = 'nan' then null else cast(date_parse("fulfillment tat", '%Y-%m-%d %H:%i:%s') as date) end as fulfillment_tat,
        case when "invoice created" = 'nan' then null else cast(date_parse("invoice created", '%Y-%m-%d %H:%i:%s') as date) end as invoice_created,

        -- report extract date
        cast(extracted_date as date) as report_extracted_date,

        -- facility + payment
        facility,
        "payment instrument" as payment_instrument,
        "channel name" as channel_name,

        -- channel mapping
        case
            when lower("channel name") like 'ajio%' then 'Ajio'
            when lower("channel name") like 'flipkart%' then 'Flipkart'
            when lower("channel name") like 'jiomart%' then 'Jiomart'
            when lower("channel name") like 'meesho%' then 'Meesho'
            when lower("channel name") like 'myntrappmp%' or lower("channel name") like 'myntra%' then 'Myntra'
            when lower("channel name") = 'shopify' then 'Shopify'
            when lower("channel name") like 'snapmint%' then 'Snapmint'
            else "channel name"
        end as master_mapping_channel_name,
        'Kolkata1' as facility_code
        

    

    from source
)

select * from renamed;

