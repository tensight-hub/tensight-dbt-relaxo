with

source as (

    select * from {{ source('lookups', 'product_master') }}

),

renamed as (

    select  
       channel,
       "relaxo sku" as relaxo_sku,
        sku as brand_sku_id,
        ean as brand_ean,
       "channel id (asin/fsn)" as channel_sku_id,	
        category as sku_category,
        "sub category" as sku_sub_category,
        size as sku_size,
        gender as sku_gender,
        "sub-brand" as sub_brand

        
    from source)

select * from renamed


