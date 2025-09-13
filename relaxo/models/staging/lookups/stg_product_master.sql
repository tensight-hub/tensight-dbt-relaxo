with

source as (

    select * from {{ source('lookups', 'product_master') }}

),

renamed as (

    select  
       channel,
       "relaxo sku" as sku_relaxo,
        sku as brand_sku_id,
        "item.sku code" as sku_item_code,
        ean as brand_ean,
       "channel id (asin/fsn)" as channel_sku_id,	
        name,
        lower(category) as sku_category,
        lower("sub category") as sku_sub_category,
        size as sku_size,
        lower(gender) as sku_gender,
        "sub-brand" as sub_brand,
        "article+colour" as article_colour,
        upper,   
        sole
        


        
    from source)

select * from renamed


