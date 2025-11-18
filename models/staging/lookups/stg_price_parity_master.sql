with

source as (

    select * from {{ source('lookups', 'price_parity_master') }}

),

renamed as (

    select  
    "ean /relaxo sku (which ever is common across the channels)" as relaxo_sku,
    "amazon asin" as amazon_asin,
    "flipkart fsn" as flipkart_fsn,
    "myntra id" as myntra_id,
    "ajio id" as ajio_id,
     tagging

      
        
    from source)

select * from renamed


