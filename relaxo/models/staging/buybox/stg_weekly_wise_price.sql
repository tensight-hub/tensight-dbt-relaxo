with source as (
  select *
  from {{ source('buybox_overview', 'price_across_channels') }}
),
renamed as (
  select  


date_trunc('week', date_parse(scraped_date, '%Y-%m-%d')) AS week_start,
relaxo_sku,
sku_category,
sku_sub_category,
name,
sku_size,
sku_gender,
amazon_price,
flipkart_price,
myntra_price,
ajio_price   

from source


)
select * from renamed;
