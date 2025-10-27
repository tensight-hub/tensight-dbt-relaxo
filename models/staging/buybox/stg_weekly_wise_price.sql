 with source as (
  select *
  from {{ source('buybox_overview', 'price_across_channels_latest_month') }}
),
renamed as (
  select distinct  

scraped_date,
date_trunc('week', date_parse(scraped_date, '%Y-%m-%d')) AS week_start,
relaxo_sku,
sku_category,
sku_sub_category,
sku_size,
sku_gender,
amazon_price,
flipkart_price,
myntra_price,
ajio_price,
image_url,
max(amazon_url)  OVER () AS amazon_url,
max(flipkart_url) OVER () AS flipkart_url,
max(myntra_url)   OVER () AS myntra_url,
max(ajio_url)     OVER () AS ajio_url


from source



)
select * from renamed;



