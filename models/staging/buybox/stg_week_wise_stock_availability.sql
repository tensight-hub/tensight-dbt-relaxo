with source as (
  select *
  from {{ source('buybox_overview', 'product_availability_latest_month') }}
),
renamed as (
  select distinct
scraped_date,
--date_trunc('week', date_parse(scraped_date, '%Y-%m-%d')) AS week_start,
date_add('day', -1, date_trunc('week', date_add('day', 1, date_parse(scraped_date, '%Y-%m-%d')))) AS week_start,
relaxo_sku,
sku_category,
sku_sub_category,
name,
sku_size,
sku_gender,
amazon_stock_availability,
flipkart_stock_availability,
myntra_stock_availability,
ajio_stock_availability,
image_url,
max(amazon_url)  OVER () AS amazon_url,
max(flipkart_url) OVER () AS flipkart_url,
max(myntra_url)   OVER () AS myntra_url,
max(ajio_url)     OVER () AS ajio_url
    
    FROM source

    )
select * from renamed;