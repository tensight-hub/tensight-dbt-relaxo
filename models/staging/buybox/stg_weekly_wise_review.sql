with source as (
  select *
  from {{ source('buybox_overview', 'review_across_channels_latest_month') }}
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
amazon_review_count,
flipkart_review_count,
myntra_review_count,
ajio_review_count,
image_url,
max(amazon_url)  OVER () AS amazon_url,
max(flipkart_url) OVER () AS flipkart_url,
max(myntra_url)   OVER () AS myntra_url,
max(ajio_url)     OVER () AS ajio_url
    
    FROM source

    )
select * from renamed;