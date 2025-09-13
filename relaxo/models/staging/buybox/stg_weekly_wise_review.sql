with source as (
  select *
  from {{ source('buybox_overview', 'review_across_channels') }}
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
amazon_review_count,
flipkart_review_count,
myntra_review_count,
ajio_review_count

from source


)
select * from renamed;
