with source as (
  select *
  from {{ source('unicommerce', 'rating_and_reviews') }}
),
renamed as (
  select    


"sr.no" as sr_no,
 product_id,
 catalog_name,
 catalog_id,
 source,
 DATE_FORMAT(date_parse(scraped_date, '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d') AS scraped_date,
 product_name,
 image_url,
 product_price,
 is_sold_out,
 discount,
 mrp,
 product_url,
 number_of_ratings,
 avg_rating,
"no of reviews" as no_of_reviews,
 images,
 product_details,
 specifications,
"seller name" as seller_name,
 brand,
 product_code
  from source
)
select * from renamed;
