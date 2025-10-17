select * from {{ ref('stock_availability_across_channels') }} 
where scraped_date = 
(select max(scraped_date) as scraped_date from {{ ref('stock_availability_across_channels') }})



