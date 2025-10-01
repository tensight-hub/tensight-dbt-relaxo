select * from
(select distinct pp.relaxo_sku,
units_sold as unit_sold,
uni_sales.master_mapping_channel_name,
rr.avg_rating,
(CASE
        WHEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN NULL ELSE rr.avg_rating END AS DECIMAL(10,2)) > 4
        THEN 'Rating > 4'
        WHEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN NULL ELSE rr.avg_rating END AS DECIMAL(10,2)) BETWEEN 2 AND 4
        THEN 'rating>2and<4'
         WHEN CAST(CASE WHEN lower(rr.avg_rating) = 'nan' THEN NULL ELSE rr.avg_rating END AS DECIMAL(10,2)) <2
        THEN  'rating<2'
       ELSE 'No Rating'
    END) AS rating_category,
rr.image_url
from  {{ ref('stg_price_parity_master') }} pp
left join (
select item_sku_code , master_mapping_channel_name, sum(units_sold) units_sold from
(select *,count(*) units_sold from {{ ref('stg_unicommerce_orders') }}
group by 1,2,3,4,5,6,7,8,9,10,
11,12,13,14,15,16,17,18,19,20,
21,22,23,24,25,26,27,28,29,30,
31,32,33,34,35,36,37,38,39,40,
41,42,43,44,45,46,47,48,49,50,
51,52)
where lower(master_mapping_channel_name) in ('ajio','myntra','flipkart','amazon')
group by 1,2
) uni_sales
on pp.relaxo_sku = uni_sales.item_sku_code
left join {{ ref('stg_product_master') }} pm
 on  uni_sales.item_sku_code = pm.sku_relaxo
and lower(uni_sales.master_mapping_channel_name) = lower(pm.channel)
left join {{ ref('stg_buybox_rating_and_reviews') }} rr
on rr.product_id = pm.channel_sku_id
and rr.source = pm.channel)