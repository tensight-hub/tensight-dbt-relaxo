select latest_inv.*, 
COALESCE(drr_calc.drr_30d,0) as quantity_30d_drr,
cast(latest_inv.total/drr_calc.drr_30d as int) as doh_30d
from {{ ref('uniware_inventory_segment_latest') }} latest_inv 
left join 
{{ ref('uniware_sales_drr_calc') }} drr_calc
on latest_inv.facility_code = drr_calc.facility_code
and latest_inv.sku_code = drr_calc.sku_id