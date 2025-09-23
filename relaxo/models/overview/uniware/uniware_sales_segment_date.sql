{{ config(
    partitioned_by=['invoice_created'],
    insert_overwrite=True
) }}

select 
master_mapping_channel_name as platform,
sum(total_amount) as order_amount,
sum(total_quantity) as order_quantity,
sum(returned_amount) as returned_amount,
sum(returned_quantity) as returned_quantity,
sum(net_amount) as net_amount,
sum(net_quantity) as net_quantity,
invoice_created
from 
{{ ref('int_uniware_net_with_master_attributes') }}
group by 1,8