{{ config(
    partitioned_by=['stock_date'],
    insert_overwrite=True
) }}

select * from {{ ref('int_uniware_inventory_ledger_raw') }}