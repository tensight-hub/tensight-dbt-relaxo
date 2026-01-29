with

source as (

select "sku code", "item name", shelf, batch, batchstatus, type, "total (stock on hand)", "available (atp)", "blocked (committed)", "not found", size, color, brand, updated, "inventory allocation", "inventory sync", "sku mixing", "shelf on hold", image, "pendency on all channels", "total inventory", "item type inventory id", stock_date, 'Bangalore1' as facility_code from {{ source('unicommerce', 'uniware_inventory_banglore') }}

UNION ALL 

select "sku code", "item name", shelf, batch, batchstatus, type, "total (stock on hand)", "available (atp)", "blocked (committed)", "not found", size, color, brand, updated, "inventory allocation", "inventory sync", "sku mixing", "shelf on hold", image, "pendency on all channels", "total inventory", "item type inventory id", stock_date, 'Bhiwandi1' as facility_code from {{ source('unicommerce', 'uniware_inventory_bhiwandi') }}

UNION ALL
  
select "sku code", "item name", shelf, batch, batchstatus, type, "total (stock on hand)", "available (atp)", "blocked (committed)", "not found", size, color, brand, updated, "inventory allocation", "inventory sync", "sku mixing", "shelf on hold", image, "pendency on all channels", "total inventory", "item type inventory id", stock_date, 'Ghevra1' as facility_code from {{ source('unicommerce', 'uniware_inventory_ghevra') }}

UNION ALL 

select "sku code", "item name", shelf, batch, batchstatus, type, "total (stock on hand)", "available (atp)", "blocked (committed)", "not found", size, color, brand, updated, "inventory allocation", "inventory sync", "sku mixing", "shelf on hold", image, "pendency on all channels", "total inventory", "item type inventory id", stock_date, 'Gurugram1' as facility_code from {{ source('unicommerce', 'uniware_inventory_gurgram') }}

UNION ALL

select "sku code", "item name", shelf, batch, batchstatus, type, "total (stock on hand)", "available (atp)", "blocked (committed)", "not found", size, color, brand, updated, "inventory allocation", "inventory sync", "sku mixing", "shelf on hold", image, "pendency on all channels", "total inventory", "item type inventory id", stock_date, 'Hyderabad1' as facility_code from {{ source('unicommerce', 'uniware_inventory_hyderabad') }}

UNION ALL

select "sku code", "item name", shelf, batch, batchstatus, type, "total (stock on hand)", "available (atp)", "blocked (committed)", "not found", size, color, brand, updated, "inventory allocation", "inventory sync", "sku mixing", "shelf on hold", image, "pendency on all channels", "total inventory", "item type inventory id", stock_date, 'Kolkata1' as facility_code from {{ source('unicommerce', 'uniware_inventory_kolkata') }}

UNION ALL

select "sku code", "item name", shelf, batch, batchstatus, type, "total (stock on hand)", "available (atp)", "blocked (committed)", "not found", size, color, brand, updated, "inventory allocation", "inventory sync", "sku mixing", "shelf on hold", image, "pendency on all channels", "total inventory", "item type inventory id", stock_date, 'Mundkanew1' as facility_code from {{ source('unicommerce', 'uniware_inventory_mundkanew') }}

),

renamed as (

    select
     CASE
        WHEN substr("sku code", 1, 3) IN ('A1Z', 'A1M')
            THEN substr("sku code", 1, 19)
        ELSE substr("sku code", 1, 18)
    END AS sku_code,
    "sku code" as sku_code_raw,
    "item name" as item_name,
    shelf,
    batch,
    batchstatus,
    type,
    "total (stock on hand)" as total_stock_on_hand,
    "available (atp)" as available_atp,
    "blocked (committed)" as blocked_committed, 
    "not found" as not_found,
    size,
    color,
    brand,
    updated, 
    "inventory allocation" as inventory_allocation, 
    "inventory sync" as inventory_sync, 
    "sku mixing" as sku_mixing,
    "shelf on hold" as shelf_on_hold,
    image,
    "pendency on all channels" as dependency_on_all_channels, 
    "total inventory" as total_inventory, 
    "item type inventory id" as item_type_inventory_id,
    stock_date, 
    facility_code

    from source)

select * from renamed