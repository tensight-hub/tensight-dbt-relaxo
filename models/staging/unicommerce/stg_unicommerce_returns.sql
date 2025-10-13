with

source as (

select  date, "sale order number","invoice number","channel entry","channel ledger","product name","product sku code",qty,"unit price",currency,"conversion rate",total,"customer name","shipping address name","shipping address line 1","shipping address line 2","shipping address city","shipping address state","shipping address country","shipping address pincode","shipping address phone","shipping provider","awb num",sales,"sales ledger","other charges","other charges ledger","other charges1","other charges ledger1","service tax","st ledger","discount ledger","discount amount",imei,godown,"dispatch date/cancellation date",narration,entity,"voucher type name ","tin no","original invoice date ","original invoice no","invoice channel created","channel state","channel_party gstin","customer gstin","billing party code","tax verification","gst registration type","rp code",irn,"acknowledgement number","product hsn code","return type","extracted_date", 'Bangalore1' as facility_code from {{ source('unicommerce', 'returns_uniware_bangalore') }}


UNION ALL 

select  date, "sale order number","invoice number","channel entry","channel ledger","product name","product sku code",qty,"unit price",currency,"conversion rate",total,"customer name","shipping address name","shipping address line 1","shipping address line 2","shipping address city","shipping address state","shipping address country","shipping address pincode","shipping address phone","shipping provider","awb num",sales,"sales ledger","other charges","other charges ledger","other charges1","other charges ledger1","service tax","st ledger","discount ledger","discount amount",imei,godown,"dispatch date/cancellation date",narration,entity,"voucher type name ","tin no","original invoice date ","original invoice no","invoice channel created","channel state","channel_party gstin","customer gstin","billing party code","tax verification","gst registration type","rp code",irn,"acknowledgement number","product hsn code","return type","extracted_date", 'Bhiwandi1' as facility_code from {{ source('unicommerce', 'returns_uniware_bhiwandi') }}

    
UNION ALL

select  date, "sale order number","invoice number","channel entry","channel ledger","product name","product sku code",qty,"unit price",currency,"conversion rate",total,"customer name","shipping address name","shipping address line 1","shipping address line 2","shipping address city","shipping address state","shipping address country","shipping address pincode","shipping address phone","shipping provider","awb num",sales,"sales ledger","other charges","other charges ledger","other charges1","other charges ledger1","service tax","st ledger","discount ledger","discount amount",imei,godown,"dispatch date/cancellation date",narration,entity,"voucher type name ","tin no","original invoice date ","original invoice no","invoice channel created","channel state","channel_party gstin","customer gstin","billing party code","tax verification","gst registration type","rp code",irn,"acknowledgement number","product hsn code","return type","extracted_date", 'Ghevra1' as facility_code from {{ source('unicommerce', 'returns_uniware_ghevra') }}


UNION ALL

select  date, "sale order number","invoice number","channel entry","channel ledger","product name","product sku code",qty,"unit price",currency,"conversion rate",total,"customer name","shipping address name","shipping address line 1","shipping address line 2","shipping address city","shipping address state","shipping address country","shipping address pincode","shipping address phone","shipping provider","awb num",sales,"sales ledger","other charges","other charges ledger","other charges1","other charges ledger1","service tax","st ledger","discount ledger","discount amount",imei,godown,"dispatch date/cancellation date",narration,entity,"voucher type name ","tin no","original invoice date ","original invoice no","invoice channel created","channel state","channel_party gstin","customer gstin","billing party code","tax verification","gst registration type","rp code",irn,"acknowledgement number","product hsn code","return type","extracted_date", 'Gurugram1' as facility_code from {{ source('unicommerce', 'returns_uniware_gurugram') }}


UNION ALL

select  date, "sale order number","invoice number","channel entry","channel ledger","product name","product sku code",qty,"unit price",currency,"conversion rate",total,"customer name","shipping address name","shipping address line 1","shipping address line 2","shipping address city","shipping address state","shipping address country","shipping address pincode","shipping address phone","shipping provider","awb num",sales,"sales ledger","other charges","other charges ledger","other charges1","other charges ledger1","service tax","st ledger","discount ledger","discount amount",imei,godown,"dispatch date/cancellation date",narration,entity,"voucher type name ","tin no","original invoice date ","original invoice no","invoice channel created","channel state","channel_party gstin","customer gstin","billing party code","tax verification","gst registration type","rp code",irn,"acknowledgement number","product hsn code","return type","extracted_date", 'Hyderabad1' as facility_code from {{ source('unicommerce', 'returns_uniware_hyderabad') }}


UNION ALL

select  date, "sale order number","invoice number","channel entry","channel ledger","product name","product sku code",qty,"unit price",currency,"conversion rate",total,"customer name","shipping address name","shipping address line 1","shipping address line 2","shipping address city","shipping address state","shipping address country","shipping address pincode","shipping address phone","shipping provider","awb num",sales,"sales ledger","other charges","other charges ledger","other charges1","other charges ledger1","service tax","st ledger","discount ledger","discount amount",imei,godown,"dispatch date/cancellation date",narration,entity,"voucher type name ","tin no","original invoice date ","original invoice no","invoice channel created","channel state","channel_party gstin","customer gstin","billing party code","tax verification","gst registration type","rp code",irn,"acknowledgement number","product hsn code","return type","extracted_date", 'Kolkata1' as facility_code from {{ source('unicommerce', 'returns_uniware_kolkata') }}



UNION ALL

select  date, "sale order number","invoice number","channel entry","channel ledger","product name","product sku code",qty,"unit price",currency,"conversion rate",total,"customer name","shipping address name","shipping address line 1","shipping address line 2","shipping address city","shipping address state","shipping address country","shipping address pincode","shipping address phone","shipping provider","awb num",sales,"sales ledger","other charges","other charges ledger","other charges1","other charges ledger1","service tax","st ledger","discount ledger","discount amount",imei,godown,"dispatch date/cancellation date",narration,entity,"voucher type name ","tin no","original invoice date ","original invoice no","invoice channel created","channel state","channel_party gstin","customer gstin","billing party code","tax verification","gst registration type","rp code",irn,"acknowledgement number","product hsn code","return type","extracted_date",'Mundkanew1' as facility_code from {{ source('unicommerce', 'returns_uniware_mundkha_new') }}

),

renamed as (

    select
    CAST(date_parse("date", '%d-%m-%Y') AS DATE) AS created_date,
    "sale order number" as sale_order_number,
    "invoice number" as invoice_number,
    case
    when lower("channel entry") like 'ajio%' then 'Ajio'
    when lower("channel entry") like 'amazon%' then 'Amazon'
    when lower("channel entry") like 'flipkart%' then 'Flipkart'
    when lower("channel entry") like 'jiomart3p%' then 'JioMart 3P'
    when lower("channel entry") like 'jiomart%' then 'JioMart'
    when lower("channel entry") like 'meesho%' then 'Meesho'
    when lower("channel entry") like 'myntrappmp%' 
       or lower("channel entry") like 'myntra%' then 'Myntra'
    when lower("channel entry") = 'shopify' then 'Shopify'
    when lower("channel entry") like 'snapdeal%' then 'Snapdeal'
    when lower("channel entry") like 'snapmint%' then 'Snapmint'
    when lower("channel entry") like 'tatacliq%' then 'TataCliq'
    else "channel entry"
    end as master_mapping_channel_entry,
    "channel ledger" as channel_ledger,
    "product name" as product_name,
    CASE
        WHEN substr("product sku code", 1, 3) IN ('A1Z', 'A1M')
            THEN substr("product sku code", 1, 19)
        ELSE substr("product sku code", 1, 18)
    END AS product_sku_code,
    qty as  Warehouse_Return_Quantity,
    "unit price" as unit_price,
    currency,
    "conversion rate" as conversion_rate,
    total,
    "customer name" as customer_name,
    "shipping address name" as shipping_address_name,
    "shipping address line 1" as shipping_address_line_1,
    "shipping address line 2" as shipping_address_line_2,
    "shipping address city" as shipping_address_city,
    "shipping address state" as shipping_address_state,
    "shipping address country" as shipping_address_country,
    "shipping address pincode" as shipping_address_pincode,
    "shipping address phone" as shipping_address_phone,
    "shipping provider" as shipping_provider,
    "awb num" as awb_num,
    sales,
    "sales ledger"  as sales_ledger,
    "other charges" as other_charges,
    "other charges ledger" as other_charges_ledger,
    "other charges1" as other_charges1,
    "other charges ledger1" as other_charges_ledger1,
    "service tax" as service_tax,
    "st ledger" as st_ledger,
    "discount ledger" as discount_ledger,
    "discount amount" as discount_amount,
    imei,
    godown,
    CAST(date_parse("dispatch date/cancellation date", '%Y-%m-%d %H:%i:%s') AS DATE) AS dispatch_date,
    narration,
    entity,
    "voucher type name " as voucher_type_name,
    "tin no" as tin_no,
     CAST(date_parse("original invoice date ", '%d-%m-%Y') AS DATE) AS original_invoice_date,
    "original invoice no" as original_invoice_no,
    CAST(CASE WHEN lower("invoice channel created") = 'nan' THEN NULL
    ELSE date_parse("invoice channel created", '%d-%m-%Y') END AS DATE ) AS invoice_created_date,
    "channel state" as channel_state,
    "channel_party gstin" as channel_party_gstin,
    "customer gstin" as customer_gstin,
    "billing party code" as billing_party_code,
    "tax verification"  as tax_verification,
    "gst registration type" as gst_registration_type,
    "rp code" as rp_code,
    irn,
    "acknowledgement number" as acknowledgement_number,
    "product hsn code" as product_hsn_code,
    "return type" as return_type,
    CAST("extracted_date" AS DATE) AS extracted_date,
    facility_code


   
  from source

),

final as (

    select
        *, 
        CONCAT(product_sku_code, '_', sale_order_number) AS channel_uniware_order_id 
    from renamed

)

select * from final