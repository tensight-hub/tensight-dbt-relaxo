with

source as (

    select * from {{ source('ajio', 'returns_ajio') }}

),

renamed as (

    select  
       "return order number" as return_order_no,
       	jiocode,
        CASE
        WHEN substr("seller sku", 1, 3) IN ('A1Z', 'A1M')THEN substr("seller sku", 1, 19)
        ELSE substr("seller sku", 1, 18)
        END AS channel_sku_code,
        ean,
        hsn,	
     "return status" as return_status,
       case when "return created date" = 'nan' then null else cast(date_parse("return created date", '%a %b %d %H:%i:%s IST %Y') as date) end as channel_date,
       "return qty" as channel_return_quantity,
       "return shipment id" as return_shipment_id,
       "return carrier name" as return_carrier_name,
       "return awb no" as return_awb_no,
       "3pl delivery status" as delivery_status_3pl,
       case when "return delivered date" = 'nan' then null else cast(date_parse("return delivered date", '%a %b %d %H:%i:%s IST %Y') as date) end as return_delivered_date,
       "delivery challan no" as delivery_challan_no,
        case when "delivery challan date" = 'nan' then null else cast(date_parse("delivery challan date", '%a %b %d %H:%i:%s IST %Y') as date) end as delivery_challan_date,
        "delivery challan posting status" as delivery_challan_posting_status,
        "return value" as return_value,
        "qc completion date" as qc_completion_date,
        "disposition" as disposition,
        "qc reason coding" as qc_reason_coding,
        "return type" as return_type,
        "ret doc no" as return_doc_no,
        "cust return reason" as customer_return_reason,
        "credit note number" as credit_note_no,
        case when "credit note generation date" = 'nan' then null else cast(date_parse("credit note generation date", '%a %b %d %H:%i:%s IST %Y') as date) end as credit_note_generation_date,
        case when "credit note acceptance date" = 'nan' then null else cast(date_parse("credit note acceptance date", '%a %b %d %H:%i:%s IST %Y') as date) end as credit_note_acceptance_date,
        "credit note value" as credit_note_value,
        "credit note pre tax value" as credit_note_pre_tax_value,
        "credit note tax value" as credit_note_tax_value,
        "cgst amount" as cgst_amount,
        "cgst percentage" as cgst_percentage,
        "igst amount" as igst_amount,
        "igst percentage" as igst_percentage,
        "sgst amount" as sgst_amount,
        "sgst percentage" as sgst_percentage,
        "credit note posting status" as credit_note_posting_status,
        "fwd seller order id" as fwd_seller_order_id,
        "cust order no" as customer_order_no,
         CAST("fwd po no" AS VARCHAR) AS order_id,
         case  when "fwd po date" = 'nan' then null else cast(date_parse("fwd po date", '%a %b %d %H:%i:%s IST %Y') as date) end as fwd_po_date,
        "fwd b2b invoice no" as fwd_b2b_invoice_no,
        CAST("fwd b2b invoice date" AS DATE) AS fwd_b2b_invoice_date,
        "fwd b2b invoice amt" as fwd_b2b_invoice_amt,
        "fwd carrier name" as fwd_carrier_name,
        "fwd awb" as fwd_awb,
        "mrp" as mrp,
        "brand" as brand,
        "fulfillment type" as fulfillment_type,
        "dc code" as dc_code,
        "pob id" as pob_id,
        "seller name" as seller_name,
        'Ajio' AS channel_name

        from source)

select * from renamed


