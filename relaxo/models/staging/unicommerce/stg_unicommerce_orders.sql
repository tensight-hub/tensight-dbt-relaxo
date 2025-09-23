with

source as (

    select * from {{ ref('stg_unicommerce_facility_wise_banglore') }}

    UNION ALL 

    select * from {{ ref('stg_unicommerce_facility_wise_bhiwandi') }}

    UNION ALL 

    select * from {{ ref('stg_unicommerce_facility_wise_ghevra') }}

    UNION ALL 

    select * from {{ ref('stg_unicommerce_facility_wise_gurgram') }}

    UNION ALL 

    select * from {{ ref('stg_unicommerce_facility_wise_hyderabad') }}

    UNION ALL

    select * from {{ ref('stg_unicommerce_facility_wise_kolkata') }}

    UNION ALL

    select * from {{ ref('stg_unicommerce_facility_wise_mundkanew') }}

)

select * from source