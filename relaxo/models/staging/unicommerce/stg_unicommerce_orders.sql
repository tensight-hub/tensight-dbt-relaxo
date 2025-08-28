with

source as (

    select * from {{ ref('stg_unicommerce_facility_wise_banglore') }}

    UNION ALL 

    select * from {{ ref('stg_unicommerce_facility_wise_bhiwandi') }}

    UNION ALL 

    select * from {{ ref('stg_unicommerce_facility_wise_ghevra') }}

    UNION ALL 

    select * from {{ ref('stg_unicommerce_facility_wise_gurgram') }}

)

select * from source