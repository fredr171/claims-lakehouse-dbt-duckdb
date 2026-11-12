-- MAP_ELIG a fictional Medicare Advantage payer eligibility: one row per member per delivery in
-- which the member is new, changed or terminated. Each row carries the member's full
-- current values. Names and address are synthetic PII: staging only, never in marts.

with source as (

    -- Only the current delivery of each file; superseded re-sends stay in raw.
    {{ current_delivery_rows('ma_payer', 'member_eligibility') }}

),

renamed as (

    select
        -- identifiers
        nullif(member_id, '')                                   as member_id,
        nullif(mbi, '')                                         as beneficiary_mbi,

        -- demographics (synthetic PII)
        nullif(first_name, '')                                  as first_name,
        nullif(middle_initial, '')                              as middle_initial,
        nullif(last_name, '')                                   as last_name,
        cast(nullif(dob, '') as date)                           as birth_date,
        nullif(gender, '')                                      as gender_code,
        cast(nullif(death_dt, '') as date)                      as death_date,

        -- address (synthetic PII)
        nullif(address_line_1, '')                              as address_line_1,
        nullif(city, '')                                        as city,
        nullif(state, '')                                       as usps_state_code,
        nullif(zip, '')                                         as zip5_code,
        nullif(county_fips, '')                                 as county_fips_code,

        -- plan
        nullif(contract_id, '')                                 as contract_id,
        nullif(pbp_id, '')                                      as plan_benefit_package_id,
        nullif(plan_name, '')                                   as plan_name,
        nullif(product_type, '')                                as product_type,
        cast(nullif(enrollment_start_dt, '') as date)           as enrollment_start_date,
        cast(nullif(enrollment_end_dt, '') as date)             as enrollment_end_date,

        -- attributed primary care provider
        nullif(pcp_npi, '')                                     as pcp_npi,
        nullif(pcp_tin, '')                                     as pcp_tin,
        cast(nullif(pcp_attribution_start_dt, '') as date)      as pcp_attribution_start_date,

        -- status flags (Y/N, kept as delivered)
        nullif(dual_flag, '')                                   as dual_flag,
        nullif(lis_flag, '')                                    as low_income_subsidy_flag,
        nullif(esrd_flag, '')                                   as esrd_flag,
        nullif(hospice_flag, '')                                as hospice_flag,

        -- file
        cast(nullif(file_as_of_dt, '') as date)                 as file_as_of_date,
        nullif(record_change_type, '')                          as record_change_type,

        -- delivery + lineage
        cast(delivery_month || '-01' as date)                   as delivery_month,
        _source_file,
        _source_row_number,
        _source_checksum,
        _ingested_at,
        _batch_id

    from source

)

select * from renamed
