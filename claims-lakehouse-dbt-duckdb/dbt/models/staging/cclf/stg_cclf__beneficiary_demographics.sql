-- CCLF8 beneficiary demographics: one row per beneficiary per delivery in which it is
-- new or changed. Every row is a full snapshot; the latest delivery is the current
-- state. Picking that row (and SCD2 history) happens downstream, not here.
-- Contains synthetic names and addresses: never expose these columns in marts or issues.

with source as (

    -- Only the current delivery of each file; superseded re-sends stay in raw.
    {{ current_delivery_rows('cclf', 'beneficiary_demographics') }}

),

renamed as (

    select
        -- identifiers
        nullif(bene_mbi_id, '')                                 as beneficiary_mbi,
        nullif(bene_hic_num, '')                                as beneficiary_hicn,

        -- demographics
        cast(nullif(bene_dob, '') as date)                      as birth_date,
        cast(nullif(bene_death_dt, '') as date)                 as death_date,
        cast(nullif(bene_age, '') as integer)                   as age,  -- as of the delivery month
        nullif(bene_sex_cd, '')                                 as sex_code,
        nullif(bene_race_cd, '')                                as race_code,

        -- name and address (synthetic)
        nullif(bene_1st_name, '')                               as first_name,
        nullif(bene_midl_name, '')                              as middle_name,
        nullif(bene_last_name, '')                              as last_name,
        nullif(bene_line_1_adr, '')                             as address_line_1,
        nullif(geo_zip_plc_name, '')                            as city,
        nullif(geo_usps_state_cd, '')                           as usps_state_code,
        nullif(geo_zip5_cd, '')                                 as zip5_code,
        nullif(bene_zip_cd, '')                                 as zip9_code,
        nullif(bene_fips_state_cd, '')                          as fips_state_code,
        nullif(bene_fips_cnty_cd, '')                           as fips_county_code,

        -- entitlement and enrollment
        nullif(bene_mdcr_stus_cd, '')                           as medicare_status_code,
        nullif(bene_dual_stus_cd, '')                           as dual_status_code,
        nullif(bene_orgnl_entlmt_rsn_cd, '')                    as original_entitlement_reason_code,
        nullif(bene_entlmt_buyin_ind, '')                       as entitlement_buyin_indicator,
        cast(nullif(bene_part_a_enrlmt_bgn_dt, '') as date)     as part_a_enrollment_start_date,
        cast(nullif(bene_part_b_enrlmt_bgn_dt, '') as date)     as part_b_enrollment_start_date,
        cast(nullif(bene_rng_bgn_dt, '') as date)               as coverage_start_date,
        cast(nullif(bene_rng_end_dt, '') as date)               as coverage_end_date,

        -- Synthetic helper from the generator, NOT a real CCLF field. Kept only to
        -- verify change detection that downstream models derive themselves.
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
