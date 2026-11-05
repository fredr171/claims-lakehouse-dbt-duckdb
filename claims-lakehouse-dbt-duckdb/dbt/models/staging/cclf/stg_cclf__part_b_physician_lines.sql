-- CCLF5 Part B physician claim lines: one row per line per claim version per delivery.
-- Claim-level fields (dates, adjustment type, control number) repeat on every line.

with source as (

    select * from {{ source('cclf', 'part_b_physician_lines') }}

),

renamed as (

    select
        -- identifiers
        nullif(cur_clm_uniq_id, '')                             as claim_id,
        cast(nullif(clm_line_num, '') as integer)               as line_number,
        nullif(clm_cntl_num, '')                                as claim_control_number,
        nullif(bene_mbi_id, '')                                 as beneficiary_mbi,
        nullif(bene_hic_num, '')                                as beneficiary_hicn,

        -- rendering provider
        nullif(rndrg_prvdr_npi_num, '')                         as rendering_npi,
        nullif(clm_rndrg_prvdr_tax_num, '')                     as rendering_provider_tin,
        nullif(rndrg_prvdr_type_cd, '')                         as rendering_provider_type_code,
        nullif(rndrg_prvdr_fips_st_cd, '')                      as rendering_provider_fips_state_code,
        nullif(clm_prvdr_spclty_cd, '')                         as provider_specialty_code,

        -- claim classification
        nullif(clm_type_cd, '')                                 as claim_type_code,
        nullif(clm_adjsmt_type_cd, '')                          as adjustment_type_code,
        nullif(clm_carr_pmt_dnl_cd, '')                         as carrier_payment_denial_code,
        nullif(clm_prcsg_ind_cd, '')                            as processing_indicator_code,

        -- service
        nullif(clm_line_hcpcs_cd, '')                           as hcpcs_code,
        nullif(hcpcs_1_mdfr_cd, '')                             as hcpcs_modifier_1_code,
        nullif(clm_fed_type_srvc_cd, '')                        as type_of_service_code,
        nullif(clm_pos_cd, '')                                  as place_of_service_code,
        cast(nullif(clm_line_srvc_unit_qty, '') as decimal(18, 3)) as service_unit_quantity,
        nullif(clm_line_prmry_pyr_cd, '')                       as line_primary_payer_code,

        -- diagnoses (ICD-10-CM, stored without the decimal point, as delivered)
        nullif(clm_line_dgns_cd, '')                            as line_diagnosis_code,
        nullif(clm_dgns_1_cd, '')                               as diagnosis_1_code,
        nullif(clm_dgns_2_cd, '')                               as diagnosis_2_code,
        nullif(clm_dgns_3_cd, '')                               as diagnosis_3_code,
        nullif(dgns_prcdr_icd_ind, '')                          as icd_version_indicator,

        -- dates
        cast(nullif(clm_from_dt, '') as date)                   as claim_start_date,
        cast(nullif(clm_thru_dt, '') as date)                   as claim_end_date,
        cast(nullif(clm_line_from_dt, '') as date)              as line_start_date,
        cast(nullif(clm_line_thru_dt, '') as date)              as line_end_date,
        cast(nullif(clm_efctv_dt, '') as date)                  as claim_effective_date,
        cast(nullif(clm_idr_ld_dt, '') as date)                 as claim_load_date,

        -- amounts (negative on cancellations)
        cast(nullif(clm_line_cvrd_pd_amt, '') as decimal(18, 2))    as line_paid_amount,
        cast(nullif(clm_line_alowd_chrg_amt, '') as decimal(18, 2)) as line_allowed_amount,

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
