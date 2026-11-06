-- CCLF4 Part A diagnosis codes: one row per diagnosis per claim version per delivery.
-- Sequence 0 is the principal diagnosis (matches CCLF1 principal_diagnosis_code).

with source as (

    -- Only the current delivery of each file; superseded re-sends stay in raw.
    {{ current_delivery_rows('cclf', 'claims_diagnosis') }}

),

renamed as (

    select
        -- identifiers
        nullif(cur_clm_uniq_id, '')                     as claim_id,
        cast(nullif(clm_val_sqnc_num, '') as integer)   as diagnosis_sequence_number,
        nullif(bene_mbi_id, '')                         as beneficiary_mbi,
        nullif(bene_hic_num, '')                        as beneficiary_hicn,
        nullif(prvdr_oscar_num, '')                     as facility_ccn,

        -- classification
        nullif(clm_type_cd, '')                         as claim_type_code,
        nullif(clm_prod_type_cd, '')                    as claim_product_type_code,

        -- diagnosis (ICD-10-CM, stored without the decimal point, as delivered)
        nullif(clm_dgns_cd, '')                         as diagnosis_code,
        nullif(clm_poa_ind, '')                         as present_on_admission_indicator,
        nullif(dgns_prcdr_icd_ind, '')                  as icd_version_indicator,

        -- dates
        cast(nullif(clm_from_dt, '') as date)           as claim_start_date,
        cast(nullif(clm_thru_dt, '') as date)           as claim_end_date,

        -- delivery + lineage
        cast(delivery_month || '-01' as date)           as delivery_month,
        _source_file,
        _source_row_number,
        _source_checksum,
        _ingested_at,
        _batch_id

    from source

)

select * from renamed
