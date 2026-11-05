-- CCLF1 Part A claim header: one row per claim version per delivery, typed and renamed.
-- Identifiers and codes stay text (leading zeros); empty strings become NULL.
-- No row filtering or de-duplication: cancellations/adjustments are resolved downstream
-- (only superseded deliveries of a file are left out, see stg_cclf__deliveries).

with source as (

    -- Only the current delivery of each file; superseded re-sends stay in raw.
    {{ current_delivery_rows('cclf', 'claims_header') }}

),

renamed as (

    select
        -- identifiers
        nullif(cur_clm_uniq_id, '')                                 as claim_id,
        nullif(clm_cntl_num, '')                                    as claim_control_number,
        nullif(clm_org_cntl_num, '')                                as original_claim_control_number,
        nullif(bene_mbi_id, '')                                     as beneficiary_mbi,
        nullif(bene_hic_num, '')                                    as beneficiary_hicn,

        -- providers
        nullif(prvdr_oscar_num, '')                                 as facility_ccn,
        nullif(fac_prvdr_npi_num, '')                               as facility_npi,
        nullif(atndg_prvdr_npi_num, '')                             as attending_npi,
        nullif(prvdr_fac_fips_st_cd, '')                            as facility_fips_state_code,

        -- claim classification
        nullif(clm_type_cd, '')                                     as claim_type_code,
        nullif(clm_adjsmt_type_cd, '')                              as adjustment_type_code,
        nullif(clm_bill_fac_type_cd, '')                            as bill_facility_type_code,
        nullif(clm_bill_clsfctn_cd, '')                             as bill_classification_code,
        nullif(clm_bill_freq_cd, '')                                as bill_frequency_code,
        nullif(clm_op_srvc_type_cd, '')                             as outpatient_service_type_code,
        nullif(clm_admsn_type_cd, '')                               as admission_type_code,
        nullif(clm_admsn_src_cd, '')                                as admission_source_code,
        nullif(bene_ptnt_stus_cd, '')                               as patient_discharge_status_code,
        nullif(dgns_drg_cd, '')                                     as drg_code,
        nullif(clm_mdcr_npmt_rsn_cd, '')                            as medicare_nonpayment_reason_code,
        nullif(clm_nch_prmry_pyr_cd, '')                            as primary_payer_code,

        -- diagnoses (ICD-10-CM, stored without the decimal point, as delivered)
        nullif(prncpl_dgns_cd, '')                                  as principal_diagnosis_code,
        nullif(admtg_dgns_cd, '')                                   as admitting_diagnosis_code,
        nullif(dgns_prcdr_icd_ind, '')                              as icd_version_indicator,

        -- dates
        cast(nullif(clm_from_dt, '') as date)                       as claim_start_date,
        cast(nullif(clm_thru_dt, '') as date)                       as claim_end_date,
        cast(nullif(clm_efctv_dt, '') as date)                      as claim_effective_date,
        cast(nullif(clm_idr_ld_dt, '') as date)                     as claim_load_date,

        -- amounts (negative on cancellations)
        cast(nullif(clm_pmt_amt, '') as decimal(18, 2))             as payment_amount,
        cast(nullif(clm_mdcr_instnl_tot_chrg_amt, '') as decimal(18, 2)) as total_charge_amount,

        -- delivery + lineage
        cast(delivery_month || '-01' as date)                       as delivery_month,
        _source_file,
        _source_row_number,
        _source_checksum,
        _ingested_at,
        _batch_id

    from source

)

select * from renamed
