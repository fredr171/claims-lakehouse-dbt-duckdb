-- HUM_MEDCLM Humana Medicare Advantage medical claims: one row per line per claim
-- version per delivery. Institutional (UB-04, form I) and professional (CMS-1500,
-- form P) claims share the file; fields that do not apply to a form are NULL.
-- Claim-level fields repeat on every line. A reversal reuses claim_id with frequency 8
-- (amounts negated); a replacement has a new claim_id with frequency 7. Both point to
-- the original through original_claim_id.

with source as (

    -- Only the current delivery of each file; superseded re-sends stay in raw.
    {{ current_delivery_rows('humana', 'medical_claim_lines') }}

),

renamed as (

    select
        -- identifiers
        nullif(claim_id, '')                                    as claim_id,
        cast(nullif(claim_line_num, '') as integer)             as line_number,
        nullif(orig_claim_id, '')                               as original_claim_id,
        nullif(member_id, '')                                   as member_id,
        nullif(mbi, '')                                         as beneficiary_mbi,
        nullif(contract_id, '')                                 as contract_id,
        nullif(pbp_id, '')                                      as plan_benefit_package_id,

        -- claim classification
        nullif(claim_status, '')                                as claim_status_code,
        nullif(claim_freq_cd, '')                               as claim_frequency_code,
        nullif(claim_form_type, '')                             as claim_form_type_code,
        nullif(bill_type, '')                                   as bill_type_code,

        -- institutional stay
        cast(nullif(admit_dt, '') as date)                      as admission_date,
        cast(nullif(discharge_dt, '') as date)                  as discharge_date,
        nullif(admit_type, '')                                  as admission_type_code,
        nullif(admit_source, '')                                as admission_source_code,
        nullif(discharge_status, '')                            as patient_discharge_status_code,
        nullif(drg_cd, '')                                      as drg_code,

        -- service line
        nullif(revenue_cd, '')                                  as revenue_center_code,
        nullif(procedure_cd, '')                                as procedure_code,
        nullif(modifier_1, '')                                  as procedure_modifier_1_code,
        cast(nullif(units, '') as decimal(18, 3))               as service_unit_quantity,
        nullif(place_of_service, '')                            as place_of_service_code,

        -- providers
        nullif(billing_npi, '')                                 as billing_npi,
        nullif(billing_tin, '')                                 as billing_tin,
        nullif(rendering_npi, '')                               as rendering_npi,
        nullif(attending_npi, '')                               as attending_npi,
        nullif(facility_ccn, '')                                as facility_ccn,
        nullif(provider_specialty, '')                          as provider_specialty_code,
        nullif(in_network_flag, '')                             as in_network_flag,
        nullif(capitated_flag, '')                              as capitated_flag,

        -- diagnoses (ICD-10-CM without the decimal point, as delivered; 1 = principal)
        nullif(icd_version, '')                                 as icd_version_indicator,
        {%- for n in range(1, 13) %}
        nullif(dx_code_{{ n }}, '')                             as diagnosis_{{ n }}_code,
        {%- endfor %}

        -- dates
        cast(nullif(service_from_dt, '') as date)               as claim_start_date,
        cast(nullif(service_to_dt, '') as date)                 as claim_end_date,
        cast(nullif(line_from_dt, '') as date)                  as line_start_date,
        cast(nullif(line_to_dt, '') as date)                    as line_end_date,
        cast(nullif(received_dt, '') as date)                   as received_date,
        cast(nullif(processed_dt, '') as date)                  as processed_date,
        cast(nullif(paid_dt, '') as date)                       as paid_date,

        -- amounts (negated on reversals)
        cast(nullif(billed_amt, '') as decimal(18, 2))          as billed_amount,
        cast(nullif(allowed_amt, '') as decimal(18, 2))         as allowed_amount,
        cast(nullif(paid_amt, '') as decimal(18, 2))            as paid_amount,
        cast(nullif(copay_amt, '') as decimal(18, 2))           as copay_amount,
        cast(nullif(coinsurance_amt, '') as decimal(18, 2))     as coinsurance_amount,
        cast(nullif(deductible_amt, '') as decimal(18, 2))      as deductible_amount,

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
