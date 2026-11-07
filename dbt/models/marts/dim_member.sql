-- One row per PERSON across all payers, keyed by MBI (the identity Medicare assigns and every
-- payer delivers). A beneficiary who moves from FFS to Medicare Advantage is one member with
-- spans at both payers.
--
-- Attributes come from the most recently delivered span (any payer). current_payer_id is the
-- active payer of the latest open span; spans at a retired payer are history, not current
-- coverage. No names, addresses or full birth dates: those stay in staging.

with coverage as (

    {{ union_conformed('coverage') }}

),

payers as (

    select payer_id, payer_status from {{ ref('dim_payer') }}

),

latest_attributes as (

    select *
    from coverage
    qualify row_number() over (
        partition by beneficiary_mbi
        order by last_delivery_month desc, coverage_start_date desc, payer_id
    ) = 1

),

current_coverage as (

    select coverage.beneficiary_mbi, coverage.payer_id, coverage.coverage_start_date
    from coverage
    join payers using (payer_id)
    where coverage.coverage_end_date is null and payers.payer_status = 'active'
    qualify row_number() over (
        partition by coverage.beneficiary_mbi
        order by coverage.coverage_start_date desc, coverage.payer_id
    ) = 1

),

history as (

    select
        beneficiary_mbi,
        count(distinct payer_id)                                    as payer_count,
        count(*)                                                    as coverage_span_count,
        min(coverage_start_date)                                    as first_coverage_start_date
    from coverage
    group by beneficiary_mbi

)

select
    latest_attributes.beneficiary_mbi                               as member_key,
    latest_attributes.beneficiary_mbi,
    latest_attributes.birth_year,
    latest_attributes.sex,
    latest_attributes.state_code,
    latest_attributes.is_dual,
    latest_attributes.is_deceased,
    current_coverage.payer_id                                       as current_payer_id,
    current_coverage.coverage_start_date                            as current_coverage_start_date,
    history.payer_count,
    history.payer_count > 1                                         as is_multi_payer,
    history.coverage_span_count,
    history.first_coverage_start_date
from latest_attributes
join history using (beneficiary_mbi)
left join current_coverage using (beneficiary_mbi)
