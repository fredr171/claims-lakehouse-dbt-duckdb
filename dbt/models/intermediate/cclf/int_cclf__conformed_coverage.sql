-- CMS Medicare FFS coverage in the conformed contract (macros/payers.sql): one row per
-- coverage span of a beneficiary. The marts union this with every other payer's spans.
--
-- CCLF8 is change-only: a span is a BENE_RNG_BGN_DT, and its end date (and attributes) come
-- from the latest delivered row with that start date. A beneficiary who leaves FFS and
-- returns later has two spans. Coverage ends at death if that comes first.
-- Code mapping: sex '1'/'2' -> 'M'/'F'; dual status 'NA' -> not dual. FFS has no plan.

{{ config(materialized='view', enabled=is_registered('cclf')) }}

with latest_per_span as (

    select
        *,
        row_number() over (
            partition by beneficiary_mbi, coverage_start_date
            order by delivery_month desc
        ) as row_rank
    from {{ ref('stg_cclf__beneficiary_demographics') }}
    where coverage_start_date is not null

)

select
    cast({{ payer_id('cclf') }} as varchar)                         as payer_id,
    cast(beneficiary_mbi as varchar)                                as payer_member_id,
    cast(beneficiary_mbi as varchar)                                as beneficiary_mbi,
    cast(coverage_start_date as date)                               as coverage_start_date,
    cast(
        case
            when death_date is not null
                and (coverage_end_date is null or death_date < coverage_end_date)
                then death_date
            else coverage_end_date
        end as date
    )                                                               as coverage_end_date,
    cast('MEDICARE_FFS' as varchar)                                 as plan_code,
    cast(year(birth_date) as integer)                               as birth_year,
    cast(case sex_code when '1' then 'M' when '2' then 'F' else 'U' end as varchar)
                                                                    as sex,
    cast(usps_state_code as varchar)                                as state_code,
    cast(coalesce(dual_status_code, 'NA') <> 'NA' as boolean)       as is_dual,
    cast(death_date is not null as boolean)                         as is_deceased,
    cast(delivery_month as date)                                    as last_delivery_month
from latest_per_span
where row_rank = 1
