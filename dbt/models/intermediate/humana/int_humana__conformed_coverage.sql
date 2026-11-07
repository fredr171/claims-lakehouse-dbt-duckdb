-- Humana MA coverage in the conformed contract (macros/payers.sql): one row per enrollment
-- span of a member. The marts union this with every other payer's spans.
--
-- HUM_ELIG is change-only: a span is an enrollment_start_date of a member_id, and its end
-- date (termination) and attributes come from the latest delivered row with that start.
-- Coverage ends at death if that comes first.
-- Code mapping: gender 'M'/'F' kept; dual_flag 'Y' -> dual; plan = contract-PBP.

{{ config(materialized='view', enabled=is_registered('humana')) }}

with latest_per_span as (

    select
        *,
        row_number() over (
            partition by member_id, enrollment_start_date
            order by delivery_month desc
        ) as row_rank
    from {{ ref('stg_humana__member_eligibility') }}
    where enrollment_start_date is not null

)

select
    cast({{ payer_id('humana') }} as varchar)                       as payer_id,
    cast(member_id as varchar)                                      as payer_member_id,
    cast(beneficiary_mbi as varchar)                                as beneficiary_mbi,
    cast(enrollment_start_date as date)                             as coverage_start_date,
    cast(
        case
            when death_date is not null
                and (enrollment_end_date is null or death_date < enrollment_end_date)
                then death_date
            else enrollment_end_date
        end as date
    )                                                               as coverage_end_date,
    cast(contract_id || '-' || plan_benefit_package_id as varchar)  as plan_code,
    cast(year(birth_date) as integer)                               as birth_year,
    cast(case when gender_code in ('M', 'F') then gender_code else 'U' end as varchar)
                                                                    as sex,
    cast(usps_state_code as varchar)                                as state_code,
    cast(dual_flag = 'Y' as boolean)                                as is_dual,
    cast(death_date is not null as boolean)                         as is_deceased,
    cast(delivery_month as date)                                    as last_delivery_month
from latest_per_span
where row_rank = 1
