-- One row per member per payer per month of coverage: the denominator for per-member-per-
-- month (PMPM) metrics.
--
-- A month counts if the member was covered on any day of it. Months are bounded by the
-- payer's delivery window (dim_payer first/last_delivery_month): before it nothing is known,
-- after it (a retired payer, or data not delivered yet) coverage is not assumed. Overlapping
-- spans of one member at one payer count once.

with coverage as (

    {{ union_conformed('coverage') }}

),

windows as (

    select payer_id, first_delivery_month, last_delivery_month
    from {{ ref('dim_payer') }}
    where last_delivery_month is not null

),

bounded as (

    select
        coverage.payer_id,
        coverage.beneficiary_mbi                                    as member_key,
        coverage.payer_member_id,
        coverage.plan_code,
        greatest(date_trunc('month', coverage.coverage_start_date), windows.first_delivery_month)
                                                                    as from_month,
        least(
            date_trunc('month', coalesce(coverage.coverage_end_date, windows.last_delivery_month)),
            windows.last_delivery_month
        )                                                           as to_month
    from coverage
    join windows using (payer_id)

),

months as (

    select
        payer_id,
        member_key,
        payer_member_id,
        plan_code,
        cast(unnest(generate_series(from_month, to_month, interval 1 month)) as date) as month
    from bounded
    where from_month <= to_month

)

select
    payer_id,
    member_key,
    month,
    max(payer_member_id)                                            as payer_member_id,
    max(plan_code)                                                  as plan_code
from months
group by payer_id, member_key, month
