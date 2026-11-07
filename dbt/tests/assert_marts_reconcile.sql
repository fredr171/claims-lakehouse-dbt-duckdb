-- The gold summary must add up to the gold facts, per payer: net paid and non-voided claim
-- counts (agg_payer_monthly vs fct_claims) and member months (vs fct_member_months). A row
-- here means the full outer join or a filter lost or duplicated something.
-- STOP (macros/test_scoping.sql): a published mart would report wrong totals.

with summary as (

    select
        payer_id,
        sum(net_paid_amount) as net_paid_amount,
        sum(claim_count) as claim_count,
        sum(member_months) as member_months
    from {{ ref('agg_payer_monthly') }}
    group by payer_id

),

claims as (

    select
        payer_id,
        sum(net_paid_amount) as net_paid_amount,
        count(*) filter (where claim_status <> 'VOIDED') as claim_count
    from {{ ref('fct_claims') }}
    group by payer_id

),

member_months as (

    select payer_id, count(*) as member_months
    from {{ ref('fct_member_months') }}
    group by payer_id

)

select
    coalesce(summary.payer_id, claims.payer_id, member_months.payer_id) as payer_id,
    summary.net_paid_amount as summary_net_paid,
    claims.net_paid_amount as fact_net_paid,
    summary.claim_count as summary_claims,
    claims.claim_count as fact_claims,
    summary.member_months as summary_member_months,
    member_months.member_months as fact_member_months
from summary
full outer join claims using (payer_id)
full outer join member_months using (payer_id)
where summary.net_paid_amount is distinct from coalesce(claims.net_paid_amount, 0)
    or summary.claim_count is distinct from coalesce(claims.claim_count, 0)
    or summary.member_months is distinct from coalesce(member_months.member_months, 0)
