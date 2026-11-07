-- One row per payer per month: members, claims and net paid by SERVICE month, and PMPM
-- (net paid per member month). The business-facing summary built only from the other marts.
--
-- Claims count by service month; voided claims are excluded from claim_count but their net
-- (zero) is still summed, so net_paid_amount equals fct_claims in total. PMPM is NULL where
-- there are no member months (service months before the payer's delivery window).

with member_months as (

    select payer_id, month, count(*) as member_months
    from {{ ref('fct_member_months') }}
    group by payer_id, month

),

claims as (

    select
        payer_id,
        service_month                                               as month,
        count(*) filter (where claim_status <> 'VOIDED')            as claim_count,
        sum(net_paid_amount)                                        as net_paid_amount
    from {{ ref('fct_claims') }}
    group by payer_id, service_month

)

select
    coalesce(member_months.payer_id, claims.payer_id)               as payer_id,
    coalesce(member_months.month, claims.month)                     as month,
    coalesce(member_months.member_months, 0)                        as member_months,
    coalesce(claims.claim_count, 0)                                 as claim_count,
    cast(coalesce(claims.net_paid_amount, 0) as decimal(18, 2))     as net_paid_amount,
    cast(
        coalesce(claims.net_paid_amount, 0) / nullif(member_months.member_months, 0)
        as decimal(18, 2)
    )                                                               as pmpm_net_paid
from member_months
full outer join claims
    on member_months.payer_id = claims.payer_id
    and member_months.month = claims.month
