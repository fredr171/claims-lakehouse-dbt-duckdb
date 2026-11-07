-- Every claim's member should have coverage at some payer (a dim_member row). A row here is a
-- claim whose member never arrived in any eligibility file: it drops out of member-level
-- reporting, and its paid amount has no member months behind it.
-- WARN (macros/test_scoping.sql): the marts stay internally consistent; scoped to the batch.
{{ config(severity='warn') }}

select fct_claims.payer_id, fct_claims.claim_key, fct_claims.latest_delivery_month
from {{ ref('fct_claims') }} as fct_claims
left join {{ ref('dim_member') }} as dim_member using (member_key)
where dim_member.member_key is null
    and {{ in_batch('fct_claims.latest_delivery_month') }}
