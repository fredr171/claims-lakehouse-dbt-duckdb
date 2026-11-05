-- Every Humana claim's member must have arrived in an eligibility delivery in the same
-- month or earlier. A row here is a claim for an unknown (or not-yet-delivered) member,
-- which would drop out of any join to the member dimension.
-- WARN (macros/test_scoping.sql): the intermediate layer stays correct; scoped to the batch.
{{ config(severity='warn') }}

with claim_members as (

    select member_id, min(delivery_month) as first_claim_month
    from {{ ref('stg_humana__medical_claim_lines') }}
    where {{ in_batch() }}
    group by member_id

),

first_eligibility as (

    select member_id, min(delivery_month) as first_member_month
    from {{ ref('stg_humana__member_eligibility') }}
    group by member_id

)

select c.*, e.first_member_month
from claim_members as c
left join first_eligibility as e using (member_id)
where e.first_member_month is null
   or e.first_member_month > c.first_claim_month
