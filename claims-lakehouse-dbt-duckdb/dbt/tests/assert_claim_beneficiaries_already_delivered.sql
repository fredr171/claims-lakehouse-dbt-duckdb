-- Every claim's beneficiary must have arrived in a CCLF8 delivery in the same month
-- or earlier. A row here is a claim for an unknown (or not-yet-delivered) beneficiary,
-- which would drop out of any join to the member dimension.

with claim_beneficiaries as (

    select 'claims_header' as model, beneficiary_mbi, min(delivery_month) as first_claim_month
    from {{ ref('stg_cclf__claims_header') }}
    group by beneficiary_mbi

    union all

    select 'part_b_physician_lines', beneficiary_mbi, min(delivery_month)
    from {{ ref('stg_cclf__part_b_physician_lines') }}
    group by beneficiary_mbi

),

first_beneficiary_delivery as (

    select beneficiary_mbi, min(delivery_month) as first_member_month
    from {{ ref('stg_cclf__beneficiary_demographics') }}
    group by beneficiary_mbi

)

select c.*, b.first_member_month
from claim_beneficiaries as c
left join first_beneficiary_delivery as b using (beneficiary_mbi)
where b.first_member_month is null
   or b.first_member_month > c.first_claim_month
