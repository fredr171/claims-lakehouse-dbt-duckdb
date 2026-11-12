-- An MA payer member_id maps to exactly one MBI and vice versa, across all deliveries and
-- both files. The MBI is what links a member to their CCLF history when they switch
-- between fee-for-service and Medicare Advantage, so a remapped ID would merge or split
-- people downstream. Returns IDs only (never names).
-- WARN (macros/test_scoping.sql): the intermediate layer stays correct; scoped to the batch.
{{ config(severity='warn') }}

with pairs as (

    select distinct member_id, beneficiary_mbi from {{ ref('stg_ma_payer__member_eligibility') }}
    union
    select distinct member_id, beneficiary_mbi from {{ ref('stg_ma_payer__medical_claim_lines') }}

),

-- report only IDs that occur in the batch (all history is still used to find conflicts)
batch_ids as (

    select member_id, beneficiary_mbi from {{ ref('stg_ma_payer__member_eligibility') }}
    where {{ in_batch() }}
    union
    select member_id, beneficiary_mbi from {{ ref('stg_ma_payer__medical_claim_lines') }}
    where {{ in_batch() }}

)

select 'member_id with several MBIs' as problem, member_id as id, count(*) as mapped_ids
from pairs
where member_id in (select member_id from batch_ids)
group by member_id having count(*) > 1

union all

select 'MBI with several member_ids', beneficiary_mbi, count(*)
from pairs
where beneficiary_mbi in (select beneficiary_mbi from batch_ids)
group by beneficiary_mbi having count(*) > 1
