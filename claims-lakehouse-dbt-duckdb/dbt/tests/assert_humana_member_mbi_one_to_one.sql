-- A Humana member_id maps to exactly one MBI and vice versa, across all deliveries and
-- both files. The MBI is what links a member to their CCLF history when they switch
-- between fee-for-service and Medicare Advantage, so a remapped ID would merge or split
-- people downstream. Returns IDs only (never names).

with pairs as (

    select distinct member_id, beneficiary_mbi from {{ ref('stg_humana__member_eligibility') }}
    union
    select distinct member_id, beneficiary_mbi from {{ ref('stg_humana__medical_claim_lines') }}

)

select 'member_id with several MBIs' as problem, member_id as id, count(*) as mapped_ids
from pairs group by member_id having count(*) > 1

union all

select 'MBI with several member_ids', beneficiary_mbi, count(*)
from pairs group by beneficiary_mbi having count(*) > 1
