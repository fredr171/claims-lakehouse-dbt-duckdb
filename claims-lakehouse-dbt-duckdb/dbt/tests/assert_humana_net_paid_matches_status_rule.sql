-- Humana net paid is computed as the sum over every claim version (reversals carry
-- negated amounts). Independently, the family's status implies what was finally paid:
--   VOIDED -> 0;  REPLACED -> the replacement's paid;  DENIED -> 0;  else the original's.
-- Both must agree. A row here means a reversal that does not fully negate its
-- original, or a version attached to the wrong family.

select claim_family_key, family_status, net_paid_amount, original_paid_amount,
       replacement_paid_amount
from {{ ref('int_humana__claim_families') }}
where has_original
  and net_paid_amount <> case family_status
        when 'VOIDED' then 0
        when 'REPLACED' then replacement_paid_amount
        when 'DENIED' then 0
        else original_paid_amount
    end
