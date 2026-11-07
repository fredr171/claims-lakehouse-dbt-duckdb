-- CMS Medicare FFS claims in the conformed contract (macros/payers.sql): one row per claim
-- family from int_cclf__claim_families. The marts union this with every other payer's claims.
--
-- Mapping: Part A -> INSTITUTIONAL, Part B -> PROFESSIONAL. Status ORIGINAL -> PAID,
-- ADJUSTED -> ADJUSTED, CANCELLED -> VOIDED (CCLF has no denied claims). FFS has no capitation.

{{ config(materialized='view', enabled=is_registered('cclf')) }}

select
    cast({{ payer_id('cclf') }} as varchar)                         as payer_id,
    cast(claim_family_key as varchar)                               as payer_claim_id,
    cast(beneficiary_mbi as varchar)                                as payer_member_id,
    cast(beneficiary_mbi as varchar)                                as beneficiary_mbi,
    cast(case part when 'A' then 'INSTITUTIONAL' when 'B' then 'PROFESSIONAL' end as varchar)
                                                                    as claim_category,
    cast(family_status as varchar)                                  as payer_claim_status,
    cast(
        case family_status
            when 'ORIGINAL' then 'PAID'
            when 'ADJUSTED' then 'ADJUSTED'
            when 'CANCELLED' then 'VOIDED'
        end as varchar
    )                                                               as claim_status,
    cast(service_start_date as date)                                as service_start_date,
    cast(service_month as date)                                     as service_month,
    cast(first_delivery_month as date)                              as first_delivery_month,
    cast(latest_delivery_month as date)                             as latest_delivery_month,
    cast(net_paid_amount as decimal(18, 2))                         as net_paid_amount,
    cast(false as boolean)                                          as is_capitated
from {{ ref('int_cclf__claim_families') }}
