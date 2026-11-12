-- MA payer claim families: one row per original claim, across all deliveries.
--
-- MAP_MEDCLM is line-level; lines roll up to claim versions first. A family is the
-- original (frequency 1) plus any reversal (8: same claim_id, amounts negated) and
-- replacement (7: new claim_id). Both point back through original_claim_id, so the
-- family key is coalesce(original_claim_id, claim_id) = the original's claim_id.
--
-- Status comes from which versions are still ACTIVE, i.e. not cancelled by a reversal
-- (a reversal repeats the claim_id it reverses), so it never depends on version order:
-- an active replacement -> REPLACED; else an active original -> DENIED (status D) or
-- ORIGINAL; else nothing active -> VOIDED. Unlike CCLF, MA payer reversals carry
-- negated amounts, so net_paid_amount is simply the sum over every version; a test
-- checks it against the status rule (voided 0, replaced = replacement, denied 0,
-- else the original). Service month = month of the original's claim_start_date.
--
-- Incremental by affected family (macros/incremental_by_affected_keys.sql): a reversal
-- or replacement landing months later recomputes the family it belongs to.

{{
    config(
        unique_key='claim_family_key',
        incremental_strategy='delete+insert',
        post_hook="{{ delete_rows_from_vanished_files([ref('stg_ma_payer__medical_claim_lines')]) }}",
    )
}}

with claim_versions as (

    select
        coalesce(any_value(original_claim_id), claim_id)    as claim_family_key,
        claim_id,
        claim_frequency_code,
        any_value(claim_status_code)                         as claim_status_code,
        any_value(claim_form_type_code)                      as claim_form_type_code,
        any_value(member_id)                                 as member_id,
        any_value(beneficiary_mbi)                           as beneficiary_mbi,
        any_value(contract_id)                               as contract_id,
        min(claim_start_date)                                as claim_start_date,
        max(processed_date)                                  as processed_date,
        delivery_month,
        count(*)                                             as line_count,
        sum(paid_amount)                                     as paid_amount,
        sum(allowed_amount)                                  as allowed_amount,
        bool_or(capitated_flag = 'Y')                        as is_capitated,
        _source_checksum
    from {{ ref('stg_ma_payer__medical_claim_lines') }}
    group by claim_id, claim_frequency_code, delivery_month, _source_checksum

),

{% if is_incremental() %}
affected as (

    {{ affected_keys('claim_versions', 'claim_family_key') }}

),
{% endif %}

scoped as (

    select * from claim_versions
    {% if is_incremental() %}
    where claim_family_key in (select affected_key from affected)
    {% endif %}

),

sequenced as (

    select
        *,
        -- an original or replacement whose claim_id also has a reversal is cancelled
        claim_frequency_code <> '8'
            and bool_or(claim_frequency_code = '8') over (
                partition by claim_family_key, claim_id
            ) as is_reversed,
        -- order only picks among several versions of one kind (e.g. chained replacements)
        row_number() over (
            partition by claim_family_key
            order by delivery_month, processed_date, claim_id, claim_frequency_code
        ) as version_sequence
    from scoped

),

families as (

    select
        claim_family_key,

        -- original
        arg_min(claim_id, version_sequence) filter (where claim_frequency_code = '1')             as original_claim_id,
        arg_min(claim_status_code, version_sequence) filter (where claim_frequency_code = '1')    as original_status_code,
        arg_min(delivery_month, version_sequence) filter (where claim_frequency_code = '1')       as original_delivery_month,
        arg_min(claim_start_date, version_sequence) filter (where claim_frequency_code = '1')     as original_claim_start_date,
        arg_min(paid_amount, version_sequence) filter (where claim_frequency_code = '1')          as original_paid_amount,
        arg_min(allowed_amount, version_sequence) filter (where claim_frequency_code = '1')       as original_allowed_amount,
        arg_min(line_count, version_sequence) filter (where claim_frequency_code = '1')           as original_line_count,
        arg_min(is_capitated, version_sequence) filter (where claim_frequency_code = '1')         as original_is_capitated,
        count(*) filter (where claim_frequency_code = '1')                                         as original_count,

        -- reversals and replacements
        count(*) filter (where claim_frequency_code = '8')                                         as reversal_count,
        count(*) filter (where claim_frequency_code = '7')                                         as replacement_count,
        arg_max(claim_id, version_sequence) filter (where claim_frequency_code = '7')             as replacement_claim_id,
        arg_max(paid_amount, version_sequence) filter (where claim_frequency_code = '7')          as replacement_paid_amount,
        max(delivery_month) filter (where claim_frequency_code <> '1')                             as action_delivery_month,

        -- what is still active decides the status
        count(*) filter (where claim_frequency_code = '7' and not is_reversed)                     as active_replacements,
        count(*) filter (where claim_frequency_code = '1' and not is_reversed)                     as active_originals,

        -- family-wide
        arg_min(member_id, version_sequence)                                                      as member_id,
        arg_min(beneficiary_mbi, version_sequence)                                                as beneficiary_mbi,
        arg_min(contract_id, version_sequence)                                                    as contract_id,
        arg_min(claim_form_type_code, version_sequence)                                           as claim_form_type_code,
        min(claim_start_date)                                                                     as earliest_claim_start_date,
        sum(paid_amount)                                                                          as net_paid_amount,
        count(*)                                                                                  as version_count,
        min(delivery_month)                                                                       as first_delivery_month,
        max(delivery_month)                                                                       as latest_delivery_month,
        list_sort(list(distinct _source_checksum))                                                as _source_checksums
    from sequenced
    group by claim_family_key

)

select
    claim_family_key,
    member_id,
    beneficiary_mbi,
    contract_id,
    claim_form_type_code,
    original_claim_id is not null                                       as has_original,
    original_claim_id,
    original_status_code,
    original_delivery_month,
    cast(original_paid_amount as decimal(18, 2))                        as original_paid_amount,
    original_line_count,
    coalesce(original_is_capitated, false)                              as is_capitated,
    cast(case when original_is_capitated then original_allowed_amount end as decimal(18, 2))
                                                                        as capitated_allowed_amount,
    case
        when replacement_count > 0 then 'REPLACE'
        when reversal_count > 0 then 'VOID'
    end                                                                 as action_type,
    action_delivery_month,
    replacement_claim_id,
    cast(replacement_paid_amount as decimal(18, 2))                     as replacement_paid_amount,
    case
        when active_replacements > 0 then 'REPLACED'
        when active_originals > 0 and original_status_code = 'D' then 'DENIED'
        when active_originals > 0 then 'ORIGINAL'
        else 'VOIDED'
    end                                                                 as family_status,
    cast(net_paid_amount as decimal(18, 2))                             as net_paid_amount,
    coalesce(original_claim_start_date, earliest_claim_start_date)      as service_start_date,
    cast(
        date_trunc('month', coalesce(original_claim_start_date, earliest_claim_start_date))
        as date
    )                                                                   as service_month,
    version_count,
    original_count,
    reversal_count,
    replacement_count,
    first_delivery_month,
    latest_delivery_month,
    _source_checksums
from families
