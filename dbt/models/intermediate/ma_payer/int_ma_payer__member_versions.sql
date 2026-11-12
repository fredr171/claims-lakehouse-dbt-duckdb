-- MA payer member history (SCD type 2) from MAP_ELIG: one row per member per version.
--
-- Same pattern as int_cclf__beneficiary_versions: a new version starts whenever the
-- tracked attributes differ from the member's previous delivered row (hash comparison),
-- so it works for change-only and full-snapshot deliveries. change_type is derived here
-- (NEW / UPDATE / TERM), never read from the synthetic record_change_type helper:
-- TERM = the version in which enrollment_end_date first appears.
--
-- valid_from = delivery month the version arrived; valid_to = the next version's
-- valid_from (exclusive), NULL while current. file_as_of_date is not tracked (it moves
-- every month); last_file_as_of_date is the newest as-of date carried by the version.
--
-- Incremental by affected member (macros/incremental_by_affected_keys.sql).

{{
    config(
        unique_key='member_id',
        incremental_strategy='delete+insert',
        post_hook="{{ delete_rows_from_vanished_files([ref('stg_ma_payer__member_eligibility')]) }}",
    )
}}

{%- set tracked_columns = [
    'beneficiary_mbi',
    'first_name',
    'middle_initial',
    'last_name',
    'birth_date',
    'gender_code',
    'death_date',
    'address_line_1',
    'city',
    'usps_state_code',
    'zip5_code',
    'county_fips_code',
    'contract_id',
    'plan_benefit_package_id',
    'plan_name',
    'product_type',
    'enrollment_start_date',
    'enrollment_end_date',
    'pcp_npi',
    'pcp_tin',
    'pcp_attribution_start_date',
    'dual_flag',
    'low_income_subsidy_flag',
    'esrd_flag',
    'hospice_flag',
] %}

with staged as (

    select
        *,
        {{ row_hash(tracked_columns) }} as attribute_hash
    from {{ ref('stg_ma_payer__member_eligibility') }}

),

{% if is_incremental() %}
affected as (

    {{ affected_keys('staged', 'member_id') }}

),
{% endif %}

scoped as (

    select * from staged
    {% if is_incremental() %}
    where member_id in (select affected_key from affected)
    {% endif %}

),

flagged as (

    select
        *,
        (lag(attribute_hash) over by_delivery) is distinct from attribute_hash as starts_version
    from scoped
    window by_delivery as (partition by member_id order by delivery_month)

),

numbered as (

    select
        *,
        sum(case when starts_version then 1 else 0 end) over (
            partition by member_id
            order by delivery_month
            rows between unbounded preceding and current row
        ) as version_number
    from flagged

),

-- Delivered rows that collapse into one version (more than one only for snapshot-style
-- deliveries that repeat an unchanged member).
version_spans as (

    select
        member_id,
        version_number,
        max(delivery_month)                         as last_delivered_month,
        max(file_as_of_date)                        as last_file_as_of_date,
        count(*)                                    as delivery_count,
        list_sort(list(distinct _source_checksum))  as _source_checksums
    from numbered
    group by member_id, version_number

),

versions as (

    select
        n.member_id,
        n.version_number,
        n.delivery_month as valid_from,
        lead(n.delivery_month) over by_version as valid_to,
        {%- for column in tracked_columns %}
        n.{{ column }},
        lag(n.{{ column }}) over by_version as previous_{{ column }},
        {%- endfor %}
        n.attribute_hash,
        s.last_delivered_month,
        s.last_file_as_of_date,
        s.delivery_count,
        s._source_checksums
    from numbered as n
    inner join version_spans as s
        on s.member_id = n.member_id
        and s.version_number = n.version_number
    where n.starts_version
    window by_version as (partition by n.member_id order by n.version_number)

)

select
    member_id || '|' || strftime(valid_from, '%Y-%m') as member_version_id,
    member_id,
    version_number,
    valid_from,
    valid_to,
    valid_to is null as is_current,
    case
        when version_number = 1 then 'NEW'
        when enrollment_end_date is not null and previous_enrollment_end_date is null then 'TERM'
        else 'UPDATE'
    end as change_type,
    case
        when version_number = 1 then []::varchar[]
        else list_filter(
            [
                {%- for column in tracked_columns %}
                case when {{ column }} is distinct from previous_{{ column }} then '{{ column }}' end
                {%- if not loop.last %},{% endif %}
                {%- endfor %}
            ],
            lambda changed: changed is not null
        )
    end as changed_columns,
    enrollment_end_date is not null as is_terminated,
    {%- for column in tracked_columns %}
    {{ column }},
    {%- endfor %}
    attribute_hash,
    last_delivered_month,
    last_file_as_of_date,
    delivery_count,
    _source_checksums
from versions
