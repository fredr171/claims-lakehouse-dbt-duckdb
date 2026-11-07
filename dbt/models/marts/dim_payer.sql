-- One row per registered payer (dbt_project.yml `payers`), with its delivery window.
--
-- A payer appears as soon as it is registered (payer_status 'awaiting_data' until its first
-- file is built), and stays after it is retired: retirement ends its window, never its
-- history. first/last_delivery_month bound fct_member_months.

with registry as (

    {% for p in payer_registry() %}
    select
        '{{ p.payer_id }}'                                          as payer_id,
        '{{ p.payer_name | replace("'", "''") }}'                   as payer_name,
        '{{ p.line_of_business | replace("'", "''") }}'             as line_of_business,
        '{{ p.dbt_source }}'                                        as dbt_source,
        cast('{{ p.onboarded_month }}-01' as date)                  as onboarded_month,
        {% if p.retired_month -%}
        cast('{{ p.retired_month }}-01' as date)
        {%- else -%}
        cast(null as date)
        {%- endif %}                                                as retired_month
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),

deliveries as (

    {% for part in built_payer_relations('stg_', '__deliveries') %}
    select
        '{{ part.payer.payer_id }}'                                 as payer_id,
        min(delivery_month)                                         as first_delivery_month,
        max(delivery_month)                                         as last_delivery_month,
        count(*)                                                    as current_file_count
    from {{ part.relation }}
    where is_current
    {% if not loop.last %}union all{% endif %}
    {% else %}
    select
        null::varchar as payer_id, null::date as first_delivery_month,
        null::date as last_delivery_month, null::bigint as current_file_count
    where false
    {% endfor %}

)

select
    registry.payer_id,
    registry.payer_name,
    registry.line_of_business,
    registry.dbt_source,
    registry.onboarded_month,
    registry.retired_month,
    case
        when registry.retired_month is not null then 'retired'
        when deliveries.last_delivery_month is null then 'awaiting_data'
        else 'active'
    end                                                             as payer_status,
    deliveries.first_delivery_month,
    deliveries.last_delivery_month,
    coalesce(deliveries.current_file_count, 0)                      as current_file_count
from registry
left join deliveries using (payer_id)
