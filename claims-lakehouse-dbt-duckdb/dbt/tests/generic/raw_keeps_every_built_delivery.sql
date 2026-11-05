{#
    STOP (immutable bronze). Raw never loses a delivery: a re-send only ADDS a delivery,
    and a superseded one stays listed in stg_<feed>__deliveries. So every checksum the
    intermediate tables were built from must still be in the deliveries index. If one is
    not, raw lost data (a mistaken delete, a wrong source glob, a layout change deployed
    before the objects were migrated) and the post-hook would otherwise silently delete
    every key built from it. Failing here skips the feed's staging and intermediate
    models, so their last good state is kept.

    Reads the intermediate tables by name, not ref(): a ref would make this test run
    AFTER intermediate, too late to protect it. Missing tables (a first build) are skipped.
    Returns checksums only.
#}
{% test raw_keeps_every_built_delivery(model, intermediate_models) %}

{%- set built = [] -%}
{%- if execute -%}
    {%- for name in intermediate_models -%}
        {%- set relation = adapter.get_relation(
            database=target.database, schema='intermediate', identifier=name) -%}
        {%- if relation is not none -%}{%- do built.append(relation) -%}{%- endif -%}
    {%- endfor -%}
{%- endif %}

{% if built %}
select distinct built.checksum, built.model
from (
    {%- for relation in built %}
    select unnest(_source_checksums) as checksum, '{{ relation.identifier }}' as model
    from {{ relation }}
    {%- if not loop.last %} union all{% endif %}
    {%- endfor %}
) as built
where built.checksum not in (select checksum from {{ model }})
{% else %}
select null::varchar as checksum, null::varchar as model where false
{% endif %}

{% endtest %}
