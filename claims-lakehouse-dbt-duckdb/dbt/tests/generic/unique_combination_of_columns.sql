{#
    Fails for every combination of the given columns that occurs more than once.
    A local stand-in for dbt_utils.unique_combination_of_columns, so the project
    needs no package download.
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}

select
    {{ combination_of_columns | join(', ') }},
    count(*) as row_count
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1

{% endtest %}
