{# Generic test: the given columns are unique together (a small stand-in for dbt_utils, without the package). #}
{% test dbt_utils_unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as rows_with_key
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
