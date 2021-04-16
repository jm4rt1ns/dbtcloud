{% macro camel_to_snake(camel_string) -%}
{% set name = modules.re.sub("(.)([A-Z][a-z]+)", "\\1_\\2", camel_string) -%}
{% set snake_case = modules.re.sub("([a-z0-9])([A-Z])", "\\1_\\2", name).lower() -%}
{{ snake_case }}
{%- endmacro %}

{% macro get_credit_rules() %}

{% set credit_rules_query %}
select distinct
rule.value:credit_rule_name_l::string
from
{{ source('raw','loan_accounts') }},
lateral flatten(input => json:_credit_rules_l) rule
where
rule.value:credit_rule_name_l::string is not null
order by 1
{% endset %}

{% set results = run_query(credit_rules_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
