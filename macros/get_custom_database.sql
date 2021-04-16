{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- if target.name in ['prod'] -%}

        {{ target.name }}_{{ target.database }}

    {%- else -%}
      {%- if custom_database_name is not none -%}

        {{ target.name }}_{{ custom_database_name }}

      {%- else -%}

        {{ env_var('DBT_USER') }}_{{ target.name }}_{{ target.database }}

      {%- endif -%}
    {%- endif -%}

{%- endmacro %}
