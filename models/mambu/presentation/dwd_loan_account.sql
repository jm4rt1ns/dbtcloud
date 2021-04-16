{{ config(schema='dw_pres') }}

select
  {{ dbt_utils.star(from=ref('stg_current_loan_account'), except=["ROWNUM"]) }}
from
  {{ ref('stg_current_loan_account') }}
