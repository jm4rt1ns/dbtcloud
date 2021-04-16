
with
ranked_loan_account as (
  select
    *,
    row_number() over (partition by loan_account_id order by last_modified_ts desc) as rownum
  from
    {{ ref('base_loan_account') }}
),

current_loan_account as (
  select
    {{ dbt_utils.surrogate_key(['loan_account_id']) }} as dw_loan_account_key,
    *
  from
    ranked_loan_account
  where
    rownum=1
)
select * from current_loan_account
