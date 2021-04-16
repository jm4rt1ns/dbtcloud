{% set credit_rules = get_credit_rules() %}

with
src as (
  select
    json:id::string as loan_account_id,
    json:loanName::string as loan_name,
    json:loanAmount::string as loan_amount,
    json:paymentMethod::string as payment_method,
    json:accountHolderKey::string as account_holder_key,
    json:accountHolderType::string as account_holder_type,
    json:accountState::string as account_state,
    json:accountSubState::string as account_sub_state,
    json:creationDate::timestamp as created_ts,
    json:approvedDate::timestamp as approved_ts,
    json:closedDate::timestamp as closed_ts,
    json:disbursementDetails.disbursementDate::timestamp as disbursement_ts,
    json:disbursementDetails.firstRepaymentDate::timestamp as first_repayment_ts,
    json:_application_information_l.application_type_l::string as application_type,
    nvl(json:_application_information_l.previous_application_id_l::string, 'N/A') as previous_application_id,
    nvl(json:_application_information_l.original_application_id_l::string, 'N/A') as original_application_id,
    json:_application_information_l.data_processing_consent_l::boolean as ap_data_processing_consent,
    try_to_number(json:_application_information_l.initial_application_amount_l::string) as initial_application_amount,
    json:_application_information_l.ip_address_l::string as ap_ip_address,
    try_to_number(json:_application_information_l.monthly_living_cost_l::string) as ap_monthly_living_cost,
    try_to_number(json:_application_information_l.monthly_net_income_l::string) as ap_monthly_net_income,
    json:_application_information_l.pep_acceptance_l::boolean as ap_pep_acceptance,
    json:_application_information_l.terms_acceptance_l::boolean as ap_terms_acceptance,
    json:_application_information_l.wealth_l::string as ap_wealth,
    json:_arvato_l.arvato_account_id_l::string as arvato_account_id,
    json:_credit_decision.bisnode_rgs_pd_l::numeric as bisnode_rgs_pd,
    json:_credit_decision.bisnode_rgs_score_l::string as bisnode_rgs_score,
    json:_credit_rules_l as json_credit_rules,
    last_modified_at::timestamp as last_modified_ts
  from
    {{ source('raw','loan_accounts') }}
),

credit_rules as (
  select
    loan_account_id,
    last_modified_ts,
    rule.value:credit_rule_name_l::string as credit_rule,
    rule.value:credit_rule_decision_l::string as credit_rule_decision,
    rule.value:_index::int as index,
    max(rule.value:_index::int) over (partition by loan_account_id, last_modified_ts, credit_rule) as max_index
  from
    src,
    lateral flatten(input => json_credit_rules) rule
),

{#- The order of the fields on the pivot table definition and on the   -#}
{#- following credit rules CTEs matters. Fields must be referenced     -#}
{#- in the same order throughout these CTEs.                           -#}
{#- When making changes, keep the field ordering consistent.           -#}
{#- Many instances of the same rule can exist due to the current       -#}
{#- implementation logic. Hence, we are here filtering rules in order  -#}
{#- to get only the latest result for each rule name.                  -#}

filtered_rules as (
  select
    loan_account_id,
    last_modified_ts,
    credit_rule,
    credit_rule_decision
  from
    credit_rules
  where
    index = max_index
),

pivot_credit_rules as (
  select
    loan_account_id,
    last_modified_ts,
    {% for rule_name in credit_rules -%}
    nvl(cr_{{ camel_to_snake(rule_name) }}, 'N/A') as cr_{{ camel_to_snake(rule_name) }}
    {%- if not loop.last %},{% endif %}
    {% endfor -%}
    from
      filtered_rules
    pivot (max(credit_rule_decision) for credit_rule in {{ credit_rules }})
    as pv (loan_account_id,
           last_modified_ts,
           {% for rule_name in credit_rules -%}
           cr_{{ camel_to_snake(rule_name) }}
           {%- if not loop.last %},{% endif %}
           {% endfor -%}
       )
)

select
  src.loan_account_id,
  src.loan_name,
  src.loan_amount,
  src.payment_method,
  src.account_holder_key,
  src.account_holder_type,
  src.account_state,
  src.account_sub_state,
  src.created_ts,
  src.approved_ts,
  src.closed_ts,
  src.disbursement_ts,
  src.first_repayment_ts,
  src.application_type,
  src.previous_application_id,
  src.original_application_id,
  src.ap_data_processing_consent,
  src.initial_application_amount,
  src.ap_ip_address,
  src.ap_monthly_living_cost,
  src.ap_monthly_net_income,
  src.ap_pep_acceptance,
  src.ap_terms_acceptance,
  src.ap_wealth,
  src.arvato_account_id,
  src.bisnode_rgs_pd,
  src.bisnode_rgs_score,
  {% for rule_name in credit_rules -%}
  pv.cr_{{ camel_to_snake(rule_name) }},
  {% endfor -%}
  src.last_modified_ts
from
  src
join pivot_credit_rules pv on (pv.loan_account_id=src.loan_account_id and pv.last_modified_ts=src.last_modified_ts)
