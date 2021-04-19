
  create or replace  view dbt_dev_dw.dw_stg.base_loan_account  as (
    

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
    dev_raw.mambu.loan_accounts
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
),filtered_rules as (
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
    nvl(cr_affordability, 'N/A') as cr_affordability,
    nvl(cr_arvato_credit, 'N/A') as cr_arvato_credit,
    nvl(cr_arvato_limit_increase, 'N/A') as cr_arvato_limit_increase,
    nvl(cr_bisnode_aml_pep, 'N/A') as cr_bisnode_aml_pep,
    nvl(cr_bisnode_rgs_decision, 'N/A') as cr_bisnode_rgs_decision,
    nvl(cr_blocked_customer, 'N/A') as cr_blocked_customer,
    nvl(cr_credit_assessment, 'N/A') as cr_credit_assessment,
    nvl(cr_credit_check, 'N/A') as cr_credit_check,
    nvl(cr_customer_age, 'N/A') as cr_customer_age,
    nvl(cr_customer_debt, 'N/A') as cr_customer_debt,
    nvl(cr_customer_info_change, 'N/A') as cr_customer_info_change,
    nvl(cr_loan_multi_exposure, 'N/A') as cr_loan_multi_exposure,
    nvl(cr_money_laundering, 'N/A') as cr_money_laundering
    from
      filtered_rules
    pivot (max(credit_rule_decision) for credit_rule in ('Affordability', 'ArvatoCredit', 'ArvatoLimitIncrease', 'BisnodeAmlPep', 'BisnodeRGSDecision', 'BlockedCustomer', 'CreditAssessment', 'CreditCheck', 'CustomerAge', 'CustomerDebt', 'CustomerInfoChange', 'LoanMultiExposure', 'MoneyLaundering'))
    as pv (loan_account_id,
           last_modified_ts,
           cr_affordability,
           cr_arvato_credit,
           cr_arvato_limit_increase,
           cr_bisnode_aml_pep,
           cr_bisnode_rgs_decision,
           cr_blocked_customer,
           cr_credit_assessment,
           cr_credit_check,
           cr_customer_age,
           cr_customer_debt,
           cr_customer_info_change,
           cr_loan_multi_exposure,
           cr_money_laundering
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
  pv.cr_affordability,
  pv.cr_arvato_credit,
  pv.cr_arvato_limit_increase,
  pv.cr_bisnode_aml_pep,
  pv.cr_bisnode_rgs_decision,
  pv.cr_blocked_customer,
  pv.cr_credit_assessment,
  pv.cr_credit_check,
  pv.cr_customer_age,
  pv.cr_customer_debt,
  pv.cr_customer_info_change,
  pv.cr_loan_multi_exposure,
  pv.cr_money_laundering,
  src.last_modified_ts
from
  src
join pivot_credit_rules pv on (pv.loan_account_id=src.loan_account_id and pv.last_modified_ts=src.last_modified_ts)
  );
