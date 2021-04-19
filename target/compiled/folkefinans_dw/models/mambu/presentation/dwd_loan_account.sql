

select
  "DW_LOAN_ACCOUNT_KEY",
  "LOAN_ACCOUNT_ID",
  "LOAN_NAME",
  "LOAN_AMOUNT",
  "PAYMENT_METHOD",
  "ACCOUNT_HOLDER_KEY",
  "ACCOUNT_HOLDER_TYPE",
  "ACCOUNT_STATE",
  "ACCOUNT_SUB_STATE",
  "CREATED_TS",
  "APPROVED_TS",
  "CLOSED_TS",
  "DISBURSEMENT_TS",
  "FIRST_REPAYMENT_TS",
  "APPLICATION_TYPE",
  "PREVIOUS_APPLICATION_ID",
  "ORIGINAL_APPLICATION_ID",
  "AP_DATA_PROCESSING_CONSENT",
  "INITIAL_APPLICATION_AMOUNT",
  "AP_IP_ADDRESS",
  "AP_MONTHLY_LIVING_COST",
  "AP_MONTHLY_NET_INCOME",
  "AP_PEP_ACCEPTANCE",
  "AP_TERMS_ACCEPTANCE",
  "AP_WEALTH",
  "ARVATO_ACCOUNT_ID",
  "BISNODE_RGS_PD",
  "BISNODE_RGS_SCORE",
  "CR_AFFORDABILITY",
  "CR_ARVATO_CREDIT",
  "CR_ARVATO_LIMIT_INCREASE",
  "CR_BISNODE_AML_PEP",
  "CR_BISNODE_RGS_DECISION",
  "CR_BLOCKED_CUSTOMER",
  "CR_CREDIT_ASSESSMENT",
  "CR_CREDIT_CHECK",
  "CR_CUSTOMER_AGE",
  "CR_CUSTOMER_DEBT",
  "CR_CUSTOMER_INFO_CHANGE",
  "CR_LOAN_MULTI_EXPOSURE",
  "CR_MONEY_LAUNDERING",
  "LAST_MODIFIED_TS"
from
  dbt_dev_dw.dw_stg.stg_current_loan_account