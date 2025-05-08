{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT  
     C_CUSTOMER_SK as CUSTOMER_SK
    ,C_CUSTOMER_ID as CUSTOMER_ID
    ,C_CURRENT_CDEMO_SK as CURRENT_CDEMO_SK
    ,C_CURRENT_HDEMO_SK as CURRENT_HDEMO_SK
    ,C_CURRENT_ADDR_SK as CURRENT_ADDR_SK
    ,C_FIRST_SHIPTO_DATE_SK as FIRST_SHIPTO_DATE_SK
    ,C_FIRST_SALES_DATE_SK as FIRST_SALES_DATE_SK
    ,C_SALUTATION as SALUTATION
    ,C_FIRST_NAME as FIRST_NAME
    ,C_LAST_NAME as LAST_NAME
    ,C_PREFERRED_CUST_FLAG as PREFERRED_CUST_FLAG
    ,C_BIRTH_DAY as BIRTH_DAY
    ,C_BIRTH_MONTH as BIRTH_MONTH
    ,C_BIRTH_YEAR as BIRTH_YEAR
    ,C_BIRTH_COUNTRY as BIRTH_COUNTRY
    ,C_LOGIN as LOGIN
    ,C_EMAIL_ADDRESS as EMAIL_ADDRESS
    ,C_LAST_REVIEW_DATE as LAST_REVIEW_DATE
 FROM {{ source('TPCDS_SF10TCL', 'CUSTOMER') }}
    where  C_CUSTOMER_SK in ('64850438'
                            ,'64783728'
                            ,'64777728'
                            ,'64768365'
                            ,'64767740'
                            ,'64743398'
                            ,'64716327'
                            ,'64688212'
                            ,'64655213'
                            ,'64652376')
   
)

SELECT
    *
    {{add_current_timestamp()}}
FROM source_data