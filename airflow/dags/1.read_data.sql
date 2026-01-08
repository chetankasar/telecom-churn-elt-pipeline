-- Step 1: Create temporary view from CSV with explicit schema definition
CREATE OR REPLACE TEMPORARY VIEW csv_data (
    CustomerID INT,
    Age INT,
    Gender STRING,
    Tenure INT,
    MonthlyCharges DECIMAL(10,2),
    ContractType STRING,
    InternetService STRING,
    TotalCharges DECIMAL(10,2),
    TechSupport STRING,
    Churn STRING
)
USING CSV
OPTIONS (
    path '/opt/airflow/data/raw/customer_churn_data.csv',
    header 'true',
    inferSchema 'false'
)
;
