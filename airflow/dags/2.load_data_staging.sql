-- Step 2: Create or replace PostgreSQL table and insert overwrite data
CREATE OR REPLACE TABLE jdbc.`jdbc:postgresql://postgres:5432/staging?user=staging_user&password=staging_password`
USING org.postgresql.Driver
OPTIONS (
    dbtable 'customer_churn_staging',
    createTableColumnTypes '''
        CustomerID INTEGER,
        Age INTEGER,
        Gender VARCHAR(50),
        Tenure INTEGER,
        MonthlyCharges DECIMAL(10,2),
        ContractType VARCHAR(50),
        InternetService VARCHAR(50),
        TotalCharges DECIMAL(10,2),
        TechSupport VARCHAR(10),
        Churn VARCHAR(10)
    '''
)
AS 
SELECT 
    CustomerID,
    Age,
    Gender,
    Tenure,
    MonthlyCharges,
    ContractType,
    InternetService,
    TotalCharges,
    TechSupport,
    Churn
FROM csv_data
;
