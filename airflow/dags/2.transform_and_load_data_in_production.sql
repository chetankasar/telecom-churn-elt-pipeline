-- Transform data from staging to telecom database
-- Steps:
-- 1. Read data from staging table
-- 2. Handle missing values with defaults
-- 3. Anonymize CustomerID (PII) using SHA-256 hash
-- 4. Add transformation metadata


-- Step 1: Create temporary view from staging PostgreSQL table
CREATE OR REPLACE TEMPORARY VIEW staging_data
USING JDBC
OPTIONS (
    url 'jdbc:postgresql://postgres:5432/staging',
    dbtable 'customer_churn_staging',
    user 'staging_user',
    password 'staging_password',
    driver 'org.postgresql.Driver'
)
;


-- Step 2: Create transformed view with all transformations
CREATE OR REPLACE TEMPORARY VIEW transformed_data AS
SELECT
    -- Anonymize CustomerID: SHA-256 hash, first 16 characters
    SUBSTRING(SHA2(CAST(COALESCE(CustomerID, 0) AS STRING), 256), 1, 16) AS customer_id_hash,
    
    -- Handle missing values with defaults
    COALESCE(Age, 0) AS Age,
    COALESCE(Gender, 'Unknown') AS Gender,
    COALESCE(Tenure, 0) AS Tenure,
    COALESCE(MonthlyCharges, 0.0) AS MonthlyCharges,
    COALESCE(ContractType, 'Unknown') AS ContractType,
    COALESCE(InternetService, 'Unknown') AS InternetService,
    COALESCE(TotalCharges, 0.0) AS TotalCharges,
    COALESCE(TechSupport, 'Unknown') AS TechSupport,
    COALESCE(Churn, 'Unknown') AS Churn,
    
    -- Add transformation metadata
    CURRENT_TIMESTAMP() AS created_at,
    '1.0' AS transformation_version
    
    -- Note: Excluding staging metadata columns (source_file, loaded_at) if they exist
FROM staging_data
;


-- Step 3: Create table if not exists in telecom database
CREATE TABLE IF NOT EXISTS jdbc.`jdbc:postgresql://postgres:5432/telecom?user=telecom_user&password=telecom_password`
USING org.postgresql.Driver
OPTIONS (
    dbtable 'customer_churn',
    createTableColumnTypes '''
        customer_id_hash VARCHAR(16),
        Age INTEGER,
        Gender VARCHAR(50),
        Tenure INTEGER,
        MonthlyCharges DECIMAL(10,2),
        ContractType VARCHAR(50),
        InternetService VARCHAR(50),
        TotalCharges DECIMAL(10,2),
        TechSupport VARCHAR(10),
        Churn VARCHAR(10),
        created_at TIMESTAMP,
        transformation_version VARCHAR(10)
    '''
)
AS
SELECT
    customer_id_hash,
    Age,
    Gender,
    Tenure,
    MonthlyCharges,
    ContractType,
    InternetService,
    TotalCharges,
    TechSupport,
    Churn,
    created_at,
    transformation_version
FROM transformed_data
WHERE 1=0
;


-- Step 4: Append transformed data to telecom table
INSERT INTO TABLE jdbc.`jdbc:postgresql://postgres:5432/telecom?user=telecom_user&password=telecom_password`
USING org.postgresql.Driver
OPTIONS (
    dbtable 'customer_churn',
    truncate 'false'
)
SELECT
    customer_id_hash,
    Age,
    Gender,
    Tenure,
    MonthlyCharges,
    ContractType,
    InternetService,
    TotalCharges,
    TechSupport,
    Churn,
    created_at,
    transformation_version
FROM transformed_data
;
