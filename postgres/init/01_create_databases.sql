-- ============================================
-- PostgreSQL Initialization Script
-- Creates databases: airflow, staging, telecom, superset
-- ============================================

-- Create Airflow database and user
CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Create Staging database and user
CREATE DATABASE staging;
CREATE USER staging_user WITH ENCRYPTED PASSWORD 'staging_password';
GRANT ALL PRIVILEGES ON DATABASE staging TO staging_user;
GRANT ALL PRIVILEGES ON DATABASE staging TO postgres;

-- Create Telecom database and user
CREATE DATABASE telecom;
CREATE USER telecom_user WITH ENCRYPTED PASSWORD 'telecom_password';
GRANT ALL PRIVILEGES ON DATABASE telecom TO telecom_user;
GRANT ALL PRIVILEGES ON DATABASE telecom TO postgres;

-- Create Superset database and user
CREATE DATABASE superset;
CREATE USER superset WITH ENCRYPTED PASSWORD 'superset';
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;

-- Grant schema permissions for Airflow
\c airflow;
GRANT ALL ON SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;

-- Grant schema permissions for Staging
\c staging;
GRANT ALL ON SCHEMA public TO staging_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO staging_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO staging_user;
GRANT CREATE ON SCHEMA public TO staging_user;

-- Grant schema permissions for Telecom
\c telecom;
GRANT ALL ON SCHEMA public TO telecom_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO telecom_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO telecom_user;
GRANT CREATE ON SCHEMA public TO telecom_user;

-- Grant schema permissions for Superset
\c superset;
GRANT ALL ON SCHEMA public TO superset;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO superset;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO superset;
