# Telecom Churn Insights Platform

A complete data engineering platform for telecom churn analysis using Apache Airflow, PostgreSQL, and Apache Superset.


## 📦 Services

| Service | Port | Description | Credentials |
|---------|------|-------------|-------------|
| **Airflow Webserver** | 8080 | DAG management & monitoring | `airflow` / `airflow` |
| **PostgreSQL** | 5432 | Data warehouse | `postgres` / `postgres` |
| **Superset** | 8088 | Data visualization | `admin` / `admin` |


## 🗄️ Databases

| Database | User | Password | Purpose |
|----------|------|----------|---------|
| `airflow` | `airflow` | `airflow` | Airflow metadata |
| `staging` | `staging_user` | `staging_password` | Raw data landing zone |
| `telecom` | `telecom_user` | `telecom_password` | Transformed data warehouse |


## 🚀 Quick Start

### Prerequisites

- Podman Desktop
- Podman CLI

### 1. Setup Environment

```bash
# Navigate to project directory
cd telecom-churn-insights

# Copy environment file
cp .env.example .env

# Set Airflow UID (Linux/Mac)
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Fix permissions on logs and data directories

chmod -R 777 airflow/logs
chmod -R 777 data

### 3. Start Services

```bash
# Start all containers
podman-compose up -d

# View logs
podman-compose logs -f
```

### 4. Access Services

- **Airflow**: http://localhost:8080 (username: `airflow`, password: `airflow`)
- **Superset**: http://localhost:8088 (username: `admin`, password: `admin`)
- **PostgreSQL**: `localhost:5432`

## 🔧 Configuration

### Adding Airflow Connections

After starting the containers, add database connections:

```bash
# Add staging database connection
podman exec -it telecom_airflow_webserver airflow connections add 'staging_db' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-schema 'staging' \
    --conn-login 'staging_user' \
    --conn-password 'staging_password' \
    --conn-port '5432'

# Add telecom database connection
podman exec -it telecom_airflow_webserver airflow connections add 'telecom_db' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-schema 'telecom' \
    --conn-login 'telecom_user' \
    --conn-password 'telecom_password' \
    --conn-port '5432'
```

### Connecting Superset to Databases

1. Login to Superset (http://localhost:8088)
2. Go to **Data** → **Databases** → **+ Database**
3. Select PostgreSQL
4. Use these connection strings:

**Staging Database:**
```
postgresql://staging_user:staging_password@postgres:5432/staging
```

**Telecom Database:**
```
postgresql://telecom_user:telecom_password@postgres:5432/telecom
```

## 🛠️ Common Commands

```bash
# Start all services
podman-compose up -d

# Stop all services
podman-compose down

# Stop and remove volumes (WARNING: deletes data)
podman-compose down -v

# Check service status
podman-compose ps
```
