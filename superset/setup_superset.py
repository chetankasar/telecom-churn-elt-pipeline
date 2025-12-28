"""
Superset Setup Script
- Adds telecom database connection
- Creates dataset from customer_churn table
- Creates 2 meaningful charts
"""

import requests
import json
import time

# Superset configuration
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

# Database configuration
DB_NAME = "Telecom Database"
DB_CONNECTION = "postgresql://telecom_user:telecom_password@postgres:5432/telecom"
TABLE_NAME = "customer_churn"
SCHEMA = "public"


class SupersetClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        self._login(username, password)
    
    def _login(self, username, password):
        """Login and get access token"""
        # Get CSRF token
        response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
        if response.status_code == 200:
            self.csrf_token = response.json().get("result")
            self.session.headers.update({"X-CSRFToken": self.csrf_token})
        
        # Login
        login_data = {
            "username": username,
            "password": password,
            "provider": "db",
            "refresh": True
        }
        response = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json=login_data
        )
        
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            self.session.headers.update({
                "Authorization": f"Bearer {self.access_token}"
            })
            print("✓ Logged in successfully")
        else:
            raise Exception(f"Login failed: {response.text}")
    
    def _refresh_csrf(self):
        """Refresh CSRF token"""
        response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
        if response.status_code == 200:
            self.csrf_token = response.json().get("result")
            self.session.headers.update({"X-CSRFToken": self.csrf_token})
    
    def create_database(self, name, connection_string):
        """Create database connection"""
        self._refresh_csrf()
        
        # Check if database already exists
        response = self.session.get(f"{self.base_url}/api/v1/database/")
        if response.status_code == 200:
            databases = response.json().get("result", [])
            for db in databases:
                if db.get("database_name") == name:
                    print(f"✓ Database '{name}' already exists (ID: {db['id']})")
                    return db["id"]
        
        # Create new database
        payload = {
            "database_name": name,
            "sqlalchemy_uri": connection_string,
            "expose_in_sqllab": True,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": True,
            "allow_run_async": True,
            "extra": json.dumps({
                "allows_virtual_table_explore": True
            })
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/database/",
            json=payload
        )
        
        if response.status_code in [200, 201]:
            db_id = response.json().get("id")
            print(f"✓ Database '{name}' created (ID: {db_id})")
            return db_id
        else:
            print(f"✗ Failed to create database: {response.text}")
            return None
    
    def create_dataset(self, database_id, table_name, schema="public"):
        """Create dataset from table"""
        self._refresh_csrf()
        
        # Check if dataset already exists
        response = self.session.get(f"{self.base_url}/api/v1/dataset/")
        if response.status_code == 200:
            datasets = response.json().get("result", [])
            for ds in datasets:
                if ds.get("table_name") == table_name:
                    print(f"✓ Dataset '{table_name}' already exists (ID: {ds['id']})")
                    return ds["id"]
        
        # Create new dataset
        payload = {
            "database": database_id,
            "table_name": table_name,
            "schema": schema
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/dataset/",
            json=payload
        )
        
        if response.status_code in [200, 201]:
            dataset_id = response.json().get("id")
            print(f"✓ Dataset '{table_name}' created (ID: {dataset_id})")
            return dataset_id
        else:
            print(f"✗ Failed to create dataset: {response.text}")
            return None
    
    def create_chart(self, name, dataset_id, viz_type, params):
        """Create a chart"""
        self._refresh_csrf()
        
        # Check if chart already exists
        response = self.session.get(f"{self.base_url}/api/v1/chart/")
        if response.status_code == 200:
            charts = response.json().get("result", [])
            for chart in charts:
                if chart.get("slice_name") == name:
                    print(f"✓ Chart '{name}' already exists (ID: {chart['id']})")
                    return chart["id"]
        
        payload = {
            "slice_name": name,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": viz_type,
            "params": json.dumps(params)
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/chart/",
            json=payload
        )
        
        if response.status_code in [200, 201]:
            chart_id = response.json().get("id")
            print(f"✓ Chart '{name}' created (ID: {chart_id})")
            return chart_id
        else:
            print(f"✗ Failed to create chart: {response.text}")
            return None


def main():
    print("\n" + "="*50)
    print("Superset Setup Script")
    print("="*50 + "\n")
    
    # Initialize client
    try:
        client = SupersetClient(SUPERSET_URL, USERNAME, PASSWORD)
    except Exception as e:
        print(f"Error: {e}")
        return
    
    # Create database connection
    print("\n--- Creating Database Connection ---")
    db_id = client.create_database(DB_NAME, DB_CONNECTION)
    if not db_id:
        print("Failed to create database. Exiting.")
        return
    
    # Wait a moment for database to be ready
    time.sleep(2)
    
    # Create dataset
    print("\n--- Creating Dataset ---")
    dataset_id = client.create_dataset(db_id, TABLE_NAME, SCHEMA)
    if not dataset_id:
        print("Failed to create dataset. Exiting.")
        return
    
    # Wait a moment for dataset to be ready
    time.sleep(2)
    
    # Create Chart 1: Churn Distribution Pie Chart
    print("\n--- Creating Charts ---")
    
    pie_chart_params = {
        "viz_type": "pie",
        "groupby": ["Churn"],
        "metric": {
            "label": "COUNT(*)",
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)"
        },
        "row_limit": 100,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "label_type": "key_percent",
        "number_format": "SMART_NUMBER",
        "date_format": "smart_date",
        "extra_form_data": {},
        "dashboards": []
    }
    
    chart1_id = client.create_chart(
        name="Customer Churn Distribution",
        dataset_id=dataset_id,
        viz_type="pie",
        params=pie_chart_params
    )
    
    # Create Chart 2: Churn by Contract Type Bar Chart
    bar_chart_params = {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "ContractType",
        "groupby": ["Churn"],
        "metrics": [{
            "label": "count",
            "expressionType": "SQL", 
            "sqlExpression": "COUNT(*)"
        }],
        "row_limit": 1000,
        "color_scheme": "supersetColors",
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_title": "Contract Type",
        "y_axis_title": "Customer Count",
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "extra_form_data": {},
        "dashboards": []
    }
    
    chart2_id = client.create_chart(
        name="Churn by Contract Type",
        dataset_id=dataset_id,
        viz_type="echarts_timeseries_bar",
        params=bar_chart_params
    )
    
    print("\n" + "="*50)
    print("Setup Complete!")
    print("="*50)
    print(f"\n📊 View your charts at: {SUPERSET_URL}/chart/list/")
    print(f"📈 Database ID: {db_id}")
    print(f"📋 Dataset ID: {dataset_id}")
    if chart1_id:
        print(f"🥧 Pie Chart: {SUPERSET_URL}/explore/?slice_id={chart1_id}")
    if chart2_id:
        print(f"📊 Bar Chart: {SUPERSET_URL}/explore/?slice_id={chart2_id}")
    print()


if __name__ == "__main__":
    main()

