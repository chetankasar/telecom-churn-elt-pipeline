# ============================================
# Apache Superset Configuration
# ============================================

import os
from datetime import timedelta

# Superset specific config
ROW_LIMIT = 5000

# Flask App Builder configuration
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'telecom_superset_secret_key_change_in_production')

# Database connection for Superset metadata
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{os.environ.get('DATABASE_USER', 'superset')}:"
    f"{os.environ.get('DATABASE_PASSWORD', 'superset')}@"
    f"{os.environ.get('DATABASE_HOST', 'postgres')}:"
    f"{os.environ.get('DATABASE_PORT', '5432')}/"
    f"{os.environ.get('DATABASE_DB', 'superset')}"
)

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "ENABLE_EXPLORE_DRAG_AND_DROP": True,
}

# Webserver configuration
WEBSERVER_ADDRESS = '0.0.0.0'
WEBSERVER_PORT = 8088

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
}

# Celery configuration (optional - for async queries)
class CeleryConfig:
    broker_url = 'redis://redis:6379/0'
    imports = ('superset.sql_lab', 'superset.tasks')
    result_backend = 'redis://redis:6379/0'
    worker_prefetch_multiplier = 10
    task_acks_late = True
    task_annotations = {
        'sql_lab.get_sql_results': {
            'rate_limit': '100/s',
        },
    }

# CELERY_CONFIG = CeleryConfig  # Uncomment if using Redis

# SQL Lab settings
SQLLAB_TIMEOUT = 300
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300

# Enable SQL Lab
SQL_MAX_ROW = 100000

# Logging
ENABLE_PROXY_FIX = True

# Time zone
BABEL_DEFAULT_TIMEZONE = 'UTC'

# Available database engines for connections
PREFERRED_DATABASES = [
    'PostgreSQL',
]

# Alert/Report configuration (optional)
ALERT_REPORTS_NOTIFICATION_DRY_RUN = True

print("Superset configuration loaded successfully!")

