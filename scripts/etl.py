import os
import logging
import psycopg2
from dotenv import load_dotenv

from load_bronze import (
    create_schema_and_tables as create_bronze_schema_and_tables,
    load_csv_to_table,
    SCHEMA as BRONZE_SCHEMA_NAME,
)
from load_silver import (
    create_silver_schemas_and_tables,
    build_silver_layer,
    SILVER_TABLES,
    SILVER_SCHEMA,
)
from load_gold import (
    create_gold_schema_and_tables,
    build_gold_layer,
    GOLD_TABLES,   # ✅ removed GOLD_SCHEMA (no longer needed)
)

# =====================
# 🔹 Load Environment
# =====================
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("❌ Missing required environment variables for Supabase connection.")

SUPABASE_CONN_STR = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =====================
# 🔹 Project Paths & Logs
# =====================
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "etl.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)

# =====================
# 🔹 Bronze Config
# =====================
BRONZE_SCHEMA = BRONZE_SCHEMA_NAME
BRONZE_TABLES = ["students", "courses", "enrollment", "assessment", "payments"]

BASE_PATH = os.path.join(PROJECT_ROOT, "bronze_inputs")
FILES = {
    "students": "students.csv",
    "courses": "courses.csv",
    "enrollment": "enrollment.csv",
    "assessment": "assessment.csv",
    "payments": "payments.csv",
}

# =====================
# 🔹 Utility Functions
# =====================
def test_supabase_connection():
    """Check if Supabase DB is reachable before running Gold layer."""
    try:
        conn = psycopg2.connect(SUPABASE_CONN_STR)
        conn.close()
        logging.info("✅ Supabase connection successful.")
    except Exception as e:
        logging.error(f"❌ Supabase connection failed: {e}")
        raise


def load_all_bronze():
    """Load all CSVs into Bronze schema (local DB)."""
    create_bronze_schema_and_tables()
    for table, filename in FILES.items():
        file_path = os.path.join(BASE_PATH, filename)
        if os.path.exists(file_path):
            logging.info(f"📥 Loading {file_path} → {BRONZE_SCHEMA}.{table}")
            load_csv_to_table(file_path, table)
        else:
            logging.warning(f"⚠️ File not found: {file_path}")


def run_etl():
    """Run full ETL pipeline: Bronze (local) → Silver (local) → Gold (Supabase)."""
    logging.info("🚀 Starting ETL Pipeline")

    # Bronze
    logging.info("📥 Bronze Layer (local)")
    load_all_bronze()
    logging.info(f"✅ Bronze Layer ready → Schema: {BRONZE_SCHEMA}, Tables: {BRONZE_TABLES}")

    # Silver
    logging.info("⚙️ Silver Layer (local)")
    create_silver_schemas_and_tables()
    build_silver_layer()
    logging.info(f"✅ Silver Layer ready → Schema: {SILVER_SCHEMA}, Tables: {list(SILVER_TABLES.keys())}")

    # Gold (Supabase)
    logging.info("📊 Gold Layer (Supabase)")
    test_supabase_connection()
    logging.info(f"🔗 Connecting as {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    create_gold_schema_and_tables()
    build_gold_layer()
    logging.info(f"✅ Gold Layer ready → Tables: {list(GOLD_TABLES.keys())}")  # ✅ removed schema reference

    logging.info("🎉 ETL Pipeline Completed Successfully")


if __name__ == "__main__":
    run_etl()
