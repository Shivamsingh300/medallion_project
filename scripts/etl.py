import os
import logging

from load_bronze import create_schema_and_tables, load_csv_to_table, SCHEMA as BRONZE_SCHEMA_NAME
from load_silver import (
    create_silver_schemas_and_tables,
    build_silver_layer,
    SILVER_TABLES,
    SILVER_SCHEMA,
)

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "etl.log")

# Logging to console + file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)

# Bronze configuration (do not change load_bronze.py; wrap here)
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


def load_all_bronze():
    create_schema_and_tables()
    for table, filename in FILES.items():
        file_path = os.path.join(BASE_PATH, filename)
        if os.path.exists(file_path):
            logging.info(f"Loading {file_path} into {BRONZE_SCHEMA}.{table}")
            load_csv_to_table(file_path, table)
        else:
            logging.warning(f"File not found: {file_path}")


def run_etl():
    logging.info("Starting ETL Pipeline")

    logging.info("Loading Bronze Layer")
    load_all_bronze()
    logging.info(f"Bronze Layer loaded into schema: {BRONZE_SCHEMA}")
    logging.info(f"Bronze Tables: {BRONZE_TABLES}")

    logging.info("Building Silver Layer")
    create_silver_schemas_and_tables()
    build_silver_layer()
    logging.info(f"Silver Layer built into schema: {SILVER_SCHEMA}")
    logging.info(f"Silver Tables: {list(SILVER_TABLES.keys())}")

    logging.info("ETL Pipeline Completed Successfully")


if __name__ == "__main__":
    run_etl()
