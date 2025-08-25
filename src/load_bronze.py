import os
import psycopg2
import hashlib
import pandas as pd

# ✅ Database connection parameters
DB_CONFIG = {
    "dbname": "medallion",       # your database
    "user": "nineleaps",         # correct user
    "password": "newpassword",   # correct password
    "host": "localhost",
    "port": 5432,
}

SCHEMA = "bronze"

# ✅ Table creation statements
TABLES = {
    "students": """
        CREATE TABLE IF NOT EXISTS bronze.students (
            student_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            gender VARCHAR(10)
        )
    """,
    "courses": """
        CREATE TABLE IF NOT EXISTS bronze.courses (
            course_id SERIAL PRIMARY KEY,
            course_name VARCHAR(100),
            instructor VARCHAR(100)
        )
    """,
    "enrollment": """
        CREATE TABLE IF NOT EXISTS bronze.enrollment (
            enrollment_id SERIAL PRIMARY KEY,
            student_id INT,
            course_id INT,
            enroll_date DATE,
            FOREIGN KEY (student_id) REFERENCES bronze.students(student_id),
            FOREIGN KEY (course_id) REFERENCES bronze.courses(course_id)
        )
    """,
    "assessment": """
        CREATE TABLE IF NOT EXISTS bronze.assessment (
            assessment_id SERIAL PRIMARY KEY,
            student_id INT,
            course_id INT,
            attempt_date DATE,
            score INT,
            FOREIGN KEY (student_id) REFERENCES bronze.students(student_id),
            FOREIGN KEY (course_id) REFERENCES bronze.courses(course_id)
        )
    """,
    "payments": """
        CREATE TABLE IF NOT EXISTS bronze.payments (
            payment_id SERIAL PRIMARY KEY,
            student_id INT,
            amount DECIMAL(10,2),
            payment_date DATE,
            FOREIGN KEY (student_id) REFERENCES bronze.students(student_id)
        )
    """
}

# ✅ Compute MD5 checksum of file
def get_checksum(file_path):
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        hasher.update(f.read())
    return hasher.hexdigest()

# ✅ Ensure schema and tables exist
def create_schema_and_tables():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
        for ddl in TABLES.values():
            cur.execute(ddl)

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Schema '{SCHEMA}' and tables ensured.")
    except Exception as e:
        print(f"❌ Error creating schema/tables: {e}")
        exit()

# ✅ Load CSV into table with automatic date conversion
def load_csv_to_table(file_path, table_name):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        checksum = get_checksum(file_path)
        print(f"✅ Checksum for {table_name}: {checksum}")

        # Read CSV
        df = pd.read_csv(file_path)

        # Convert all columns containing "date" in the name
        for col in df.columns:
            if "date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce', format="%m/%d/%Y")

        # Save to temporary CSV for COPY
        temp_file = file_path + "_fixed.csv"
        df.to_csv(temp_file, index=False)

        # Truncate table before loading
        cur.execute(f"TRUNCATE TABLE {SCHEMA}.{table_name} CASCADE;")

        # Load CSV into Postgres
        with open(temp_file, "r") as f:
            cur.copy_expert(f"COPY {SCHEMA}.{table_name} FROM STDIN WITH CSV HEADER", f)

        conn.commit()
        cur.close()
        conn.close()
        os.remove(temp_file)
        print(f"✅ Loaded {file_path} into {SCHEMA}.{table_name}")
    except Exception as e:
        print(f"❌ Error loading {table_name}: {e}")

if __name__ == "__main__":
    # Step 1: Ensure schema and tables exist
    create_schema_and_tables()

    # Step 2: Load CSV files
    base_path = "/home/nineleaps/medallion_project/bronze_inputs"
    files = {
        "students": "students.csv",
        "courses": "courses.csv",
        "enrollment": "enrollment.csv",
        "assessment": "assessment.csv",
        "payments": "payments.csv"
    }

    for table, filename in files.items():
        file_path = os.path.join(base_path, filename)
        if os.path.exists(file_path):
            print(f"📥 Loading {file_path} into {SCHEMA}.{table}")
            load_csv_to_table(file_path, table)
        else:
            print(f"⚠️ File not found: {file_path}")
