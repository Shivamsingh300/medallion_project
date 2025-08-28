import pandas as pd
from sqlalchemy import create_engine, text
import logging
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Supabase (Public) DB Config ---
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

# --- Local (Silver/Gold) DB Config ---
LOCAL_DB_CONFIG = {
    "dbname": os.getenv("LOCAL_DB_NAME", "medallion"),  # default name
    "user": os.getenv("LOCAL_DB_USER", "postgres"),
    "password": os.getenv("LOCAL_DB_PASSWORD", "password"),
    "host": os.getenv("LOCAL_DB_HOST", "localhost"),
    "port": os.getenv("LOCAL_DB_PORT", "5432"),
}

SILVER_SCHEMA = "silver"
LOCAL_GOLD_SCHEMA = "gold"   # used only for local Postgres
SUPABASE_SCHEMA = "public"   # used only for Supabase

# -----------------------------------------------------------------------------
# Gold Queries → becomes GOLD_TABLES
# -----------------------------------------------------------------------------
GOLD_TABLES = {
    "student_overview": """
        SELECT
            s.student_id,
            s.name,
            s.email,
            s.city,
            s.signup_date
        FROM silver.students s
        ORDER BY s.student_id;
    """,

    "students_with_no_payments": """
        SELECT DISTINCT
            s.student_id,
            s.name AS student_name,
            s.email
        FROM silver.students s
        LEFT JOIN silver.payments p ON s.student_id = p.student_id
        WHERE p.student_id IS NULL;
    """,

    "student_engagement": """
        WITH enrolled_students AS (
            SELECT DISTINCT student_id FROM silver.enrollment
        ),
        attempted_students AS (
            SELECT DISTINCT a.student_id
            FROM silver.assessment a
            JOIN silver.enrollment e ON a.student_id = e.student_id
        )
        SELECT
            (SELECT COUNT(*) FROM enrolled_students) AS total_enrolled_students,
            (SELECT COUNT(*) FROM attempted_students) AS students_attempted_assessment,
            ROUND(
                (SELECT COUNT(*)::NUMERIC FROM attempted_students) * 100.0 / 
                NULLIF((SELECT COUNT(*) FROM enrolled_students), 0), 2
            ) AS percentage_engaged;
    """,

    "top_students_by_score": """
        SELECT
            a.student_id,
            s.name AS student_name,
            s.email,
            AVG(a.score) AS average_score
        FROM silver.assessment a
        JOIN silver.students s ON a.student_id = s.student_id
        GROUP BY a.student_id, s.name, s.email
        ORDER BY AVG(a.score) DESC;
    """,

    "top_students_by_spending": """
        SELECT
            p.student_id,
            s.name AS student_name,
            s.email,
            SUM(p.amount) AS total_spent,
            COUNT(DISTINCT p.course_id) AS courses_purchased_count
        FROM silver.payments p
        JOIN silver.students s ON p.student_id = s.student_id
        GROUP BY p.student_id, s.name, s.email
        ORDER BY SUM(p.amount) DESC;
    """,

    "course_payments_summary": """
        SELECT
            c.course_id,
            c.course_name,
            SUM(p.amount) AS total_revenue,
            COUNT(p.payment_id) AS total_payments
        FROM silver.courses c
        JOIN silver.payments p ON c.course_id = p.course_id
        GROUP BY c.course_id, c.course_name
        ORDER BY total_revenue DESC;
    """,

    "course_assessment_summary": """
        SELECT
            c.course_id,
            c.course_name,
            AVG(a.score) AS avg_score,
            COUNT(a.assessment_id) AS total_attempts
        FROM silver.courses c
        JOIN silver.assessment a ON c.course_id = a.course_id
        GROUP BY c.course_id, c.course_name
        ORDER BY avg_score DESC;
    """,

    "payment_method_distribution": """
        SELECT
            method,
            COUNT(payment_id) AS payment_count,
            SUM(amount) AS total_revenue,
            ROUND(COUNT(payment_id) * 100.0 / 
                NULLIF((SELECT COUNT(*) FROM silver.payments), 0), 2
            ) AS percentage_of_payments
        FROM silver.payments
        GROUP BY method
        ORDER BY total_revenue DESC;
    """,

    "students_with_active_courses": """
        SELECT
            s.student_id,
            s.name AS student_name,
            COUNT(e.enrollment_id) AS active_courses_count
        FROM silver.students s
        JOIN silver.enrollment e ON s.student_id = e.student_id
        WHERE e.status = 'active'
        GROUP BY s.student_id, s.name
        ORDER BY active_courses_count DESC;
    """,

    "students_with_courses_not_attempted": """
        SELECT
            s.student_id,
            s.name AS student_name,
            COUNT(e.enrollment_id) AS courses_not_attempted_count
        FROM silver.students s
        JOIN silver.enrollment e ON s.student_id = e.student_id
        LEFT JOIN silver.assessment a ON s.student_id = a.student_id AND e.course_id = a.course_id
        WHERE a.assessment_id IS NULL
        GROUP BY s.student_id, s.name
        ORDER BY courses_not_attempted_count DESC;
    """,

    "revenue_per_course": """
        SELECT
            c.course_name,
            c.category,
            SUM(p.amount) AS total_revenue
        FROM silver.courses c
        JOIN silver.payments p ON c.course_id = p.course_id
        GROUP BY c.course_name, c.category
        ORDER BY total_revenue DESC;
    """,

    "dropout_rate_per_course": """
        SELECT
            c.course_name,
            COUNT(CASE WHEN e.status = 'dropped' THEN 1 END) AS dropped_students,
            COUNT(e.student_id) AS total_enrolled_students,
            ROUND(
                COUNT(CASE WHEN e.status = 'dropped' THEN 1 END) * 100.0 / 
                NULLIF(COUNT(e.student_id), 0), 2
            ) AS dropout_rate_percentage
        FROM silver.courses c
        JOIN silver.enrollment e ON c.course_id = e.course_id
        GROUP BY c.course_name
        ORDER BY dropout_rate_percentage DESC;
    """,

    "total_spending_per_student": """
        SELECT
            s.student_id,
            s.name AS student_name,
            SUM(p.amount) AS total_spent
        FROM silver.students s
        JOIN silver.payments p ON s.student_id = p.student_id
        GROUP BY s.student_id, s.name
        ORDER BY total_spent DESC;
    """,

    "pass_fail_rate": """
        SELECT
            c.course_name,
            COUNT(CASE WHEN a.score >= 70 THEN 1 END) AS passing_students,
            COUNT(CASE WHEN a.score < 70 THEN 1 END) AS failing_students,
            ROUND(
                COUNT(CASE WHEN a.score >= 70 THEN 1 END) * 100.0 / 
                NULLIF(COUNT(a.score), 0), 2
            ) AS pass_rate_percentage
        FROM silver.courses c
        JOIN silver.assessment a ON c.course_id = a.course_id
        GROUP BY c.course_name
        ORDER BY pass_rate_percentage DESC;
    """
}

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
def create_gold_schema_and_tables():
    """Ensure gold schema exists in local Postgres."""
    try:
        conn = psycopg2.connect(**LOCAL_DB_CONFIG)
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {LOCAL_GOLD_SCHEMA};")
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Schema '{LOCAL_GOLD_SCHEMA}' ensured in local Postgres.")
    except Exception as e:
        logging.error(f"Error creating local gold schema: {e}")
        raise


def validate_gold_data():
    """Validate data quality: local Silver vs Supabase Public."""
    try:
        supabase_url = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        )
        supabase_engine = create_engine(supabase_url)

        local_url = (
            f"postgresql://{LOCAL_DB_CONFIG['user']}:{LOCAL_DB_CONFIG['password']}"
            f"@{LOCAL_DB_CONFIG['host']}:{LOCAL_DB_CONFIG['port']}/{LOCAL_DB_CONFIG['dbname']}"
        )
        local_engine = create_engine(local_url)

        with supabase_engine.connect() as supabase_conn, local_engine.connect() as silver_conn:
            validations = {
                "Student count consistency": """
                    SELECT COUNT(DISTINCT student_id) as silver_students FROM silver.students
                """,
                "Total revenue reconciliation": """
                    SELECT COALESCE(SUM(amount), 0) as silver_total_revenue FROM silver.payments
                """
            }

            for check_name, query in validations.items():
                silver_result = pd.read_sql(query, silver_conn).to_dict("records")[0]
                if "students" in check_name:
                    gold_count = pd.read_sql(f"SELECT COUNT(DISTINCT student_id) as supabase_students FROM {SUPABASE_SCHEMA}.student_overview", supabase_conn).to_dict("records")[0]
                    logging.info(f"{check_name}: {silver_result} vs {gold_count}")
                else:
                    gold_revenue = pd.read_sql(f"SELECT COALESCE(SUM(total_revenue),0) as supabase_total_revenue FROM {SUPABASE_SCHEMA}.course_payments_summary", supabase_conn).to_dict("records")[0]
                    logging.info(f"{check_name}: {silver_result} vs {gold_revenue}")

    except Exception as e:
        logging.error(f"Error during validation: {e}")
        raise


def build_gold_layer():
    """Build gold tables: read from local Silver → write to Supabase Public."""
    logging.info("Starting gold layer build (local Silver → Supabase Public).")
    try:
        local_url = (
            f"postgresql://{LOCAL_DB_CONFIG['user']}:{LOCAL_DB_CONFIG['password']}"
            f"@{LOCAL_DB_CONFIG['host']}:{LOCAL_DB_CONFIG['port']}/{LOCAL_DB_CONFIG['dbname']}"
        )
        local_engine = create_engine(local_url)

        supabase_url = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        )
        supabase_engine = create_engine(supabase_url)

        with supabase_engine.begin() as conn:
            # Drop old Public tables in Supabase
            for table_name in GOLD_TABLES.keys():
                conn.execute(text(f"DROP TABLE IF EXISTS {SUPABASE_SCHEMA}.{table_name} CASCADE;"))
                logging.info(f"Dropped existing {table_name} table in Supabase ({SUPABASE_SCHEMA}).")

        # For each query: read from local → write to Supabase
        for table_name, query in GOLD_TABLES.items():
            logging.info(f"Building {table_name}...")

            df = pd.read_sql(query, local_engine)
            df.to_sql(table_name, supabase_engine, schema=SUPABASE_SCHEMA, if_exists="replace", index=False)

            logging.info(f"Table {SUPABASE_SCHEMA}.{table_name} built with {len(df)} rows in Supabase.")

        validate_gold_data()
        logging.info("Gold → Public layer build completed successfully with validation.")

    except Exception as e:
        logging.error(f"Error while building Supabase public layer: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    create_gold_schema_and_tables()
    build_gold_layer()
