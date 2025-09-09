import pandas as pd
from sqlalchemy import create_engine, text
import logging
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
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
    "dbname": os.getenv("LOCAL_DB_NAME", "medallion"),
    "user": os.getenv("LOCAL_DB_USER", "postgres"),
    "password": os.getenv("LOCAL_DB_PASSWORD", "password"),
    "host": os.getenv("LOCAL_DB_HOST", "localhost"),
    "port": os.getenv("LOCAL_DB_PORT", "5432"),
}

SILVER_SCHEMA = "silver"
LOCAL_GOLD_SCHEMA = "gold"   # for local Postgres
SUPABASE_SCHEMA = "public"  # used only for Supabase

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
            s.email,
            s.signup_date
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
            AVG(a.score) AS average_score,
            MAX(a.attempt_date) AS last_attempt_date
        FROM silver.assessment a
        JOIN silver.students s ON a.student_id = s.student_id
        GROUP BY a.student_id, s.name, s.email
        ORDER BY AVG(a.score) DESC;
    """,

"monthly_revenue_summary": """
        SELECT
            DATE_TRUNC('month', p.payment_date) AS payment_month,
            SUM(p.amount) AS total_monthly_revenue
        FROM silver.payments p
        GROUP BY 1
        ORDER BY payment_month;
    """,

    "top_students_by_spending": """
        SELECT
            p.student_id,
            s.name AS student_name,
            s.email,
            SUM(p.amount) AS total_spent,
            COUNT(DISTINCT p.course_id) AS courses_purchased_count,
            MAX(p.payment_date) AS last_payment_date
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
            COUNT(p.payment_id) AS total_payments,
            MIN(p.payment_date) AS first_payment_date,
            MAX(p.payment_date) AS last_payment_date
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
            COUNT(a.assessment_id) AS total_attempts,
            MAX(a.attempt_date) AS last_attempt_date
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
            ) AS percentage_of_payments,
            MAX(payment_date) AS last_payment_date
        FROM silver.payments
        GROUP BY method
        ORDER BY total_revenue DESC;
    """,

    "students_with_active_courses": """
        SELECT
            s.student_id,
            s.name AS student_name,
            COUNT(e.enrollment_id) AS active_courses_count,
            MAX(e.enroll_date) AS last_enroll_date
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
            COUNT(e.enrollment_id) AS courses_not_attempted_count,
            MAX(e.enroll_date) AS last_enroll_date
        FROM silver.students s
        JOIN silver.enrollment e ON s.student_id = e.student_id
        LEFT JOIN silver.assessment a 
            ON s.student_id = a.student_id AND e.course_id = a.course_id
        WHERE a.assessment_id IS NULL
        GROUP BY s.student_id, s.name
        ORDER BY courses_not_attempted_count DESC;
    """,

    "revenue_per_course": """
        SELECT
            c.course_name,
            c.category,
            SUM(p.amount) AS total_revenue,
            DATE_TRUNC('month', p.payment_date) AS payment_month
        FROM silver.courses c
        JOIN silver.payments p ON c.course_id = p.course_id
        GROUP BY c.course_name, c.category, DATE_TRUNC('month', p.payment_date)
        ORDER BY payment_month, total_revenue DESC;
    """,

    "dropout_rate_per_course": """
        SELECT
            c.course_name,
            COUNT(CASE WHEN e.status = 'dropped' THEN 1 END) AS dropped_students,
            COUNT(e.student_id) AS total_enrolled_students,
            ROUND(
                COUNT(CASE WHEN e.status = 'dropped' THEN 1 END) * 100.0 / 
                NULLIF(COUNT(e.student_id), 0), 2
            ) AS dropout_rate_percentage,
            MAX(e.enroll_date) AS last_enroll_date
        FROM silver.courses c
        JOIN silver.enrollment e ON c.course_id = e.course_id
        GROUP BY c.course_name
        ORDER BY dropout_rate_percentage DESC;
    """,

    "total_spending_per_student": """
        SELECT
            s.student_id,
            s.name AS student_name,
            SUM(p.amount) AS total_spent,
            MAX(p.payment_date) AS last_payment_date
        FROM silver.students s
        JOIN silver.payments p ON s.student_id = p.student_id
        GROUP BY s.student_id, s.name
        ORDER BY total_spent DESC;
    """,

    "pass_fail_rate": """
       -- gold.pass_fail_rate (one row per course, latest attempt per student)
WITH latest_attempt AS (
  SELECT
    a.course_id,
    a.student_id,
    a.score,
    a.attempt_date,
    ROW_NUMBER() OVER (
      PARTITION BY a.course_id, a.student_id
      ORDER BY a.attempt_date DESC, a.assessment_id DESC
    ) AS rn
  FROM silver.assessment a
),
per_student AS (
  SELECT
    course_id,
    student_id,
    CASE WHEN score >= 70 THEN 1 ELSE 0 END AS is_pass,
    CASE WHEN score < 70 THEN 1 ELSE 0 END AS is_fail
  FROM latest_attempt
  WHERE rn = 1
)
SELECT
  c.course_name,
  SUM(is_fail)    AS failing_students,
  SUM(is_pass)    AS passing_students,
  ROUND(100.0 * SUM(is_pass) / NULLIF(SUM(is_pass) + SUM(is_fail), 0), 2) AS pass_rate_percentage
FROM per_student ps
JOIN silver.courses c ON c.course_id = ps.course_id
GROUP BY c.course_name
ORDER BY pass_rate_percentage DESC, course_name;

    """
}

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
def create_gold_schema_local():
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


def build_gold_layer():
    """Build gold tables in both local Postgres (gold) and Supabase (public)."""
    logging.info("Starting gold layer build (local Silver → Local Gold + Supabase Public).")
    try:
        # Local engines
        local_url = (
            f"postgresql://{LOCAL_DB_CONFIG['user']}:{LOCAL_DB_CONFIG['password']}"
            f"@{LOCAL_DB_CONFIG['host']}:{LOCAL_DB_CONFIG['port']}/{LOCAL_DB_CONFIG['dbname']}"
        )
        local_engine = create_engine(local_url)

        # Supabase engines
        supabase_url = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        )
        supabase_engine = create_engine(supabase_url)

        # Drop old tables in both local gold and supabase
        with local_engine.begin() as conn:
            for table_name in GOLD_TABLES.keys():
                conn.execute(text(f"DROP TABLE IF EXISTS {LOCAL_GOLD_SCHEMA}.{table_name} CASCADE;"))
                logging.info(f"Dropped existing {table_name} table in local gold schema.")

        with supabase_engine.begin() as conn:
            for table_name in GOLD_TABLES.keys():
                conn.execute(text(f"DROP TABLE IF EXISTS {SUPABASE_SCHEMA}.{table_name} CASCADE;"))
                logging.info(f"Dropped existing {table_name} table in Supabase ({SUPABASE_SCHEMA}).")

        # For each query: read from silver → write to both targets
        for table_name, query in GOLD_TABLES.items():
            logging.info(f"Building {table_name}...")

            # Read from local silver schema
            df = pd.read_sql(query, local_engine)

            # Write to local gold schema
            df.to_sql(table_name, local_engine, schema=LOCAL_GOLD_SCHEMA, if_exists="replace", index=False)
            logging.info(f"Table {LOCAL_GOLD_SCHEMA}.{table_name} built with {len(df)} rows in local Postgres.")

            # Write to Supabase public schema
            df.to_sql(table_name, supabase_engine, schema=SUPABASE_SCHEMA, if_exists="replace", index=False)
            logging.info(f"Table {SUPABASE_SCHEMA}.{table_name} built with {len(df)} rows in Supabase.")

        logging.info("Gold layer build completed successfully in BOTH local and Supabase.")

    except Exception as e:
        logging.error(f"Error while building gold layer: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    create_gold_schema_local()
    build_gold_layer()