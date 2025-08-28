import pandas as pd
from sqlalchemy import create_engine, text
import logging
import psycopg2


# Database connection parameters
DB_CONFIG = {
   "dbname": "medallion",
   "user": "nineleaps",
   "password": "newpassword",
   "host": "localhost",
   "port": 5432,
}


BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"


# Silver table DDLs
SILVER_TABLES = {
   "students": """
       CREATE TABLE IF NOT EXISTS silver.students (
           student_id NUMERIC PRIMARY KEY,
           name TEXT,
           email TEXT,
           city TEXT,
           signup_date DATE
       )
   """,
   "courses": """
       CREATE TABLE IF NOT EXISTS silver.courses (
           course_id NUMERIC PRIMARY KEY,
           course_name TEXT,
           category TEXT,
           price NUMERIC,
           instructor TEXT
       )
   """,
   "enrollment": """
       CREATE TABLE IF NOT EXISTS silver.enrollment (
           enrollment_id NUMERIC PRIMARY KEY,
           student_id NUMERIC,
           course_id NUMERIC,
           enroll_date DATE,
           status TEXT,
           FOREIGN KEY (student_id) REFERENCES silver.students(student_id),
           FOREIGN KEY (course_id) REFERENCES silver.courses(course_id)
       )
   """,
   "assessment": """
       CREATE TABLE IF NOT EXISTS silver.assessment (
           assessment_id NUMERIC PRIMARY KEY,
           student_id NUMERIC,
           course_id NUMERIC,
           score NUMERIC,
           attempt_date DATE,
           FOREIGN KEY (student_id) REFERENCES silver.students(student_id),
           FOREIGN KEY (course_id) REFERENCES silver.courses(course_id)
       )
   """,
   "payments": """
       CREATE TABLE IF NOT EXISTS silver.payments (
           payment_id NUMERIC PRIMARY KEY,
           student_id NUMERIC,
           course_id NUMERIC,
           amount NUMERIC,
           payment_date DATE,
           method TEXT,
           FOREIGN KEY (student_id) REFERENCES silver.students(student_id),
           FOREIGN KEY (course_id) REFERENCES silver.courses(course_id)
       )
   """
}




def create_silver_schemas_and_tables():
   """Ensure silver and audit schemas exist, with required tables."""
   try:
       conn = psycopg2.connect(**DB_CONFIG)
       cur = conn.cursor()


       # Schemas
       cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};")
       cur.execute("CREATE SCHEMA IF NOT EXISTS audit;")


       # Audit table
       cur.execute("""
           CREATE TABLE IF NOT EXISTS audit.rejected_rows (
               id SERIAL PRIMARY KEY,
               table_name TEXT,
               rejected_data JSONB,
               reason TEXT,
               timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           );
       """)


       # Silver tables
       for ddl in SILVER_TABLES.values():
           cur.execute(ddl)


       conn.commit()
       cur.close()
       conn.close()
       logging.info(f"Schemas '{SILVER_SCHEMA}' and 'audit' ensured with required tables.")
   except Exception as e:
       logging.error(f"Error creating schemas/tables: {e}")
       raise




def build_silver_layer():
   """Build the silver layer from bronze with validation, cleaning, and rejection logging."""
   logging.info("Starting silver layer build.")
   try:
       db_url = (
           f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
           f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
       )
       engine = create_engine(db_url)


       def log_and_store_rejections(conn, table_name: str, reason: str, df_rejected: pd.DataFrame):
           if df_rejected.empty:
               return
           for _, row in df_rejected.iterrows():
               conn.execute(
                   text(
                       "INSERT INTO audit.rejected_rows (table_name, rejected_data, reason) "
                       "VALUES (:table_name, :data, :reason)"
                   ),
                   {"table_name": table_name, "data": row.to_json(), "reason": reason},
               )
           logging.warning(f"{len(df_rejected)} faulty rows in {table_name} - {reason}")


       with engine.begin() as conn:
           # Truncate silver tables
           for table in SILVER_TABLES.keys():
               conn.execute(text(f"TRUNCATE TABLE {SILVER_SCHEMA}.{table} CASCADE;"))
           logging.info("Silver tables truncated.")


           # Read from bronze
           df_students = pd.read_sql(f"SELECT * FROM {BRONZE_SCHEMA}.students", conn)
           df_courses = pd.read_sql(f"SELECT * FROM {BRONZE_SCHEMA}.courses", conn)
           df_enrollment = pd.read_sql(f"SELECT * FROM {BRONZE_SCHEMA}.enrollment", conn)
           df_assessment = pd.read_sql(f"SELECT * FROM {BRONZE_SCHEMA}.assessment", conn)
           df_payments = pd.read_sql(f"SELECT * FROM {BRONZE_SCHEMA}.payments", conn)


           # Numeric conversions
           numeric_cols = {
               "students": ["student_id"],
               "courses": ["course_id", "price"],
               "enrollment": ["enrollment_id", "student_id", "course_id"],
               "assessment": ["assessment_id", "student_id", "course_id", "score"],
               "payments": ["payment_id", "student_id", "course_id", "amount"],
           }
           for df, cols in [
               (df_students, numeric_cols["students"]),
               (df_courses, numeric_cols["courses"]),
               (df_enrollment, numeric_cols["enrollment"]),
               (df_assessment, numeric_cols["assessment"]),
               (df_payments, numeric_cols["payments"]),
           ]:
               for col in cols:
                   df[col] = pd.to_numeric(df[col], errors="coerce")


           # ✅ Date conversions (robust)
           date_cols = {
               "students": ["signup_date"],
               "enrollment": ["enroll_date"],
               "assessment": ["attempt_date"],
               "payments": ["payment_date"],
           }
           for df, cols in [
               (df_students, date_cols["students"]),
               (df_enrollment, date_cols["enrollment"]),
               (df_assessment, date_cols["assessment"]),
               (df_payments, date_cols["payments"]),
           ]:
               for col in cols:
                   # pandas now defaults to inferring the format, so the argument is unnecessary
                   df[col] = pd.to_datetime(
                       df[col], errors="coerce"
                   )


           # De-duplication
           df_students.drop_duplicates(subset=["student_id"], inplace=True)
           df_courses.drop_duplicates(subset=["course_id"], inplace=True)
           df_enrollment.drop_duplicates(subset=["enrollment_id"], inplace=True)
           df_assessment.drop_duplicates(subset=["assessment_id"], inplace=True)
           df_payments.drop_duplicates(subset=["payment_id"], inplace=True)


           # Students checks
           rej = df_students[df_students["student_id"].isna()]
           log_and_store_rejections(conn, "students", "Non-numeric student_id", rej)
           df_students = df_students.drop(rej.index)


           rej = df_students[df_students["city"].isna() | (df_students["city"].str.strip() == "")]
           log_and_store_rejections(conn, "students", "Missing city", rej)
           df_students = df_students.drop(rej.index)


           rej = df_students[df_students["email"].str.strip() != df_students["email"]]
           log_and_store_rejections(conn, "students", "Whitespace in email", rej)
           df_students["email"] = df_students["email"].str.strip()


           rej = df_students[~df_students["name"].str.istitle()]
           log_and_store_rejections(conn, "students", "Invalid casing in name", rej)
           df_students["name"] = df_students["name"].str.title()


           # Courses checks
           rej = df_courses[df_courses["course_id"].isna()]
           log_and_store_rejections(conn, "courses", "Non-numeric course_id", rej)
           df_courses = df_courses.drop(rej.index)


           rej = df_courses[df_courses["price"].isna()]
           log_and_store_rejections(conn, "courses", "Non-numeric price", rej)
           df_courses = df_courses.drop(rej.index)


           rej = df_courses[~df_courses["category"].str.islower()]
           log_and_store_rejections(conn, "courses", "Invalid casing in category", rej)
           df_courses["category"] = df_courses["category"].str.lower()


           rej = df_courses[~df_courses["price"].between(50, 500)]
           log_and_store_rejections(conn, "courses", "Price out of range [50,500]", rej)
           df_courses = df_courses.drop(rej.index)


           # Enrollment checks
           rej = df_enrollment[df_enrollment["enrollment_id"].isna()]
           log_and_store_rejections(conn, "enrollment", "Non-numeric enrollment_id", rej)
           df_enrollment = df_enrollment.drop(rej.index)


           rej = df_enrollment[df_enrollment["student_id"].isna() | df_enrollment["course_id"].isna()]
           log_and_store_rejections(conn, "enrollment", "Non-numeric student_id/course_id", rej)
           df_enrollment = df_enrollment.drop(rej.index)


           valid_status = {"active", "completed", "dropped"}
           rej = df_enrollment[~df_enrollment["status"].isin(valid_status)]
           log_and_store_rejections(conn, "enrollment", "Invalid status enum", rej)
           df_enrollment = df_enrollment.drop(rej.index)


           # Assessment checks
           rej = df_assessment[df_assessment["assessment_id"].isna()]
           log_and_store_rejections(conn, "assessment", "Non-numeric assessment_id", rej)
           df_assessment = df_assessment.drop(rej.index)


           rej = df_assessment[df_assessment["student_id"].isna() | df_assessment["course_id"].isna()]
           log_and_store_rejections(conn, "assessment", "Non-numeric student_id/course_id", rej)
           df_assessment = df_assessment.drop(rej.index)


           rej = df_assessment[df_assessment["score"].isna()]
           log_and_store_rejections(conn, "assessment", "Non-numeric score", rej)
           df_assessment = df_assessment.drop(rej.index)


           rej = df_assessment[~df_assessment["score"].between(0, 100)]
           log_and_store_rejections(conn, "assessment", "Score out of range [0,100]", rej)
           df_assessment = df_assessment.drop(rej.index)


           # Payments checks
           rej = df_payments[df_payments["payment_id"].isna()]
           log_and_store_rejections(conn, "payments", "Non-numeric payment_id", rej)
           df_payments = df_payments.drop(rej.index)


           rej = df_payments[df_payments["student_id"].isna() | df_payments["course_id"].isna()]
           log_and_store_rejections(conn, "payments", "Non-numeric student_id/course_id", rej)
           df_payments = df_payments.drop(rej.index)


           rej = df_payments[df_payments["amount"].isna()]
           log_and_store_rejections(conn, "payments", "Non-numeric amount", rej)
           df_payments = df_payments.drop(rej.index)


           rej = df_payments[~df_payments["amount"].between(50, 500)]
           log_and_store_rejections(conn, "payments", "Amount out of range [50,500]", rej)
           df_payments = df_payments.drop(rej.index)


           # ✅ Normalize and validate payment method
           df_payments["method"] = df_payments["method"].astype(str).str.strip().str.title()
           valid_methods = {"Credit Card", "Debit Card", "Upi", "Netbanking", "Paypal", "Cash", "Bank Transfer"}
           rej = df_payments[~df_payments["method"].isin(valid_methods)]
           log_and_store_rejections(conn, "payments", "Invalid method enum", rej)
           df_payments = df_payments.drop(rej.index)


           # Foreign key checks
           valid_student_ids = set(df_students["student_id"])
           valid_course_ids = set(df_courses["course_id"])


           rej = df_enrollment[
               (~df_enrollment["student_id"].isin(valid_student_ids)) |
               (~df_enrollment["course_id"].isin(valid_course_ids))
           ]
           log_and_store_rejections(conn, "enrollment", "Invalid FK student_id/course_id", rej)
           df_enrollment = df_enrollment.drop(rej.index)


           rej = df_assessment[
               (~df_assessment["student_id"].isin(valid_student_ids)) |
               (~df_assessment["course_id"].isin(valid_course_ids))
           ]
           log_and_store_rejections(conn, "assessment", "Invalid FK student_id/course_id", rej)
           df_assessment = df_assessment.drop(rej.index)


           rej = df_payments[
               (~df_payments["student_id"].isin(valid_student_ids)) |
               (~df_payments["course_id"].isin(valid_course_ids))
           ]
           log_and_store_rejections(conn, "payments", "Invalid FK student_id/course_id", rej)
           df_payments = df_payments.drop(rej.index)


           # Load cleaned data into silver
           df_students.to_sql("students", conn, schema=SILVER_SCHEMA, if_exists="append", index=False)
           df_courses.to_sql("courses", conn, schema=SILVER_SCHEMA, if_exists="append", index=False)
           df_enrollment.to_sql("enrollment", conn, schema=SILVER_SCHEMA, if_exists="append", index=False)
           df_assessment.to_sql("assessment", conn, schema=SILVER_SCHEMA, if_exists="append", index=False)
           df_payments.to_sql("payments", conn, schema=SILVER_SCHEMA, if_exists="append", index=False)


       logging.info("Silver layer build completed successfully.")


   except Exception as e:
       logging.error(f"Error while building silver layer: {e}")
       raise




if __name__ == "__main__":
   logging.basicConfig(
       level=logging.INFO,
       format="%(asctime)s - %(levelname)s - %(message)s",
       handlers=[logging.StreamHandler()],
   )
   create_silver_schemas_and_tables()
   build_silver_layer()
