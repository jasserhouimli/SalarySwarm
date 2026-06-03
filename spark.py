from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ============================================================
# SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("Salary Dashboard Analysis") \
    .getOrCreate()

# ============================================================
# LOAD DATA
# ============================================================

df = spark.read.csv(
    "hdfs://namenode:9000/datasets/job_salary_prediction_dataset.csv",
    header=True,
    inferSchema=True
)

# ============================================================
# PRINT SCHEMA
# ============================================================

print("\n===== DATASET SCHEMA =====")
df.printSchema()

print("\n===== COLUMN NAMES =====")
print(df.columns)

# ============================================================
# SHOW SAMPLE DATA
# ============================================================

print("\n===== SAMPLE DATA =====")
df.show(10, truncate=False)

# ============================================================
# DATA CLEANING
# ============================================================

# Convert numeric columns
for col_name in [
    "salary",
    "experience_years",
    "skills_count",
    "certifications"
]:
    if col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.col(col_name).cast("double")
        )

# Remove null salaries
clean_df = df.dropna(subset=["salary"])

print("\n===== CLEANED DATA =====")
clean_df.show(10, truncate=False)

# ============================================================
# KPI ANALYSIS
# ============================================================

print("\n===== KPI ANALYSIS =====")

clean_df.agg(
    F.count("*").alias("Total_Records"),
    F.avg("salary").alias("Average_Salary"),
    F.avg("experience_years").alias("Average_Experience"),
    F.min("salary").alias("Minimum_Salary"),
    F.max("salary").alias("Maximum_Salary")
).show()

# ============================================================
# TOP JOB TITLES
# ============================================================

print("\n===== TOP JOB TITLES BY AVERAGE SALARY =====")

clean_df.groupBy(
    "job_title"
).agg(
    F.avg("salary").alias("Average_Salary")
).orderBy(
    F.desc("Average_Salary")
).show(10, truncate=False)

# ============================================================
# TOP LOCATIONS
# ============================================================

print("\n===== TOP LOCATIONS BY AVERAGE SALARY =====")

clean_df.groupBy(
    "location"
).agg(
    F.avg("salary").alias("Average_Salary")
).orderBy(
    F.desc("Average_Salary")
).show(10, truncate=False)

# ============================================================
# EDUCATION LEVEL ANALYSIS
# ============================================================

print("\n===== EDUCATION LEVEL ANALYSIS =====")

clean_df.filter(
    F.col("education_level").isNotNull()
).groupBy(
    "education_level"
).agg(
    F.avg("salary").alias("Average_Salary"),
    F.count("*").alias("Records")
).orderBy(
    F.desc("Average_Salary")
).show(truncate=False)

# ============================================================
# INDUSTRY ANALYSIS
# ============================================================

print("\n===== TOP INDUSTRIES BY SALARY =====")

clean_df.groupBy(
    "industry"
).agg(
    F.avg("salary").alias("Average_Salary"),
    F.count("*").alias("Records")
).orderBy(
    F.desc("Average_Salary")
).show(10, truncate=False)

# ============================================================
# REMOTE WORK ANALYSIS
# ============================================================

print("\n===== REMOTE WORK ANALYSIS =====")

clean_df.groupBy(
    "remote_work"
).agg(
    F.avg("salary").alias("Average_Salary")
).show(truncate=False)

# ============================================================
# CERTIFICATION ANALYSIS
# ============================================================

print("\n===== CERTIFICATION ANALYSIS =====")

clean_df.filter(
    F.col("certifications").isNotNull()
).groupBy(
    "certifications"
).agg(
    F.avg("salary").alias("Average_Salary"),
    F.count("*").alias("Records")
).orderBy(
    "certifications"
).show(truncate=False)

# ============================================================
# EXPERIENCE ANALYSIS
# ============================================================

print("\n===== EXPERIENCE ANALYSIS =====")

clean_df.groupBy(
    "experience_years"
).agg(
    F.avg("salary").alias("Average_Salary")
).orderBy(
    "experience_years"
).show(truncate=False)

# ============================================================
# SKILLS ANALYSIS
# ============================================================

print("\n===== SKILLS COUNT ANALYSIS =====")

clean_df.groupBy(
    "skills_count"
).agg(
    F.avg("salary").alias("Average_Salary")
).orderBy(
    "skills_count"
).show(truncate=False)

# ============================================================
# SALARY DISTRIBUTION
# ============================================================

print("\n===== SALARY DISTRIBUTION =====")

clean_df.select("salary") \
    .describe() \
    .show()

# ============================================================
# RAW DATA SAMPLE
# ============================================================

print("\n===== RAW DATA SAMPLE =====")

clean_df.show(20, truncate=False)

# ============================================================
# FINISHED
# ============================================================

print("\n===== ANALYSIS COMPLETED SUCCESSFULLY =====")

spark.stop()