"""PySpark batch analysis of the job-salary dataset stored in HDFS.

The script reads ``job_salary_prediction_dataset.csv`` from HDFS, cleans the
typed columns, prints the KPIs and grouped aggregations used by the project
report, and writes the aggregated results back to HDFS as Parquet so they can
be reused without rescanning the raw CSV.

Run it with ``make spark`` (or ``spark-submit`` inside the ``spark-master``
container):
    spark-submit --master spark://spark-master:7077 /opt/project/analysis/salary_analysis.py
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_INPUT_PATH = (
    "hdfs://namenode:9000/datasets/job_salary_prediction_dataset.csv"
)
DEFAULT_OUTPUT_PATH = "hdfs://namenode:9000/datasets/analytics"

#: Columns cast from string to double before the analysis.
NUMERIC_COLUMNS = ("salary", "experience_years", "skills_count", "certifications")


def build_spark_session(app_name: str = "Salary Analytics") -> SparkSession:
    """Create (or reuse) the Spark session used by the batch job."""
    shuffle_partitions = os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .getOrCreate()
    )


def load_dataset(spark: SparkSession, path: str) -> DataFrame:
    """Read the salary CSV from HDFS with a header and inferred schema."""
    return spark.read.csv(path, header=True, inferSchema=True)


def clean(df: DataFrame) -> DataFrame:
    """Cast the numeric columns and drop rows without a salary."""
    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast("double"))
    return df.dropna(subset=["salary"])


def kpis(df: DataFrame) -> DataFrame:
    """Overall record count and salary/experience indicators."""
    return df.agg(
        F.count("*").alias("total_records"),
        F.avg("salary").alias("average_salary"),
        F.avg("experience_years").alias("average_experience"),
        F.min("salary").alias("minimum_salary"),
        F.max("salary").alias("maximum_salary"),
    )


def average_salary_by(df: DataFrame, column: str) -> DataFrame:
    """Average salary (and record count) grouped by ``column``, richest first."""
    return (
        df.filter(F.col(column).isNotNull())
        .groupBy(column)
        .agg(
            F.avg("salary").alias("average_salary"),
            F.count("*").alias("records"),
        )
        .orderBy(F.col("average_salary").desc())
    )


def analyze(df: DataFrame) -> dict[str, DataFrame]:
    """Compute every aggregation once so it can be printed and persisted."""
    return {
        "kpis": kpis(df),
        "salary_by_job_title": average_salary_by(df, "job_title"),
        "salary_by_location": average_salary_by(df, "location"),
        "salary_by_education_level": average_salary_by(df, "education_level"),
        "salary_by_industry": average_salary_by(df, "industry"),
        "salary_by_remote_work": average_salary_by(df, "remote_work"),
        "salary_by_certifications": average_salary_by(df, "certifications"),
        "salary_by_experience": average_salary_by(df, "experience_years"),
        "salary_by_skills_count": average_salary_by(df, "skills_count"),
    }


def print_section(title: str, df: DataFrame, limit: int = 0) -> None:
    print(f"\n===== {title} =====")
    if limit > 0:
        df.show(limit, truncate=False)
    else:
        df.show(truncate=False)


def write_outputs(results: dict[str, DataFrame], output_path: str) -> None:
    """Persist the aggregation results to HDFS as Parquet."""
    for name, frame in results.items():
        destination = f"{output_path}/{name}"
        frame.write.mode("overwrite").parquet(destination)
        print(f"wrote {destination}")


def main() -> int:
    input_path = os.getenv("SALARY_DATASET_PATH", DEFAULT_INPUT_PATH)
    output_path = os.getenv("SALARY_OUTPUT_PATH", DEFAULT_OUTPUT_PATH)

    spark = build_spark_session()

    try:
        print(f"===== LOADING {input_path} =====")
        raw = load_dataset(spark, input_path)
    except Exception as exc:
        print(
            f"ERROR: could not read {input_path}: {exc}\n"
            "Start the cluster with `make up` and upload the data with "
            "`make put-data`.",
            file=sys.stderr,
        )
        return 1

    raw.printSchema()
    clean_df = clean(raw)

    results = analyze(clean_df)
    print_section("SAMPLE DATA", clean_df, limit=10)
    print_section("KPI ANALYSIS", results["kpis"])
    for name in (
        "salary_by_job_title",
        "salary_by_location",
        "salary_by_education_level",
        "salary_by_industry",
        "salary_by_remote_work",
        "salary_by_certifications",
        "salary_by_experience",
        "salary_by_skills_count",
    ):
        print_section(name.upper().replace("_", " "), results[name], limit=10)

    print_section("SALARY DISTRIBUTION", clean_df.select("salary").summary())

    print("\n===== WRITING RESULTS =====")
    write_outputs(results, output_path)

    print("\n===== ANALYSIS COMPLETED SUCCESSFULLY =====")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
