"""Interactive Streamlit dashboard for the job-salary dataset.

The dashboard reads the cleaned dataset from HDFS through Spark and renders
the KPIs and Plotly charts described in the project report.

Launch it with ``make dashboard`` (or ``streamlit run dashboard/app.py`` inside
the ``spark-master`` container).
"""
from __future__ import annotations

import os

import plotly.express as px
import streamlit as st
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATASET_PATH = os.getenv(
    "SALARY_DATASET_PATH",
    "hdfs://namenode:9000/datasets/job_salary_prediction_dataset.csv",
)

#: Columns cast from string to double before the analysis.
NUMERIC_COLUMNS = ("salary", "experience_years", "skills_count", "certifications")

st.set_page_config(
    page_title="Salary Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 20px; border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3); }
    .stDataFrame { border-radius: 10px; }
    h1, h2, h3 { color: #ffffff; font-family: 'Inter', sans-serif; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_spark() -> SparkSession:
    """Create (or reuse) the Spark session backing the dashboard."""
    return SparkSession.builder.appName("SalaryDashboard").getOrCreate()


@st.cache_resource
def load_dataset() -> DataFrame:
    """Read, cast and clean the salary dataset once per session."""
    df = (
        get_spark()
        .read.option("header", True)
        .option("inferSchema", True)
        .csv(DATASET_PATH)
    )
    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast("double"))
    return df.dropna(subset=["salary"])


@st.cache_data(show_spinner=False)
def distinct_values(column: str) -> list[str]:
    """Sorted distinct values of a column, evaluated once."""
    rows = load_dataset().select(column).distinct().collect()
    return sorted(row[column] for row in rows if row[column])


def horizontal_bar(
    pdf,
    x: str,
    y: str,
    label_x: str,
    label_y: str,
    scale: str,
    hover: dict | None = None,
    height: int = 380,
):
    """Build a dark-theme horizontal bar chart ordered by value."""
    fig = px.bar(
        pdf,
        x=x,
        y=y,
        orientation="h",
        color=x,
        color_continuous_scale=scale,
        labels={x: label_x, y: label_y},
        template="plotly_dark",
        hover_data=hover,
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        height=height,
        coloraxis_showscale=False,
    )
    return fig


st.title("Salary Analytics Dashboard")
st.caption(
    "Interactive exploration of salaries by role, location, education and industry."
)

try:
    df = load_dataset()
except Exception as exc:  # surfaced to the user with a recovery hint
    st.error(f"Could not read the dataset from HDFS: {exc}")
    st.info(
        "Start the cluster with `make up` and upload the data with "
        "`make put-data`, then reload this page."
    )
    st.stop()

df_all = df

st.sidebar.header("Filters")
selected_industry = st.sidebar.selectbox(
    "Industry", ["All"] + distinct_values("industry")
)
df = df_all if selected_industry == "All" else df_all.filter(
    F.col("industry") == selected_industry
)

st.subheader("Key indicators")
kpi = df.agg(
    F.count("*").alias("records"),
    F.avg("salary").alias("avg_salary"),
    F.avg("experience_years").alias("avg_experience"),
    F.min("salary").alias("min_salary"),
    F.max("salary").alias("max_salary"),
).collect()[0]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Records", f"{kpi['records']:,}")
c2.metric("Average salary", f"${kpi['avg_salary']:,.0f}")
c3.metric("Average experience", f"{kpi['avg_experience']:,.1f} years")
c4.metric("Minimum salary", f"${kpi['min_salary']:,.0f}")
c5.metric("Maximum salary", f"${kpi['max_salary']:,.0f}")

st.divider()

col_left, col_right = st.columns(2)
with col_left:
    st.subheader("Top 10 job titles by average salary")
    top_jobs = (
        df.groupBy("job_title")
        .agg(F.avg("salary").alias("avg_salary"))
        .orderBy(F.col("avg_salary").desc())
        .limit(10)
        .toPandas()
    )
    st.plotly_chart(
        horizontal_bar(
            top_jobs, "avg_salary", "job_title",
            "Average salary ($)", "Job title", "Viridis",
        ),
        use_container_width=True,
    )

with col_right:
    st.subheader("Top 10 locations by average salary")
    top_locations = (
        df.groupBy("location")
        .agg(F.avg("salary").alias("avg_salary"))
        .orderBy(F.col("avg_salary").desc())
        .limit(10)
        .toPandas()
    )
    st.plotly_chart(
        horizontal_bar(
            top_locations, "avg_salary", "location",
            "Average salary ($)", "Location", "Plasma",
        ),
        use_container_width=True,
    )

st.divider()

left, _ = st.columns(2)
with left:
    st.subheader("Average salary by education level")
    by_education = (
        df.filter(F.col("education_level").isNotNull())
        .groupBy("education_level")
        .agg(F.avg("salary").alias("avg_salary"), F.count("*").alias("records"))
        .orderBy(F.col("avg_salary").desc())
        .toPandas()
    )
    st.plotly_chart(
        horizontal_bar(
            by_education, "avg_salary", "education_level",
            "Average salary ($)", "Education level", "Viridis",
            hover={"records": True},
        ),
        use_container_width=True,
    )

st.divider()

left, _ = st.columns(2)
with left:
    st.subheader("Top 10 industries by average salary")
    by_industry = (
        df_all.groupBy("industry")
        .agg(F.avg("salary").alias("avg_salary"), F.count("*").alias("records"))
        .orderBy(F.col("avg_salary").desc())
        .limit(10)
        .toPandas()
    )
    st.plotly_chart(
        horizontal_bar(
            by_industry, "avg_salary", "industry",
            "Average salary ($)", "Industry", "Cividis",
            hover={"records": True},
        ),
        use_container_width=True,
    )

st.divider()

st.subheader("Salary distribution and remote-work impact")
c_dist, c_remote = st.columns([2, 1])
with c_dist:
    sample = df.select("salary").sample(0.1).toPandas()
    fig_dist = px.histogram(
        sample, x="salary", nbins=50, marginal="box",
        title="Salary distribution (10% sample)",
        color_discrete_sequence=["#00CC96"], template="plotly_dark",
    )
    st.plotly_chart(fig_dist, use_container_width=True)
with c_remote:
    by_remote = (
        df.groupBy("remote_work").agg(F.avg("salary").alias("avg_salary")).toPandas()
    )
    fig_remote = px.pie(
        by_remote, values="avg_salary", names="remote_work",
        title="Average salary by remote work", hole=0.4, template="plotly_dark",
    )
    st.plotly_chart(fig_remote, use_container_width=True)

st.subheader("Average salary by number of certifications")
cert_left, _ = st.columns(2)
with cert_left:
    by_certifications = (
        df.filter(F.col("certifications").isNotNull())
        .groupBy("certifications")
        .agg(F.avg("salary").alias("avg_salary"), F.count("*").alias("records"))
        .orderBy(F.col("certifications").asc())
        .toPandas()
    )
    fig_cert = px.bar(
        by_certifications, x="certifications", y="avg_salary", color="avg_salary",
        color_continuous_scale="Plasma",
        labels={"certifications": "Certifications", "avg_salary": "Average salary ($)"},
        template="plotly_dark", hover_data={"records": True},
    )
    fig_cert.update_layout(height=320, coloraxis_showscale=False)
    st.plotly_chart(fig_cert, use_container_width=True)

with st.expander("Preview a sample of the raw data"):
    st.dataframe(df.limit(100).toPandas(), use_container_width=True)
