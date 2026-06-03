import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

st.set_page_config(page_title="Tableau de bord des salaires", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3); }
    .stDataFrame { border-radius: 10px; }
    h1, h2, h3 { color: #ffffff; font-family: 'Inter', sans-serif; }
    .metric-container { display: flex; justify-content: space-between; margin-bottom: 20px; }
    </style>
    """, unsafe_allow_html=True)


@st.cache_resource
def get_spark():
    return SparkSession.builder.appName("SalaryDashboard").getOrCreate()


def load_raw_data():
    spark = get_spark()
    input_csv = "hdfs://namenode:9000/datasets/job_salary_prediction_dataset.csv"
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_csv)
    for col_name in ["salary", "experience_years", "skills_count", "certifications"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))
    return df.dropna(subset=["salary"])


st.title("Tableau de bord des salaires")
st.markdown("### Exploration interactive des salaires par secteur et localisation.")


df = load_raw_data()

if df:
    df_all = df
    st.sidebar.header("Filtres du tableau de bord")
    all_industries = ["All"] + [row['industry'] for row in df.select("industry").distinct().collect() if row['industry']]
    selected_industry = st.sidebar.selectbox("Filtrer par secteur", all_industries)
    if selected_industry != "All":
        df = df.filter(F.col("industry") == selected_industry)

    st.subheader("Indicateurs clés")
    kpis = df.agg(F.count("*").alias("records"), F.avg("salary").alias("avg_salary"), F.avg("experience_years").alias("avg_exp"), F.min("salary").alias("min_salary"), F.max("salary").alias("max_salary")).collect()[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Nombre d'enregistrements", f"{kpis['records']:,}")
    with c2:
        st.metric("Salaire moyen", f"${kpis['avg_salary']:,.2f}")
    with c3:
        st.metric("Expérience moyenne", f"{kpis['avg_exp']:,.1f} ans")
    with c4:
        st.metric("Salaire minimum", f"${kpis['min_salary']:,.2f}")
    with c5:
        st.metric("Salaire maximum", f"${kpis['max_salary']:,.2f}")

    st.divider()

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Top 10 des postes par salaire moyen")
        top_jobs = df.groupBy("job_title").agg(F.avg("salary").alias("avg_salary")).orderBy(F.col("avg_salary").desc()).limit(10).toPandas()
        fig_jobs = px.bar(top_jobs, x="avg_salary", y="job_title", orientation='h', color="avg_salary", color_continuous_scale="Viridis", labels={"avg_salary": "Salaire moyen ($)", "job_title": "Intitulé du poste"}, template="plotly_dark")
        fig_jobs.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_jobs, use_container_width=True)

    with col_right:
        st.subheader("Top 10 des lieux par salaire moyen")
        top_locs = df.groupBy("location").agg(F.avg("salary").alias("avg_salary")).orderBy(F.col("avg_salary").desc()).limit(10).toPandas()
        fig_locs = px.bar(top_locs, x="avg_salary", y="location", orientation='h', color="avg_salary", color_continuous_scale="Plasma", labels={"avg_salary": "Salaire moyen ($)", "location": "Lieu"}, template="plotly_dark")
        fig_locs.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_locs, use_container_width=True)

    st.subheader("Salaire moyen par niveau d'études")
    edu_left, _ = st.columns(2)
    with edu_left:
        education_data = df.filter(F.col("education_level").isNotNull()).groupBy("education_level").agg(F.avg("salary").alias("avg_salary"), F.count("*").alias("records")).orderBy(F.col("avg_salary").desc()).toPandas()
        fig_education = px.bar(education_data, x="avg_salary", y="education_level", orientation='h', color="avg_salary", color_continuous_scale="Viridis", labels={"avg_salary": "Salaire moyen ($)", "education_level": "Niveau d'études"}, template="plotly_dark", hover_data={"records": True})
        fig_education.update_layout(yaxis={'categoryorder': 'total ascending'}, height=380)
        st.plotly_chart(fig_education, use_container_width=True)

    st.divider()
    st.subheader("Top 10 des secteurs par salaire moyen")
    ind_left, _ = st.columns(2)
    with ind_left:
        top_industries = df_all.groupBy("industry").agg(F.avg("salary").alias("avg_salary"), F.count("*").alias("records")).orderBy(F.col("avg_salary").desc()).limit(10).toPandas()
        fig_industries = px.bar(top_industries, x="avg_salary", y="industry", orientation="h", color="avg_salary", color_continuous_scale="Cividis", labels={"avg_salary": "Salaire moyen ($)", "industry": "Secteur"}, template="plotly_dark", hover_data={"records": True})
        fig_industries.update_layout(yaxis={'categoryorder': 'total ascending'}, height=380)
        st.plotly_chart(fig_industries, use_container_width=True)

    st.divider()
    st.subheader("Distribution des salaires et impact du télétravail")
    c_dist, c_remote = st.columns([2, 1])
    with c_dist:
        dist_data = df.select("salary").sample(0.1).toPandas()
        fig_dist = px.histogram(dist_data, x="salary", nbins=50, marginal="box", title="Distribution des salaires (échantillon)", color_discrete_sequence=['#00CC96'], template="plotly_dark")
        st.plotly_chart(fig_dist, use_container_width=True)

    with c_remote:
        remote_data = df.groupBy("remote_work").agg(F.avg("salary").alias("avg_salary")).toPandas()
        fig_remote = px.pie(remote_data, values="avg_salary", names="remote_work", title="Salaire moyen selon le télétravail", hole=0.4, template="plotly_dark")
        st.plotly_chart(fig_remote, use_container_width=True)

    st.subheader("Salaire moyen par nombre de certifications")
    cert_left, cert_right = st.columns(2)
    with cert_left:
        cert_data = df.filter(F.col("certifications").isNotNull()).groupBy("certifications").agg(F.avg("salary").alias("avg_salary"), F.count("*").alias("records")).orderBy(F.col("certifications").asc()).toPandas()
        fig_cert = px.bar(cert_data, x="certifications", y="avg_salary", color="avg_salary", color_continuous_scale="Plasma", labels={"certifications": "Nombre de certifications", "avg_salary": "Salaire moyen ($)"}, template="plotly_dark", hover_data={"records": True})
        fig_cert.update_layout(height=320)
        st.plotly_chart(fig_cert, use_container_width=True)

    with st.expander("Voir un échantillon des données brutes"):
        st.dataframe(df.limit(100).toPandas(), use_container_width=True)
else:
    st.warning("Veuillez vérifier la présence du jeu de données dans le répertoire du projet.")