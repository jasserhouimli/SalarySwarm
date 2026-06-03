# BigDataProject

A full-stack big data pipeline combining Hadoop HDFS, Apache Spark, and Streamlit for salary data exploration and analytics, plus a classic MapReduce workflow for transaction processing.

## Architecture

```
docker-compose up
      |
      v
+------------------------------------------------------+
|  Hadoop Cluster (HDFS + YARN)                        |
|  - namenode (WebUI :9870, HDFS :9000)                |
|  - 3x datanode                                       |
|  - resourcemanager (WebUI :8088)                     |
|  - 3x nodemanager                                    |
|  - historyserver (WebUI :19888)                      |
+------------------------------------------------------+
|  Spark Cluster                                       |
|  - spark-master (Spark :7077, UI :8080, App :8501)  |
|  - 3x spark-worker                                   |
+------------------------------------------------------+
         |
         | mounts ./datasets/ -> HDFS & Spark
         v
  hdfs://namenode:9000/datasets/
         |
         +---> spark.py        Batch analysis (console)
         |
         +---> app.py          Streamlit dashboard
         |
         +---> mapper/reducer  MapReduce on purchases.txt
```

## Technology Stack

| Component           | Technology                                       |
| ------------------- | ------------------------------------------------ |
| Language            | Python 3                                         |
| Distributed Storage | Hadoop HDFS 3.2.1 (bde2020 images)               |
| Resource Management | Hadoop YARN 3.2.1                                |
| Distributed Compute | Apache Spark 3.5.1 (PySpark)                     |
| Dashboard           | Streamlit                                        |
| Charts              | Plotly (Express + Graph Objects)                 |
| Data Processing     | Pandas                                           |
| Containerisation    | Docker Compose (version 3.8)                     |
| MapReduce           | Native Hadoop streaming (mapper.py / reducer.py) |

## Prerequisites

- Docker and Docker Compose (v2 recommended)
- At least 8 GB RAM allocated to Docker
- A system capable of running 12 containers

## Project Structure

```
bigdata/
|-- app.py                    Streamlit salary dashboard
|-- spark.py                  PySpark batch analysis script
|-- mapper.py                 Hadoop MapReduce mapper (city count)
|-- reducer.py                Hadoop MapReduce reducer (city count)
|-- docker-compose.yml        12-service cluster definition
|-- purchases.txt             Transaction dataset (~15 MB)
|-- BigDataProject_Rapport.pdf  Project report (French)
|
|-- datasets/
|   |-- job_salary_prediction_dataset.csv   Salary dataset (~17 MB, 250K rows)
|
|-- .gitignore
|-- README.md
```

## Datasets

### job_salary_prediction_dataset.csv

Salary prediction dataset with 250,000 records and 9 features:

- `job_title` - Position name (Data Scientist, AI Engineer, etc.)
- `experience_years` - Years of professional experience
- `education_level` - High School, Diploma, Bachelor, Master, PhD
- `skills_count` - Number of listed skills
- `industry` - Healthcare, Finance, Technology, etc.
- `company_size` - Startup, Small, Medium, Large, Enterprise
- `location` - USA, India, Germany, UK, Canada, etc.
- `remote_work` - Yes, No, Hybrid
- `certifications` - Number of certifications held
- `salary` - Annual salary (target variable)

### purchases.txt

Transaction records (tab-separated):

- `date`, `time`, `city`, `product_category`, `amount`, `payment_method`

## Setup and Deployment

### 1. Start the Cluster

```bash
docker-compose up -d
```

This starts the entire 12-service environment. The first pull may take several minutes.

### 2. Copy Datasets to HDFS

```bash
# Copy the salary dataset
docker exec -it namenode hdfs dfs -mkdir -p /datasets
docker exec -it namenode hdfs dfs -put /datasets/job_salary_prediction_dataset.csv /datasets/

# Copy the purchases dataset (for MapReduce)
docker exec -it namenode hdfs dfs -put /purchases.txt /datasets/
```

### 3. Run the Spark Batch Analysis

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/data/spark.py
```

Output is printed to the console.

### 4. Run the MapReduce Job

```bash
docker exec -it namenode yarn jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -input /datasets/purchases.txt \
  -output /output \
  -mapper "python3 /datasets/mapper.py" \
  -reducer "python3 /datasets/reducer.py"
```

### 5. Launch the Streamlit Dashboard

```bash
docker exec -it spark-master streamlit run /opt/data/app.py
```

Open your browser to `http://localhost:8501`.

## Services and Ports

| Service         | Port  | Purpose             |
| --------------- | ----- | ------------------- |
| namenode        | 9870  | HDFS Web UI         |
| namenode        | 9000  | HDFS file system    |
| resourcemanager | 8088  | YARN Web UI         |
| historyserver   | 19888 | Job history Web UI  |
| spark-master    | 7077  | Spark master        |
| spark-master    | 8080  | Spark Web UI        |
| spark-master    | 8501  | Streamlit dashboard |

## Applications

### Streamlit Dashboard (app.py)

An interactive salary exploration dashboard with:

- Key performance indicators (record count, avg salary, avg experience, min/max salary)
- Industry filter in the sidebar
- Top 10 job titles by average salary
- Top 10 locations by average salary
- Average salary by education level
- Top 10 industries by average salary
- Salary distribution histogram with box plot
- Remote work and certification analysis
- Raw data sample viewer

### PySpark Analysis (spark.py)

A comprehensive batch analysis script that runs:

1. Schema inspection and data cleaning
2. KPI computation (count, min/max/avg salary, avg experience)
3. Grouped aggregations by job title, location, education, industry
4. Remote work, certification, experience, and skills analysis
5. Salary distribution statistics

### MapReduce (mapper.py / reducer.py)

A classic word-count-style MapReduce job that counts purchase transactions per city from `purchases.txt`.
