# Datasets

Both datasets are committed to the repository under `data/` and uploaded to
HDFS by `make put-data`.

## job_salary_prediction_dataset.csv

Tabular salary dataset used by both the PySpark job
(`analysis/salary_analysis.py`) and the Streamlit dashboard (`dashboard/app.py`).

- **Rows:** 250,000
- **Columns:** 10
- **Size:** ~17 MB
- **Source:** [Job Salary Prediction Dataset](https://www.kaggle.com/datasets/nalisha/job-salary-prediction-dataset)
  on Kaggle (`nalisha`).

| Column             | Type   | Description                          | Example            |
| ------------------ | ------ | ------------------------------------ | ------------------ |
| `job_title`        | string | Position name                        | `AI Engineer`      |
| `experience_years` | int    | Years of professional experience     | `10`               |
| `education_level`  | string | Highest education level              | `Bachelor`         |
| `skills_count`     | int    | Number of listed skills              | `2`                |
| `industry`         | string | Sector                               | `Healthcare`       |
| `company_size`     | string | Company size bucket                  | `Medium`           |
| `location`         | string | Country of the role                  | `India`            |
| `remote_work`      | string | Work arrangement                     | `Hybrid`           |
| `certifications`   | int    | Number of certifications held        | `2`                |
| `salary`           | int    | Annual salary in USD (target)        | `109413`           |

Distinct values observed for a few low-cardinality columns:

- `education_level`: `Bachelor`, `Diploma`, `High School`, `Master`, `PhD`
- `remote_work`: `Hybrid`, `No`, `Yes`

`experience_years`, `skills_count`, `certifications` and `salary` are cast to
`double` before analysis, and rows without a `salary` are dropped.

## purchases.txt

Tab-separated retail transaction log used by the Hadoop Streaming MapReduce job.
It follows the classic tutorials that count sales per city.

- **Rows:** 308,103
- **Format:** tab-separated, one transaction per line
- **Size:** ~16 MB
- **Cardinality:** 103 cities, 18 product categories, 5 payment methods

| Position | Field            | Example          |
| -------- | ---------------- | ---------------- |
| 1        | date (`YYYY-MM-DD`) | `2012-01-01`  |
| 2        | time (`HH:MM`)   | `09:00`          |
| 3        | city             | `San Jose`       |
| 4        | product category | `Men's Clothing` |
| 5        | amount           | `214.05`         |
| 6        | payment method   | `Amex`           |

Sample line:

```text
2012-01-01	09:00	San Jose	Men's Clothing	214.05	Amex
```

The mapper keys on field 3 (the city), so the MapReduce output is the number of
transactions per city.
