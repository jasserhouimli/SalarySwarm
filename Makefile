COMPOSE   := docker compose
NAMENODE  := docker exec namenode
SPARK     := docker exec spark-master
STREAMING := /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar

.DEFAULT_GOAL := help

.PHONY: help up down logs deps put-data spark mapreduce dashboard lint test clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Start the Hadoop + Spark cluster (12 containers)
	$(COMPOSE) up -d

down: ## Stop the cluster and remove containers
	$(COMPOSE) down

logs: ## Tail the logs of all services
	$(COMPOSE) logs -f

deps: ## Install the Python runtime dependencies inside the Spark container
	$(SPARK) pip install -r /opt/project/requirements.txt

put-data: ## Upload the datasets from the local filesystem into HDFS
	$(NAMENODE) hdfs dfs -mkdir -p /datasets
	$(NAMENODE) hdfs dfs -put -f /opt/project/datasets/job_salary_prediction_dataset.csv /datasets/
	$(NAMENODE) hdfs dfs -put -f /opt/project/purchases.txt /datasets/

spark: ## Run the PySpark batch analysis
	$(SPARK) /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/project/spark.py

mapreduce: ## Run the Hadoop Streaming MapReduce job (city sales count)
	$(NAMENODE) hdfs dfs -rm -r -f /output
	$(NAMENODE) yarn jar $(STREAMING) \
		-input /datasets/purchases.txt \
		-output /output \
		-mapper "python3 /opt/project/mapper.py" \
		-reducer "python3 /opt/project/reducer.py"

dashboard: ## Launch the Streamlit dashboard on http://localhost:8501
	$(SPARK) /opt/spark/bin/python3 -m streamlit run /opt/project/app.py \
		--server.port 8501 --server.address 0.0.0.0

lint: ## Run ruff against the Python sources
	ruff check .

test: ## Run the unit tests
	pytest -q

clean: ## Stop the cluster and delete generated caches
	$(COMPOSE) down
	rm -rf __pycache__ .pytest_cache .ruff_cache
