# AWS Data Engineering Pipeline

An event-driven data lake pipeline built on AWS that automatically processes ecommerce orders data from raw CSV files to analytical ready tables in Redshift.

---

## Architecture

```
Raw CSV Files
     │
     ▼
┌─────────────────────────────────┐
│         Amazon S3               │
│  sarthak-de-raw-data/orders/    │
│  year=2024/month=01/day=15/     │
│  (Date Partitioned Data Lake)   │
└────────────────┬────────────────┘
                 │ S3 PUT Event
                 ▼
┌─────────────────────────────────┐
│         AWS Lambda              │
│   sarthak-glue-trigger          │
│   (Event Driven Orchestration)  │
└────────────────┬────────────────┘
                 │ Triggers
                 ▼
┌─────────────────────────────────┐
│         AWS Glue ETL            │
│   sarthak-orders-etl            │
│   (PySpark Transformation)      │
└────────────────┬────────────────┘
                 │ Writes Parquet
                 ▼
┌─────────────────────────────────┐
│         Amazon S3               │
│  sarthak-de-raw-data/processed/ │
│  (Processed Parquet Files)      │
└──────────┬──────────────────────┘
           │                │
           ▼                ▼
┌──────────────────┐  ┌─────────────────┐
│  Amazon Athena   │  │Amazon Redshift  │
│  (Ad hoc SQL     │  │Serverless       │
│   on S3)         │  │(Analytical DWH) │
└──────────────────┘  └─────────────────┘
```

---

## Services Used

| Service | Purpose |
|---|---|
| Amazon S3 | Data lake storage. Raw CSV landing zone and processed Parquet output |
| AWS IAM | Security layer. Roles and policies for every service |
| AWS Glue Crawler | Auto detects schema from S3 and registers in Glue Data Catalog |
| AWS Glue ETL | PySpark transformation job. Cleans data and writes Parquet |
| AWS Lambda | Event driven trigger. Fires Glue job when new CSV lands in S3 |
| Amazon Athena | Serverless SQL directly on S3 Parquet files |
| Amazon Redshift Serverless | Cloud data warehouse for analytical queries and BI dashboards |

---

## Pipeline Flow

### 1. Raw Data Landing
Raw ecommerce orders CSV files land in S3 in a date partitioned structure:
```
s3://sarthak-de-raw-data/orders/year=2024/month=01/day=15/orders_jan15.csv
s3://sarthak-de-raw-data/orders/year=2024/month=01/day=16/orders_jan16.csv
s3://sarthak-de-raw-data/orders/year=2024/month=01/day=17/orders_jan17.csv
```

Date partitioning enables partition pruning in Athena, reducing data scanned and query cost significantly.

### 2. Event Driven Trigger
An S3 PUT event automatically triggers the Lambda function `sarthak-glue-trigger` whenever a new CSV file is uploaded to the `orders/` prefix. Lambda checks if the file is a CSV in the orders folder and starts the Glue ETL job via boto3.

```python
def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    if key.startswith('orders/') and key.endswith('.csv'):
        glue = boto3.client('glue', region_name='ap-south-1')
        response = glue.start_job_run(JobName='sarthak-orders-etl')
```

### 3. Glue ETL Transformation
The Glue ETL job runs a PySpark script that:
- Reads all raw CSV files from S3
- Adds `order_category` column based on business logic (High Value / Mid Value / Low Value)
- Converts customer names to uppercase
- Filters out null order IDs
- Writes cleaned output as compressed Parquet to `processed/orders/`

```python
df = df.withColumn("order_category",
    when(col("amount") >= 20000, "High Value")
    .when(col("amount") >= 5000, "Mid Value")
    .otherwise("Low Value")
)

df.write.mode("overwrite").parquet("s3://sarthak-de-raw-data/processed/orders/")
```

### 4. Schema Detection with Glue Crawler
The Glue Crawler `sarthak-orders-crawler` scans S3, auto detects column names and data types, and registers the table in the Glue Data Catalog. The catalog is shared with Athena so the table is instantly queryable without any extra setup.

### 5. Athena for Ad Hoc Queries
Athena queries processed Parquet data directly from S3 with no data loading required. Partition pruning ensures only relevant date folders are scanned:

```sql
-- Partition pruning: only scans day=15 folder, skips all others
SELECT * FROM ecommerce_db.orders
WHERE year='2024' AND month='01' AND day='15';

-- Combined filter: partition pruning + row level filter
SELECT product, amount
FROM ecommerce_db.orders
WHERE year='2024' AND month='01' AND day='16'
AND amount > 1000;
```

### 6. Redshift for Analytical Queries
Processed Parquet files are loaded into Redshift Serverless using the COPY command for fast bulk loading. Redshift enables complex analytical queries and BI dashboard connections:

```sql
-- Load data from S3 Parquet into Redshift
COPY ecommerce.orders
FROM 's3://sarthak-de-raw-data/processed/orders/'
IAM_ROLE default
FORMAT AS PARQUET;

-- Analytical query: revenue by category
SELECT
    order_category,
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value
FROM ecommerce.orders
GROUP BY order_category
ORDER BY total_revenue DESC;
```

---

## S3 Bucket Structure

```
sarthak-de-raw-data/
├── orders/
│   └── year=2024/
│       └── month=01/
│           ├── day=15/
│           │   └── orders_jan15.csv
│           ├── day=16/
│           │   └── orders_jan16.csv
│           ├── day=17/
│           │   └── orders_jan17.csv
│           ├── day=18/
│           │   └── orders_jan18.csv
│           └── day=19/
│               └── orders_jan19.csv
├── processed/
│   └── orders/
│       └── part-00000-xxxx.snappy.parquet
│       └── part-00001-xxxx.snappy.parquet
├── athena-results/
└── raw-data/
    └── orders.csv
```

---

## Key Technical Decisions

**Why date partitioned S3 folders?**
Athena charges per data scanned. Date partitioning enables partition pruning where Athena skips irrelevant folders entirely. Querying one day in a three year dataset scans 1/1095th of the data instead of everything.

**Why Parquet instead of CSV for processed data?**
Parquet is columnar, meaning Athena reads only required columns instead of entire rows. Combined with Snappy compression it is significantly smaller than CSV. Athena queries on Parquet are faster and cheaper.

**Why Lambda instead of scheduled Glue trigger?**
Lambda makes the pipeline event driven. The moment a file lands in S3, processing starts automatically without waiting for a scheduled window. Reduces data latency from hours to seconds.

**Why Redshift alongside Athena?**
Athena is ideal for ad hoc exploration and pay per query workloads. Redshift is better for repeated complex analytical queries, aggregations across large datasets, and connecting BI tools like Power BI or Tableau for dashboards.

---

## IAM Roles Created

| Role | Service | Permissions |
|---|---|---|
| AWSGlueServiceRole-sarthak-glue-role | Glue | S3 Full Access, Glue Service Role |
| AmazonRedshift-CommandsAccessRole | Redshift | S3 Read Access |
| sarthak-glue-trigger-role | Lambda | Glue Service Role, S3 Read |

---

## Challenges and Debugging

**IAM S3 write permission denied on Glue job**
Glue role lacked S3 write permission. Fixed by attaching AmazonS3FullAccess policy to the Glue IAM role. Lesson: always verify IAM permissions when a service fails to access another service.

**Athena schema mismatch on Parquet files**
Parquet stored year, month, day columns as INT32 but external table defined them as STRING. Fixed by redefining the table with INT data types. Lesson: Parquet infers and stores types at write time. External table definition must match exactly.

**Glue partition column conflict**
CSV files had year, month, day columns AND the folder structure also had year=/month=/day= partitions. Glue detected a conflict. Fixed by using `recursiveFileLookup=true` option to read files without automatic partition detection.

**Lambda EntityNotFoundException**
Lambda could not find the Glue job because job name in code did not match exactly. Fixed by ensuring exact match between Lambda code JobName parameter and actual Glue job name.

---

## Tech Stack

- **Cloud**: AWS (ap-south-1 Mumbai)
- **Storage**: Amazon S3
- **ETL**: AWS Glue (PySpark)
- **Orchestration**: AWS Lambda (Python 3.12, boto3)
- **Query Engine**: Amazon Athena
- **Data Warehouse**: Amazon Redshift Serverless
- **Security**: AWS IAM
- **File Format**: Parquet (Snappy compression)

---

## Related Projects

- [Ecommerce Analytics Pipeline](https://github.com/Sarthak016/ecommerce-analytics-pipeline) — PySpark, Airflow, dbt, PostgreSQL, Docker
- [IPL Cricket Analytics](https://github.com/Sarthak016/ipl-cricket-analytics) — Snowflake, dbt, Python connector
- [InsightMind](https://insightmind-ai.streamlit.app) — Natural language to SQL using Claude API, Streamlit, Plotly
