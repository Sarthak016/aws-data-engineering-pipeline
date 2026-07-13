-- Load processed Parquet data from S3 into Redshift
-- Uses IAM role for secure S3 access, no hardcoded credentials

COPY ecommerce.orders
FROM 's3://sarthak-de-raw-data/processed/orders/'
IAM_ROLE default
FORMAT AS PARQUET;
