import boto3
import json

def lambda_handler(event, context):
    """
    Triggered by S3 PUT events when a new CSV file lands in orders/ folder.
    Automatically starts the Glue ETL job to process the new data.
    """
    # Extract file details from S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(f"New file detected: s3://{bucket}/{key}")
    
    # Only trigger Glue if file is in orders/ folder and is a CSV
    if key.startswith('orders/') and key.endswith('.csv'):
        glue = boto3.client('glue', region_name='ap-south-1')
        
        response = glue.start_job_run(
            JobName='sarthak-orders-etl'
        )
        
        job_run_id = response['JobRunId']
        print(f"Glue job triggered successfully. Run ID: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Glue job started with run ID: {job_run_id}')
        }
    else:
        print(f"File {key} is not in orders folder or not a CSV. Skipping.")
        return {
            'statusCode': 200,
            'body': json.dumps('File skipped, not a CSV in orders folder')
        }
