import json
import boto3
import psycopg2
from psycopg2 import sql
from datetime import datetime
import os
import base64

S3_BUCKET = "scrooge-bank-g3t3-sftp-bucket"
# DB_HOST = "aurora-cluster.cluster-cdpu7odorewb.ap-southeast-1.rds.amazonaws.com"
# DB_NAME = "user_db" 
# DB_USER = "test"
# DB_PASSWORD = os.getenv('DB_PASSWORD')
# DB_PORT = "5342"

def get_secret():
    secret_name = "rds!cluster-a8789063-e4dc-4842-8b3c-fcd2058922e4" # changes everytime we terraform destroy/apply
    region_name = "ap-southeast-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        raise e

    secret = get_secret_value_response['SecretString']

    # Your code goes here.
    return secret

# Initialize AWS services
s3_client = boto3.client("s3")

def _process_file_content(content):
    """ Custom logic for processing json file content """
    content = json.loads(content)
    for c in content:
        timestamp_str = c['timestamp']
        c['timestamp'] = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
    return content

def _write_to_db(rows):
    """ Insert data into the PostgreSQL RDS database """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()
        
        for row in rows:
            try:
                insert_query = sql.SQL("""
                    INSERT INTO monetary_transaction (transaction_id, client_id, account_id, amount, status, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """)
                cursor.execute(insert_query, (
                    row['transaction_id'],
                    row['client_id'],
                    row['account_id'],
                    row['amount'],
                    row['status'],
                    row['timestamp']
                ))
                conn.commit()
            except psycopg2.IntegrityError as e:
                print(f"Skipping transaction {row['transaction_id']} due to integrity error: {e}")
                conn.rollback()
            except Exception as e:
                print(f"Error inserting transaction {row['transaction_id']}: {e}")
                conn.rollback() 

        cursor.close()
        conn.close()
        print("Data successfully inserted into RDS")

    except Exception as e:
        print(f"Database error: {e}")
        raise

def lambda_handler(event, context):
    """ Lambda entry point """
    secret = get_secret()
    DB_HOST = secret['host']
    DB_NAME = "user_db"
    DB_USER = secret['username']
    DB_PASSWORD = secret['password']
    DB_PORT = "5342"

    try:
        # Get the S3 object key from event
        for record in event["Records"]:
            s3_key = record["s3"]["object"]["key"]
            print(f"Processing file: {s3_key}")

            # Read file from S3
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            file_content = response["Body"].read().decode('utf-8')
            
            # Process and write to DB
            rows = _process_file_content(file_content)
            _write_to_db(rows)

            print(f"Finished processing {s3_key}")

        return {"statusCode": 200, "body": "Success"}
    
    except Exception as e:
        print(f"Lambda error: {e}")
        return {"statusCode": 500, "body": str(e)}