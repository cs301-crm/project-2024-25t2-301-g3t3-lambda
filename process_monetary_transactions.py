import json
import boto3
import psycopg2
from psycopg2 import sql
from datetime import datetime
import os
import urllib.parse

s3 = boto3.client('s3')

def get_secret():
    secret_name = os.getenv("DB_SECRET_ARN")
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

def _process_file_content(content):
    """ Custom logic for processing json file content """
    content = json.loads(content)
    for c in content:
        timestamp_str = c['timestamp']
        c['timestamp'] = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
    return content

def _write_to_db(rows, db_config):
    """ Insert data into the PostgreSQL RDS database """
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            database="user_db",
            user=db_config['username'],
            password=db_config['password'],
            port="5432"

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
        return {"statusCode": 500, "body": str(e)}
    
    finally:
        if conn:
            conn.close()

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))    

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        response = s3.get_object(Bucket=bucket, Key = key)
        file_content = response["Body"].read().decode('utf-8')
    except Exception as e:
        print(f"S3 error: {e}")
        return {"statusCode": 500, "body": str(e)}

    try:
        rows = _process_file_content(file_content)

        db_config = json.loads(get_secret())
        _write_to_db(rows, db_config)

    except Exception as e:
        print(f"Lambda error: {e}")
        return {"statusCode": 500, "body": str(e)}

    return