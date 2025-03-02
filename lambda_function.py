import boto3
import json
import time
from botocore.exceptions import BotoCoreError, ClientError

dynamodb = boto3.client("dynamodb")

TABLE_NAME = "business_transactions"

# Maximum batch size per DynamoDB request (DynamoDB limit: 25)
BATCH_SIZE = 25
MAX_RETRIES = 5  # Retry up to 5 times
INITIAL_BACKOFF = 0.5  # Initial backoff in seconds


def batch_write_with_retry(table_name, items):
    """Writes items to DynamoDB in batches and retries if unprocessed."""
    chunks = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    
    for chunk in chunks:
        attempts = 0
        backoff = INITIAL_BACKOFF

        while attempts < MAX_RETRIES:
            try:
                # Batch Write to DynamoDB
                response = dynamodb.batch_write_item(RequestItems={table_name: chunk})

                # Check for unprocessed items
                unprocessed = response.get("UnprocessedItems", {}).get(table_name, [])
                if not unprocessed:
                    break  # Success

                # Update the chunk with unprocessed items and retry
                chunk = unprocessed
                attempts += 1
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

            except (BotoCoreError, ClientError) as e:
                print(f"DynamoDB Write Error: {e}")
                attempts += 1
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

        if attempts == MAX_RETRIES:
            # if retries limit reach, what do we do? Secondary DB? Push to "unprocessed logs" topic?
            print(f"Failed to process batch after {MAX_RETRIES} retries: {json.dumps(chunk)}")


def lambda_handler(event, context):
    """Lambda function entry point."""
    logs = event.get("records", {})

    items = []

    for topic_partition, messages in logs.items():
        for message in messages:
            # Extract partition and offset for reference
            partition = str(message.get("partition", "unknown"))
            offset = str(message.get("offset", "unknown"))

            # Decode Kafka message payload (assuming JSON format)
            kafka_message = json.loads(message["value"])

            # Extract UUID from producer's payload
            log_id = kafka_message.get("message_id")  # Producer should send this UUID
            
            if not log_id:
                print(f"Warning: Missing message_id for Kafka message at partition {partition}, offset {offset}")
                continue  # Skip messages without a valid UUID

            # Extract other fields
            transaction_type = kafka_message.get("transaction_type", "UNKNOWN")
            actor_id = kafka_message.get("actor_id", "UNKNOWN")
            target_id = kafka_message.get("target_id", "UNKNOWN")
            timestamp = kafka_message.get("timestamp", str(time.time()))  # Default to current time if missing

            # Convert message into DynamoDB format
            item = {
                "PutRequest": {
                    "Item": {
                        "log_id": {"S": log_id},  # UUID from producer (Primary Key)
                        "transaction_type": {"S": transaction_type},
                        "actor_id": {"S": actor_id},
                        "target_id": {"S": target_id},
                        "timestamp": {"S": timestamp}
                    }
                }
            }
            items.append(item)

    if items:
        batch_write_with_retry(TABLE_NAME, items)

    return {"status": "success", "processed_logs": len(items)}
