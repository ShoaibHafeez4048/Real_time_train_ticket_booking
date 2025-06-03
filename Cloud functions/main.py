import base64
import json
from google.cloud import bigquery

# Initialize BigQuery client globally
bq_client = bigquery.Client()
table_id = "smart-arc-459310-g9.train_ticket_booking.booking_data"

def pubsub_to_bq(event, context):
    """Triggered from a message on a Pub/Sub topic."""
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        row = json.loads(pubsub_message)

        # Insert row into BigQuery
        errors = bq_client.insert_rows_json(table_id, [row])
        if errors:
            print(f"BigQuery insert errors: {errors}")
        else:
            print("Insert successful:", row)

    except Exception as e:
        print(f"Error processing message: {e}")