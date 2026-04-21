import boto3
import csv
from io import StringIO
from datetime import datetime
import pymysql
import os

s3 = boto3.client('s3')

# Env variables
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.environ['DB_NAME']

# Final schema (fixed)
TARGET_COLUMNS = ['id', 'customer_name', 'amount', 'city', 'processed_timestamp']

def lambda_handler(event, context):
    try:
        # --- S3 Info ---
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(f"Processing file: {key}")

        # infinite loop
        if key.startswith("processed/"):
            print("Skipping processed file")
            return

        # --- Read CSV from S3 ---
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        reader = csv.DictReader(StringIO(content))

        # --- Prepare output CSV ---
        output_buffer = StringIO()
        writer = csv.DictWriter(output_buffer, fieldnames=TARGET_COLUMNS)
        writer.writeheader()

        processed_time = datetime.utcnow().isoformat()

        cleaned_rows = []

        for row in reader:
            try:
                # --- Normalize & Clean Row ---
                clean_row = {
                    'id': int(row.get('id', 0)),
                    'customer_name': row.get('customer_name', '').strip(),
                    'amount': float(row.get('amount', 0)),
                    'city': row.get('city', '').strip(),
                    'processed_timestamp': processed_time
                }

                # Optional filter (example)
                if clean_row['amount'] <= 0:
                    continue

                writer.writerow(clean_row)
                cleaned_rows.append(clean_row)

            except Exception as row_error:
                print(f"Skipping bad row: {row} | Error: {row_error}")
                continue

        # --- Save to S3 (/processed/) ---
        new_key = f"processed/{key.split('/')[-1]}"

        s3.put_object(
            Bucket=bucket,
            Key=new_key,
            Body=output_buffer.getvalue()
        )

        print(f"Saved processed file: {new_key}")

        # --- Insert into RDS ---
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connect_timeout=5
        )

        cursor = connection.cursor()

        insert_query = """
            INSERT INTO salestable (id, customer_name, amount, city, processed_timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """

        data = [
            (
                row['id'],
                row['customer_name'],
                row['amount'],
                row['city'],
                row['processed_timestamp']
            )
            for row in cleaned_rows
        ]

        # Batch insert
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            cursor.executemany(insert_query, data[i:i+batch_size])
            connection.commit()

        cursor.close()
        connection.close()

        print("✅ Data inserted into RDS successfully")

    except Exception as e:
        print("❌ Error:", str(e))
        raise e
