from pyspark.sql import SparkSession, Row
from datetime import datetime
import time
import uuid

def load_table_with_logging(table_name: str, csv_path: str, spark: SparkSession):
    start_time = datetime.now()
    job_name = f"load_{table_name}"
    status = "SUCCESS"

    try:
        df = spark.read.csv(csv_path, sep=';', header=True, inferSchema=True)
        df.write.mode("overwrite").saveAsTable(table_name)
    except Exception as e:
        status = "FAILURE"
        print(f"Error loading {table_name}: {e}")

    time.sleep(5)

    end_time = datetime.now()

    log_row = Row(
        id=str(uuid.uuid4()),
        job_name=job_name,
        start_time=start_time,
        end_time=end_time,
        status=status
    )
    log_df = spark.createDataFrame([log_row])
    log_df.write.mode("append").insertInto("logs.LOGS")