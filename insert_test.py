import logging
from pyspark.sql import SparkSession

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Test logging messages
logging.info("This is an info message")
logging.warning("This is a warning message")
logging.error("This is an error message")

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Insert one row into your test table
try:
    spark.sql("""
    INSERT INTO smatracker_prod.bronze.test (person)
    VALUES ('Harry Hohl')
    """)
    logging.info("Row inserted successfully into smatracker_prod.bronze.test")
    test_secret = dbutils.secrets.get(scope="smatracker-prod", key="mysecret-testing")
    logging.info(f"Retrieved test secret: {test_secret}")
except Exception as e:
    logging.error(f"Failed to insert row: {e}")
