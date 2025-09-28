import logging
from pyspark.sql import SparkSession
import dbutils

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Test logging messages
logging.info("This is an info message")
logging.warning("This is a warning message")
logging.error("This is an error message")

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# DBFS tmp path for the clone
DBFS_TMP_PATH = "dbfs:/tmp/my_repo"

# Insert one row into your test table
try:
    spark.sql("""
    INSERT INTO smatracker_prod.bronze.test (person)
    VALUES ('Harry Hohl')
    """)
    logging.info("Row inserted successfully into smatracker_prod.bronze.test")
    logging.info("Try to clone git repo")
    dbutils.fs.mkdirs(DBFS_TMP_PATH)  # ensures parent exists
    
except Exception as e:
    logging.error(f"Failed to insert row: {e}")
