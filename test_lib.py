import logging
from pyspark.sql import SparkSession


logging.info("Trying to import mylib")
try:
    from mylib import hello
    logging.info(hello("World"))
    logging.info("Successfully imported mylib")
except ImportError:
    logging.error("Failed to import mylib")
