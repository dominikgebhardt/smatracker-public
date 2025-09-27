from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Insert one row into your test table
spark.sql("""
INSERT INTO smatracker_prod.bronze.test (person)
VALUES ('Peter Pan')
""")