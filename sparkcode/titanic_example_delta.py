from pyspark.sql import functions as f
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

from delta.tables import *

print("Reading CSV file from S3...")

schema = "PassengerId int, Survived int, Pclass int, Name string, Sex string, Age double, SibSp int, Parch int, Ticket string, Fare double, Cabin string, Embarked string"
df = spark.read.csv(
    "s3://dl-landing-zone-539445819060/titanic", 
    header=True, schema=schema, sep=";"
)

print("Writing titanic dataset as a delta table...")
df.write.format("delta").mode("overwrite").save("s3://dl-processing-zone-539445819060/silver/titanic_delta")

print("Updating and inserting new rows...")
new = df.where("PassengerId IN (1, 5)")
new = new.withColumn("Survived", f.lit(1))
newrows = [
    (892, 1, 1, "Sarah Crepalde", "female", 23.0, 1, 0, None, None, None, None),
    (893, 0, 1, "Ney Crepalde", "male", 35.0, 1, 0, None, None, None, None)
]
newrowsdf = spark.createDataFrame(newrows, schema=schema)
new = new.union(newrowsdf)

print("Create a delta table object...")
old = DeltaTable.forPath(spark, "s3://dl-processing-zone-539445819060/silver/titanic_delta")


print("UPSERT...")
# UPSERT
(
    old.alias("old")
    .merge(new.alias("new"), 
    "old.PassengerId = new.PassengerId"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Checking if everything is ok")
print("New data...")

(
    spark.read.format("delta")
    .load("s3://dl-processing-zone-539445819060/silver/titanic_delta")
    .where("PassengerId < 6 OR PassengerId > 888")
    .show()
)

print("Old data - with time travel")
(
    spark.read.format("delta")
    .option("versionAsOf", "0")
    .load("s3://dl-processing-zone-539445819060/silver/titanic_delta")
    .where("PassengerId < 6 OR PassengerId > 888")
    .show()
)

old.generate("symlink_format_manifest")
