from pyspark.sql import functions as f
from pyspark.sql import SparkSession
import pandas as pd 

def write_parquet(view, local):
    view.show()
    (
    view
        .write
        .mode("overwrite")
        .format('parquet')
        .save(local)
    )

spark = (
    SparkSession.builder
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

url_dataset = 'C:/Users/srjne/OneDrive/Documentos/Estudos/git/Airflow-Covid-19/analises_iniciais/owid-covid-data.csv'
local_file = '/tmp/covid_dataset.csv'

spark.sparkContext.setLogLevel("WARN")

print("Reading CSV file from S3...")

dataset = (
    spark
    .read
    .csv(url_dataset, 
        header=True, sep=",", inferSchema = True)
)
dataset.printSchema()

dataset.createOrReplaceTempView('covid')

deaths_per_country = spark.sql("""
    select 
        continent,
        location,
        sum(new_deaths) as count_deaths
    from covid
    group by continent, location
""")

death_per_million_per_country = spark.sql("""
    select 
        continent,
        location,
        sum(new_deaths_per_million) as deaths_per_million
    from covid
    group by continent, location
""")

excess_mortality_per_million_per_country = spark.sql("""
    select 
        continent,
        location,
        AVG(excess_mortality_cumulative_per_million) as excess_mortality_cumulative_per_million
    from covid
    group by continent, location
""")

population_density_per_country = spark.sql("""
    select 
        continent,
        location,
        max(population_density) as population_density
    from covid
    group by continent, location
""")

total_vaccinations_per_million = spark.sql("""
    select 
        continent,
        location,
        max(total_vaccinations_per_hundred)*10000 as total_vaccinations_per_million
    from covid
    group by continent, location
""")

people_vaccinated_per_million = spark.sql("""
    select 
        continent,
        location,
        max(people_vaccinated_per_hundred)*10000 as people_vaccinated_per_million
    from covid
    group by continent, location
""")

write_parquet(view = deaths_per_country, local="output/deaths")
write_parquet(view = death_per_million_per_country, local="output/death_pm")
write_parquet(view = excess_mortality_per_million_per_country, local="output/excess_mortality_pm")
write_parquet(view = population_density_per_country, local="output/population_density")
write_parquet(view = total_vaccinations_per_million, local="output/vaccinations_pm")
write_parquet(view = people_vaccinated_per_million, local="output/people_vaccinated_pm")

