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

def read_parquet(local):
    dataset = (
        spark
        .read
        .parquet(local)
    )
    dataset.printSchema()
    return dataset


spark = (
    SparkSession.builder
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Criando variáveis de local das tabelas de indicadores
deaths_per_country = "output/deaths"
death_per_million_per_country = "output/death_pm"
excess_mortality_per_million_per_country = "output/excess_mortality_pm"
population_density_per_country = "output/population_density"
total_vaccinations_per_million = "output/vaccinations_pm"
people_vaccinated_per_million = "output/people_vaccinated_pm"

# Local de output da tabela final consolidada
tabela_final_local = "output/indicadores_consolidados"

print("Reading parquet files from S3...")

deaths_pc = read_parquet(deaths_per_country)
death_pm = read_parquet(death_per_million_per_country)
excess_mortality_pm = read_parquet(excess_mortality_per_million_per_country)
population_density_pc = read_parquet(population_density_per_country)
total_vaccinations_pm = read_parquet(total_vaccinations_per_million)
people_vaccinated_pm = read_parquet(people_vaccinated_per_million)

deaths_pc.createOrReplaceTempView('view_deaths_pc')
death_pm.createOrReplaceTempView('view_death_pm')
excess_mortality_pm.createOrReplaceTempView('view_excess_mortality_pm')
population_density_pc.createOrReplaceTempView('view_population_density_pc')
total_vaccinations_pm.createOrReplaceTempView('view_total_vaccinations_pm')
people_vaccinated_pm.createOrReplaceTempView('view_people_vaccinated_pm')

indicadores_consolidados = spark.sql("""
    select 
        vd_pc.continent,
        vd_pc.location,
        vd_pc.count_deaths,
        vd_pm.deaths_per_million,
        vem.excess_mortality_cumulative_per_million,
        vpd_pc.population_density,
        vtv_pm.total_vaccinations_per_million,
        vpv_pm.people_vaccinated_per_million

    from view_deaths_pc as vd_pc
    left join view_death_pm as vd_pm on (vd_pc.location = vd_pm.location)
    left join view_excess_mortality_pm as vem on (vd_pc.location = vem.location)
    left join view_population_density_pc as vpd_pc on (vd_pc.location = vpd_pc.location)
    left join view_total_vaccinations_pm as vtv_pm on (vd_pc.location = vtv_pm.location)
    left join view_people_vaccinated_pm as vpv_pm on (vd_pc.location = vpv_pm.location)

    order by deaths_per_million desc
""")

write_parquet(view = indicadores_consolidados, local=tabela_final_local)

