# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7fe29a9b-1866-4fd9-8776-20c3ac61a624",
# META       "default_lakehouse_name": "Gold_LakeHouse",
# META       "default_lakehouse_workspace_id": "32338175-e0e6-4c7a-b3cf-225d1b46c410",
# META       "known_lakehouses": [
# META         {
# META           "id": "7a701f3d-b29e-4934-b58e-93cb5cd89308"
# META         },
# META         {
# META           "id": "7fe29a9b-1866-4fd9-8776-20c3ac61a624"
# META         },
# META         {
# META           "id": "83e7b47e-7c74-45e9-a96b-b66ae0bf51aa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

df_countries_silver = spark.read.table("silver_lakehouse.dbo.Countries_Social_Barriers")


df_countries_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.Fact_Social_Barriers")

print("✅ Tabela Gold criada !")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Definição do Dicionário de Mapeamento
data = [
    ("AFE", "Africa Eastern and Southern"), ("AFW", "Africa Western and Central"),
    ("ARB", "Arab World"), ("CEB", "Central Europe and the Baltics"),
    ("CHI", "Channel Islands"), ("CSS", "Caribbean small states"),
    ("EAP", "East Asia & Pacific (excluding high income)"), ("EAR", "Early-demographic dividend"),
    ("EAS", "East Asia & Pacific"), ("ECA", "Europe & Central Asia (excluding high income)"),
    ("ECS", "Europe & Central Asia"), ("EMU", "Euro area"),
    ("EUU", "European Union"), ("FCS", "Fragile and conflict affected situations"),
    ("HIC", "High income"), ("HPC", "Heavily indebted poor countries (HIPC)"),
    ("IBD", "IBRD only"), ("IBT", "IBRD & IDA total"),
    ("IDA", "IDA total"), ("IDB", "IDA blend"),
    ("IDX", "IDA only"), ("LAC", "Latin America & Caribbean (excluding high income)"),
    ("LCN", "Latin America & Caribbean"), ("LDC", "Least developed countries: UN classification"),
    ("LIC", "Low income"), ("LMC", "Lower middle income"),
    ("LMY", "Low & middle income"), ("LTE", "Late-demographic dividend"),
    ("MEA", "Middle East & North Africa (excluding high income)"), ("MIC", "Middle income"),
    ("MNA", "Middle East & North Africa"), ("NAC", "North America"),
    ("OED", "OECD members"), ("OSS", "Other small states"),
    ("PRE", "Pre-demographic dividend"), ("PSS", "Pacific island small states"),
    ("PST", "Post-demographic dividend"), ("SAS", "South Asia"),
    ("SSA", "Sub-Saharan Africa (excluding high income)"), ("SSF", "Sub-Saharan Africa"),
    ("SST", "Small states"), ("TEA", "East Asia & Pacific (IDA & IBRD countries)"),
    ("TEC", "Europe & Central Asia (IDA & IBRD countries)"), ("TLA", "Latin America & the Caribbean (IDA & IBRD countries)"),
    ("TMN", "Middle East & North Africa (IDA & IBRD countries)"), ("TSA", "South Asia (IDA & IBRD countries)"),
    ("TSS", "Sub-Saharan Africa (IDA & IBRD countries)"), ("UMC", "Upper middle income"),
    ("WLD", "World")
]

df_descricoes = spark.createDataFrame(data, ["Aggregate_Code", "Description"])

df_aggregates_silver = spark.read.table("silver_lakehouse.dbo.Global_Social_Barriers") \
    .withColumnRenamed("Country_Code", "Aggregate_Code")

df_gold_aggregates = df_aggregates_silver.join(df_descricoes, on="Aggregate_Code", how="inner") \
    .withColumn("Entity_Type", F.lit("Aggregate/Benchmark"))

df_gold_aggregates.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.Fact_Benchmarks")

print("✅ Tabela Gold de Agregados criada e mapeada!")
df_gold_aggregates.select("Aggregate_Code", "Description").distinct().show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_geo_silver = spark.read.table("silver_lakehouse.dbo.geography")

df_geo_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.Dim_Geography")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_geo_silver = spark.read.table("silver_lakehouse.dbo.Dim_Date")

df_geo_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.Dim_Date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Arredondar Fact_Social_Barriers (Estrutura WIDE)
# Temos de aplicar o arredondamento a cada coluna de métrica individualmente
df_social = spark.read.table("gold_lakehouse.dbo.Fact_Social_Barriers")

metric_columns = [
    "Female_Account_Ownership", "Internet_Access", "Literacy_Rate", 
    "School_Attendance", "Child_Mortality_Rate", "Life_Expectancy", "MPI"
]

for col_name in metric_columns:
    if col_name in df_social.columns:
        df_social = df_social.withColumn(col_name, F.round(F.col(col_name), 2))

df_social.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_lakehouse.dbo.Fact_Social_Barriers")
print("✅ Fact_Social_Barriers: Números arredondados.")


# 2. Arredondar Fact_Benchmarks (Estrutura LONG)
# Aqui é mais fácil, pois só existe uma coluna de valores: "Value"
df_bench = spark.read.table("gold_lakehouse.dbo.Fact_Benchmarks")

if "Value" in df_bench.columns:
    df_bench = df_bench.withColumn("Value", F.round(F.col("Value"), 2))

df_bench.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_lakehouse.dbo.Fact_Benchmarks")
print("✅ Fact_Benchmarks: Números arredondados.")

# Limpar cache para garantir que o Power BI vê os novos valores
spark.catalog.clearCache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar as fontes da Silver (Normalizando o nome para country_code_iso3 em todas)
df_gini = spark.read.table("silver_lakehouse.dbo.gini_index").select(
    F.col("Country_Code_Iso3").alias("country_code_iso3"), 
    "Year", 
    F.col("Value").alias("Gini_Index")
)

df_hdi = spark.read.table("silver_lakehouse.dbo.hdi").select(
    F.col("Country_Code_Iso3").alias("country_code_iso3"), 
    "Year", 
    F.col("Human_Development_Index").alias("HDI")
)

df_econ = spark.read.table("silver_lakehouse.dbo.economic_indicators").select(
    F.col("Country_Code_Iso3").alias("country_code_iso3"), 
    "Year", 
    "GDP_per_Capita", 
    "GDP_Annual_Growth_Pct", 
    "Inflation_CPI_Pct"
)

# 2. Unir as tabelas usando o nome comum
# Agora todas têm "country_code_iso3", por isso o join funciona perfeitamente
df_main = df_econ.join(df_gini, ["country_code_iso3", "Year"], "outer") \
                 .join(df_hdi, ["country_code_iso3", "Year"], "outer")

# 3. Arredondar e selecionar
df_final = df_main.select(
    "country_code_iso3",
    "Year",
    F.round("Gini_Index", 2).alias("Gini_Index"),
    F.round("GDP_per_Capita", 2).alias("GDP_per_Capita"),
    F.round("GDP_Annual_Growth_Pct", 2).alias("GDP_Annual_Growth_Pct"),
    F.round("Inflation_CPI_Pct", 2).alias("Inflation_CPI_Pct"),
    F.round("HDI", 3).alias("HDI")
)

# 4. Gravar na Gold
df_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.Fact_Macro_Indicators")

print("✅ Fact_Macro_Indicators criada com sucesso!")
df_final.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar a tabela Income_Share da Silver
df_wealth = spark.read.table("silver_lakehouse.dbo.income_share")

# 2. Identificar as colunas de métricas (Percentis de rendimento)
# Geralmente são: Income_Share_Lowest_20pct, Highest_20pct, etc.
# Vamos arredondar todas as colunas exceto as de identificação
exclude_cols = ["Country_Code_Iso3", "Year"]
metric_cols = [c for c in df_wealth.columns if c not in exclude_cols]

# 3. Criar a tabela final com arredondamento e renomear colunas
df_wealth_final = df_wealth.select(
    F.col("Country_Code_Iso3"),
    F.col("Year").cast("long"),
    *[F.round(F.col(c).cast("double"), 2).alias(c) for c in metric_cols]
)

# 4. Gravar na Gold como Fact_Wealth_Distribution
df_wealth_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.Fact_Wealth_Distribution")

print("✅ Fact_Wealth_Distribution criada com sucesso na Gold!")
df_wealth_final.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar o que já existe na Benchmarks e o que queremos adicionar (Macro Agregados)
df_bench_existente = spark.read.table("gold_lakehouse.dbo.Fact_Benchmarks")
df_macro = spark.read.table("gold_lakehouse.dbo.Fact_Macro_Indicators")
df_geo = spark.read.table("gold_lakehouse.dbo.Dim_Geography")

# 2. Isolar apenas os 51 agregados da Macro
valid_codes = df_geo.select("country_code_iso3").distinct()
df_macro_aggr = df_macro.join(valid_codes, ["country_code_iso3"], "left_anti") \
    .withColumnRenamed("country_code_iso3", "Aggregate_Code")

# 3. Fazer o MERGE (Outer Join)
# Isto vai juntar as colunas que já existiam com as novas colunas da Macro
# Se o Aggregate_Code e o Year coincidirem, ele junta na mesma linha.
df_bench_final = df_bench_existente.join(df_macro_aggr, ["Aggregate_Code", "Year"], "outer")

# 4. Limpeza: Remover a última coluna a mais (como pediste)
cols = df_bench_final.columns
df_bench_final = df_bench_final.drop(cols[-1])

# 5. Guardar com as novas colunas
df_bench_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.Fact_Benchmarks")

print("✅ Fact_Benchmarks atualizada!")
print(f"Novas colunas adicionadas: {[c for c in df_macro_aggr.columns if c not in ['Aggregate_Code', 'Year']]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_benchmarks = spark.read.table("gold_lakehouse.dbo.Fact_Benchmarks")

# Remover o Kosovo e as colunas indesejadas
df_benchmarks_clean = df_benchmarks.filter(
    F.col("Aggregate_Code") != "XKX"
).drop("Gini_Index", "MPI")

df_benchmarks_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.Fact_Benchmarks")

print("✅ Fact_Benchmarks limpa!")
print(f"Colunas restantes: {df_benchmarks_clean.columns}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_earnings_silver = spark.read.table("silver_lakehouse.dbo.monthly_employee_earnings")
df_geography = spark.read.table("gold_lakehouse.dbo.dim_geography")
df_fact_wealth = spark.read.table("gold_lakehouse.dbo.fact_wealth_distribution")


valid_years = df_fact_wealth.select(F.col("Year")).distinct()


df_earnings_prepared = df_earnings_silver \
    .withColumnRenamed("year", "Year") \
    .join(
        df_geography.select("country_code_numeric", "country_code_iso3"),
        on="country_code_numeric",
        how="inner"
    ) \
    .join(
        valid_years,
        on="Year",
        how="inner"
    ) \
    .select(
        F.col("country_code_iso3"),
        F.col("Year"),
        F.round(F.col("Value"), 2).alias("Monthly_Employee_Earnings")
    )


df_fact_final = df_fact_wealth.join(
    df_earnings_prepared,
    on=["country_code_iso3", "Year"],
    how="left"
)

# 5. Gravação
print("🚀 A atualizar Fact_Wealth_Distribution...")

df_fact_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.fact_wealth_distribution")

print("✅ Concluído! Monthly_Employee_Earnings integrado na Fact.")
df_fact_final.select("country_code_iso3", "Year", "Monthly_Employee_Earnings").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_fact_wealth_filtered = df_fact_final.filter(F.col("Year") >= 2010)

print("🚀 A filtrar Fact_Wealth_Distribution para anos >= 2010...")

df_fact_wealth_filtered.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.fact_wealth_distribution")

print("✅ Concluído! Tabela filtrada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_macro = spark.read.table("gold_lakehouse.dbo.fact_macro_indicators")
df_unemployment_silver = spark.read.table("silver_lakehouse.dbo.unemployment_rate")


df_unemployment_subset = df_unemployment_silver.select(
    "country_code_iso3", 
    "Year", 
    "Unemployment_Total"
)

if "Unemployment_Total" in df_macro.columns:
    df_macro = df_macro.drop("Unemployment_Total")

df_macro_final = df_macro.join(
    df_unemployment_subset,
    on=["country_code_iso3", "Year"],
    how="left"
)

print("🚀 A atualizar Fact_Macro_Indicators com a taxa de desemprego...")

df_macro_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.fact_macro_indicators")

print("✅ Concluído! Coluna Unemployment_Total integrada.")

df_macro_final.filter(F.col("Year") >= 2010).select(
    "country_code_iso3", "Year", "Unemployment_Total"
).show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_unemployment_silver = spark.read.table("silver_lakehouse.dbo.unemployment_rate")
df_fact_benchmark = spark.read.table("gold_lakehouse.dbo.fact_benchmarks")


df_unemployment_prepared = df_unemployment_silver \
    .filter(F.col("Year") >= 2010) \
    .select(
        F.col("country_code_iso3").alias("aggregate_code"),
        F.col("Year"),
        F.round(F.col("Unemployment_Total"), 2).alias("Unemployment_Rate")
    )


if "Unemployment_Rate" in df_fact_benchmark.columns:
    df_fact_benchmark = df_fact_benchmark.drop("Unemployment_Rate")


df_benchmark_final = df_fact_benchmark \
    .filter(F.col("Year") >= 2010) \
    .join(
        df_unemployment_prepared,
        on=["aggregate_code", "Year"],
        how="left"
    )


full_table_path = "gold_lakehouse.dbo.fact_benchmarks"
print(f"🚀 A atualizar {full_table_path} com dados de Desemprego (2010+)...")

df_benchmark_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(full_table_path)

print(f"✅ Concluído! Coluna Unemployment_Rate integrada na Benchmarks.")

df_validacao = df_benchmark_final.filter(F.col("Unemployment_Rate").isNotNull())
print(f"📊 Registos preenchidos encontrados: {df_validacao.count()}")
df_validacao.select("aggregate_code", "Year", "Unemployment_Rate").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_benchmark = spark.read.table("gold_lakehouse.dbo.fact_benchmarks")


metrics_to_check = [
    "Female_Account_Ownership",
    "Internet_Access",
    "Literacy_Rate",
    "School_Attendance",
    "Child_Mortality_Rate",
    "Life_Expectancy",
    "GDP_per_Capita",
    "GDP_Annual_Growth_Pct",
    "Inflation_CPI_Pct",
    "Unemployment_Rate"
]

existing_metrics = [c for c in metrics_to_check if c in df_benchmark.columns]


df_benchmark_clean = df_benchmark.dropna(how='all', subset=existing_metrics)

print(f"🧹 A limpar Fact_Benchmark...")
print(f"📉 Registos antes: {df_benchmark.count()}")
print(f"📈 Registos depois: {df_benchmark_clean.count()}")

df_benchmark_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.fact_benchmarks")

print("✅ Concluído! A tabela agora contém apenas anos e entidades com dados reais.")

df_benchmark_clean.select("Aggregate_Code", "Year", "GDP_per_Capita", "Unemployment_Rate").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar a tua tabela
df_geo = spark.read.table("gold_lakehouse.dbo.dim_geography")

# 2. Dicionário de REGIONS (Topo da Hierarquia)
regions_coords = {
    "Africa": (1.0, 17.0), "Americas": (15.0, -85.0), "Antarctica": (-75.0, 0.0),
    "Asia": (35.0, 90.0), "Europe": (50.0, 15.0), "Oceania": (-25.0, 140.0)
}

# 3. DICIONÁRIO COMPLETO (249 Países e Territórios)
# Organizado por ordem alfabética conforme a tua lista
all_countries_coords = {
    "Afghanistan": (33.9, 67.7), "Albania": (41.1, 20.2), "Algeria": (28.0, 1.6), "American Samoa": (-14.3, -170.1),
    "Andorra": (42.5, 1.5), "Angola": (-11.2, 17.8), "Anguilla": (18.2, -63.1), "Antarctica": (-75.0, 0.0),
    "Antigua and Barbuda": (17.1, -61.8), "Argentina": (-38.4, -63.6), "Armenia": (40.1, 45.0), "Aruba": (12.5, -70.0),
    "Australia": (-25.3, 133.8), "Austria": (47.5, 14.5), "Azerbaijan": (40.1, 47.6), "Bahamas": (25.0, -77.4),
    "Bahrain": (26.1, 50.5), "Bangladesh": (23.7, 90.4), "Barbados": (13.2, -59.5), "Belarus": (53.7, 28.0),
    "Belgium": (50.5, 4.5), "Belize": (17.2, -88.5), "Benin": (9.3, 2.3), "Bermuda": (32.3, -64.8),
    "Bhutan": (27.5, 90.4), "Bolivia (Plurinational State of)": (-16.3, -63.6), "Bonaire, Sint Eustatius and Saba": (12.2, -68.3),
    "Bosnia and Herzegovina": (43.9, 17.7), "Botswana": (-22.3, 24.7), "Bouvet Island": (-54.4, 3.4), "Brazil": (-14.2, -51.9),
    "British Indian Ocean Territory": (-6.0, 71.5), "British Virgin Islands": (18.4, -64.6), "Brunei Darussalam": (4.5, 114.7),
    "Bulgaria": (42.7, 25.5), "Burkina Faso": (12.2, -1.6), "Burundi": (-3.4, 29.9), "Cabo Verde": (16.0, -24.0),
    "Cambodia": (12.6, 104.9), "Cameroon": (7.4, 12.4), "Canada": (56.1, -106.3), "Cayman Islands": (19.3, -81.3),
    "Central African Republic": (6.6, 20.9), "Chad": (15.5, 18.7), "Chile": (-35.7, -71.5), "China": (35.9, 104.2),
    "China, Hong Kong Special Administrative Region": (22.3, 114.2), "China, Macao Special Administrative Region": (22.2, 113.5),
    "Christmas Island": (-10.5, 105.7), "Cocos (Keeling) Islands": (-12.2, 96.8), "Colombia": (4.6, -74.3), "Comoros": (-11.6, 43.3),
    "Congo": (-0.2, 15.8), "Cook Islands": (-21.2, -159.8), "Costa Rica": (9.7, -83.8), "Croatia": (45.1, 15.2),
    "Cuba": (21.5, -77.8), "Curaçao": (12.2, -69.0), "Cyprus": (35.1, 33.4), "Czechia": (49.8, 15.5), "Côte d’Ivoire": (7.5, -5.5),
    "Democratic People's Republic of Korea": (40.3, 127.5), "Democratic Republic of the Congo": (-4.0, 21.7), "Denmark": (56.3, 9.5),
    "Djibouti": (11.8, 42.6), "Dominica": (15.4, -61.4), "Dominican Republic": (18.7, -70.2), "Ecuador": (-1.8, -78.2),
    "Egypt": (26.8, 30.8), "El Salvador": (13.8, -88.9), "Equatorial Guinea": (1.6, 10.3), "Eritrea": (15.2, 39.8),
    "Estonia": (58.6, 25.0), "Eswatini": (-26.5, 31.5), "Ethiopia": (9.1, 40.5), "Falkland Islands (Malvinas)": (-51.8, -59.5),
    "Faroe Islands": (61.9, -6.9), "Fiji": (-17.7, 178.1), "Finland": (61.9, 25.7), "France": (46.2, 2.2),
    "French Guiana": (3.9, -53.1), "French Polynesia": (-17.7, -149.4), "French Southern Territories": (-49.2, 69.4), "Gabon": (-0.8, 11.6),
    "Gambia": (13.4, -15.3), "Georgia": (42.3, 43.4), "Germany": (51.2, 10.5), "Ghana": (7.9, -1.0), "Gibraltar": (36.1, -5.3),
    "Greece": (39.1, 21.8), "Greenland": (71.7, -42.6), "Grenada": (12.1, -61.7), "Guadeloupe": (16.2, -61.6), "Guam": (13.4, 144.8),
    "Guatemala": (15.8, -90.2), "Guernsey": (49.5, -2.6), "Guinea": (9.9, -9.7), "Guinea-Bissau": (11.8, -15.2), "Guyana": (4.9, -58.9),
    "Haiti": (18.9, -72.7), "Heard Island and McDonald Islands": (-53.1, 73.5), "Holy See": (41.9, 12.5), "Honduras": (15.2, -86.2),
    "Hungary": (47.2, 19.5), "Iceland": (64.9, -18.1), "India": (20.6, 78.9), "Indonesia": (-0.8, 113.9),
    "Iran (Islamic Republic of)": (32.4, 53.7), "Iraq": (33.2, 43.7), "Ireland": (53.4, -8.2), "Isle of Man": (54.2, -4.5),
    "Israel": (31.0, 34.9), "Italy": (41.9, 12.6), "Jamaica": (18.1, -77.3), "Japan": (36.2, 138.3), "Jersey": (49.2, -2.1),
    "Jordan": (30.6, 36.2), "Kazakhstan": (48.0, 66.9), "Kenya": (-0.02, 37.9), "Kiribati": (-3.4, -168.7), "Kuwait": (29.3, 47.5),
    "Kyrgyzstan": (41.2, 74.8), "Lao People's Democratic Republic": (19.9, 102.5), "Latvia": (56.9, 24.6), "Lebanon": (33.9, 35.9),
    "Lesotho": (-29.6, 28.2), "Liberia": (6.4, -9.4), "Libya": (26.3, 17.2), "Liechtenstein": (47.2, 9.5), "Lithuania": (55.2, 23.9),
    "Luxembourg": (49.8, 6.1), "Madagascar": (-18.8, 46.9), "Malawi": (-13.3, 34.3), "Malaysia": (4.2, 102.0),
    "Maldives": (3.2, 73.2), "Mali": (17.6, -3.9), "Malta": (35.9, 14.4), "Marshall Islands": (7.1, 171.2), "Martinique": (14.6, -61.0),
    "Mauritania": (21.0, -10.9), "Mauritius": (-20.3, 57.5), "Mayotte": (-12.8, 45.2), "Mexico": (23.6, -102.6),
    "Micronesia (Federated States of)": (7.4, 151.2), "Monaco": (43.7, 7.4), "Mongolia": (46.9, 103.8), "Montenegro": (42.7, 19.4),
    "Montserrat": (16.7, -62.2), "Morocco": (31.8, -7.1), "Mozambique": (-18.7, 35.5), "Myanmar": (21.9, 95.9),
    "Namibia": (-22.9, 18.5), "Nauru": (-0.5, 166.9), "Nepal": (28.4, 84.1), "Netherlands (Kingdom of the)": (52.1, 5.3),
    "New Caledonia": (-20.9, 165.6), "New Zealand": (-40.9, 174.9), "Nicaragua": (12.9, -85.2), "Niger": (17.6, 8.1),
    "Nigeria": (9.1, 8.7), "Niue": (-19.0, -169.9), "Norfolk Island": (-29.0, 167.9), "North Macedonia": (41.6, 21.7),
    "Northern Mariana Islands": (15.1, 145.7), "Norway": (60.5, 8.4), "Oman": (21.5, 56.0), "Pakistan": (30.4, 69.3),
    "Palau": (7.5, 134.6), "Panama": (8.5, -80.8), "Papua New Guinea": (-6.3, 143.9), "Paraguay": (-23.4, -58.4),
    "Peru": (-9.2, -75.0), "Philippines": (12.9, 121.8), "Pitcairn": (-24.7, -127.4), "Poland": (51.9, 19.1),
    "Portugal": (39.4, -8.2), "Puerto Rico": (18.2, -66.6), "Qatar": (25.3, 51.2), "Republic of Korea": (35.9, 127.7),
    "Republic of Moldova": (47.4, 28.4), "Romania": (45.9, 25.0), "Russian Federation": (61.5, 105.3), "Rwanda": (-2.0, 29.9),
    "Réunion": (-21.1, 55.5), "Saint Barthélemy": (17.9, -62.8), "Saint Helena": (-15.9, -5.7), "Saint Kitts and Nevis": (17.4, -62.8),
    "Saint Lucia": (13.9, -60.9), "Saint Martin (French Part)": (18.1, -63.0), "Saint Pierre and Miquelon": (46.9, -56.3),
    "Saint Vincent and the Grenadines": (12.9, -61.2), "Samoa": (-13.7, -172.1), "San Marino": (43.9, 12.5),
    "Sao Tome and Principe": (0.2, 6.6), "Saudi Arabia": (23.9, 45.1), "Senegal": (14.5, -14.5), "Serbia": (44.0, 21.0),
    "Seychelles": (-4.7, 55.5), "Sierra Leone": (8.5, -11.8), "Singapore": (1.3, 103.8), "Sint Maarten (Dutch part)": (18.0, -63.0),
    "Slovakia": (48.7, 19.7), "Slovenia": (46.1, 15.0), "Solomon Islands": (-9.6, 160.1), "Somalia": (5.2, 46.2),
    "South Africa": (-30.6, 22.9), "South Georgia and the South Sandwich Islands": (-54.4, -36.6), "South Sudan": (6.9, 31.3),
    "Spain": (40.5, -3.7), "Sri Lanka": (7.9, 80.7), "State of Palestine": (31.9, 35.2), "Sudan": (12.9, 30.2),
    "Suriname": (3.9, -56.0), "Svalbard and Jan Mayen Islands": (77.5, 23.6), "Sweden": (60.1, 18.6), "Switzerland": (46.8, 8.2),
    "Syrian Arab Republic": (34.8, 39.0), "Taiwan": (23.7, 121.0), "Tajikistan": (38.9, 71.2), "Thailand": (15.9, 100.9),
    "Timor-Leste": (-8.9, 125.7), "Togo": (8.6, 0.8), "Tokelau": (-9.2, -171.8), "Tonga": (-21.1, -175.2),
    "Trinidad and Tobago": (10.7, -61.2), "Tunisia": (33.9, 9.5), "Turkmenistan": (39.0, 59.5), "Turks and Caicos Islands": (21.7, -71.8),
    "Tuvalu": (-7.1, 177.6), "Türkiye": (39.0, 35.2), "Uganda": (1.4, 32.3), "Ukraine": (48.4, 31.2),
    "United Arab Emirates": (23.4, 53.8), "United Kingdom of Great Britain and Northern Ireland": (55.4, -3.4),
    "United Republic of Tanzania": (-6.3, 34.9), "United States Minor Outlying Islands": (19.3, -166.6),
    "United States Virgin Islands": (18.3, -64.9), "United States of America": (37.1, -95.7), "Uruguay": (-32.5, -55.8),
    "Uzbekistan": (41.4, 64.6), "Vanuatu": (-15.4, 166.9), "Venezuela (Bolivarian Republic of)": (6.4, -66.6),
    "Viet Nam": (14.0, 108.3), "Wallis and Futuna Islands": (-13.8, -176.2), "Western Sahara": (24.2, -12.9),
    "Yemen": (15.6, 48.5), "Zambia": (-13.1, 27.8), "Zimbabwe": (-19.0, 29.2), "Åland Islands": (60.2, 20.0)
}

# 4. Criar as Expressões de Latitude e Longitude (Região e País)
reg_lat_expr = F.when(F.col("region_name") == "Africa", 1.0)
reg_long_expr = F.when(F.col("region_name") == "Africa", 17.0)
for r, (la, lo) in regions_coords.items():
    reg_lat_expr = reg_lat_expr.when(F.col("region_name") == r, la)
    reg_long_expr = reg_long_expr.when(F.col("region_name") == r, lo)

c_lat_expr = F.lit(None).cast("double")
c_long_expr = F.lit(None).cast("double")
for country, (lat, lon) in all_countries_coords.items():
    c_lat_expr = F.when(F.col("country_or_area") == country, lat).otherwise(c_lat_expr)
    c_long_expr = F.when(F.col("country_or_area") == country, lon).otherwise(c_long_expr)

# 5. Aplicar à tabela e Gravar na Gold
df_final = df_geo.withColumn("reg_lat", reg_lat_expr) \
                 .withColumn("reg_long", reg_long_expr) \
                 .withColumn("country_lat", c_lat_expr) \
                 .withColumn("country_long", c_long_expr)

df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_lakehouse.dbo.dim_geography")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar a tabela Fact da Gold
df_fact = spark.read.table("Gold_Lakehouse.dbo.Fact_Macro_Indicators")

# 2. Carregar a tabela de População da Bronze
df_pop_bronze = spark.read.table("Bronze_LakeHouse.world_bank.population_migration")

# 3. Preparar os dados da Bronze (Ajustado aos nomes reais das colunas)
df_pop_clean = df_pop_bronze.select(
    F.col("Country_Code").alias("Pop_Country_Code"), # Nome temporário para o join
    F.col("Year").cast("int").alias("Pop_Year"),     # Nome temporário para o join
    F.col("Pop_Total_Count").cast("double")
)

# 4. Executar o Join
# Ligamos Country_Code_Iso3 (da Fact) com Pop_Country_Code (da Pop)
df_gold_enriched = df_fact.join(
    df_pop_clean,
    (df_fact.country_code_iso3 == df_pop_clean.Pop_Country_Code) & 
    (df_fact.Year == df_pop_clean.Pop_Year),
    how="left"
).drop("Pop_Country_Code", "Pop_Year") # Removemos as colunas repetidas

# 5. Gravar de volta na Gold
df_gold_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_Lakehouse.dbo.Fact_Macro_Indicators")

print("✅ Fact_Macro_Indicators atualizada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Limpar cache TOTAL do Spark para evitar metadados antigos
spark.catalog.clearCache()

# 2. Ler as tabelas (Usar nomes diferentes para os DataFrames ajuda a depurar)
df_bench_raw = spark.read.table("Gold_Lakehouse.dbo.Fact_Benchmarks")
df_pop_bronze = spark.read.table("Bronze_LakeHouse.world_bank.population_migration")

# 3. Preparar a População
df_pop_clean = df_pop_bronze.select(
    F.col("Country_Code").alias("Pop_CC"), 
    F.col("Year").cast("int").alias("Pop_YR"),
    F.col("Pop_Total_Count").cast("double")
)

# 4. Resolver nome da coluna de join (Benchmarks)
# IMPORTANTE: Se queres padronizar para 'Country_Code_Iso3', fazemos o rename ANTES do join
if "Aggregate_Code" in df_bench_raw.columns:
    df_bench_ready = df_bench_raw.withColumnRenamed("Aggregate_Code", "Country_Code_Iso3")
else:
    df_bench_ready = df_bench_raw

# Remover Pop_Total_Count se já existir para evitar colunas duplicadas
if "Pop_Total_Count" in df_bench_ready.columns:
    df_bench_ready = df_bench_ready.drop("Pop_Total_Count")

# 5. Executar o Join
df_final = df_bench_ready.join(
    df_pop_clean,
    (F.upper(F.col("Country_Code_Iso3")) == F.upper(F.col("Pop_CC"))) & 
    (F.col("Year") == F.col("Pop_YR")),
    how="left"
).drop("Pop_CC", "Pop_YR")

# 6. Filtros finais e Reordenar
df_final = df_final.filter(F.col("Year") >= 2010) \
                   .withColumn("Country_Code_Iso3", F.upper(F.col("Country_Code_Iso3")))

cols_primeiro = ["Country_Code_Iso3", "Year"]
outras_cols = [c for c in df_final.columns if c not in cols_primeiro]
df_final = df_final.select(cols_primeiro + outras_cols)

# 7. Gravar na Gold (O segredo está em não tentar ler e mostrar o DF antigo depois disto)
df_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_Lakehouse.dbo.Fact_Benchmarks")

# 8. RE-LER a tabela do disco para o Display
# Isto força o Spark a ler o novo schema que acabou de ser gravado
print("✅ Tabela Gold atualizada com sucesso!")
df_view = spark.read.table("Gold_Lakehouse.dbo.Fact_Benchmarks")
display(df_view.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar as tabelas
df_fact = spark.read.table("Gold_Lakehouse.dbo.Fact_Macro_Indicators")
df_geo = spark.read.table("silver_lakehouse.dbo.geography")

# 2. Padronizar para Upper Case (evita falhas de join por 'abc' vs 'ABC')
df_fact = df_fact.withColumn("Country_Code_Iso3", F.upper(F.col("Country_Code_Iso3")))
df_geo = df_geo.select(F.upper(F.col("Country_Code_Iso3")).alias("Country_Code_Iso3")).distinct()

# 3. Join de Limpeza: Mantém apenas países que existam na tabela Geography
# Isto remove automaticamente SSA, WLD, AFE, etc.
df_macro_clean = df_fact.join(df_geo, on="Country_Code_Iso3", how="inner")

# 4. Gravar na Gold com sobrescrita de Schema
df_macro_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_Lakehouse.dbo.Fact_Macro_Indicators")

# 5. REFRESH E DIAGNÓSTICO (O segredo para não dar erro)
spark.catalog.refreshTable("Gold_Lakehouse.dbo.Fact_Macro_Indicators")
df_final = spark.read.table("Gold_Lakehouse.dbo.Fact_Macro_Indicators")

print("🚀 Fact_Macro_Indicators sincronizada com a Geografia!")

# Comparação segura para ver o que foi expulso
codigos_antes = df_fact.select("Country_Code_Iso3").distinct()
codigos_depois = df_final.select("Country_Code_Iso3").distinct()
df_removidos = codigos_antes.subtract(codigos_depois)

if df_removidos.count() > 0:
    print(f"⚠️ Foram removidos {df_removidos.count()} códigos (Agregados/Regionais).")
    display(df_removidos)
else:
    print("✅ A tabela já estava limpa e sincronizada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 0. Limpar cache para evitar erros de esquema/metadados
spark.catalog.clearCache()

# 1. Carregar as tabelas
df_macro = spark.read.table("Gold_Lakehouse.dbo.Fact_Macro_Indicators")
df_geo = spark.read.table("silver_lakehouse.dbo.geography")

# 2. Filtro de Ano e Padronização de Caixa (Uppercase)
df_cleaned = df_macro.filter(F.col("Year") >= 2010) \
                     .withColumn("Country_Code_Iso3", F.upper(F.col("Country_Code_Iso3")))

# 3. Join para garantir que SÓ existem países (remove AFE, SSA, etc.)
df_final = df_cleaned.join(
    df_geo.select(F.upper(F.col("Country_Code_Iso3")).alias("Geo_Code")).distinct(),
    df_cleaned.Country_Code_Iso3 == F.col("Geo_Code"),
    how="inner"
).drop("Geo_Code")

# 4. Reordenar (Corrigido o erro do caractere estranho na linha 23)
cols_primeiro = ["Country_Code_Iso3", "Year"]
outras_cols = [c for c in df_final.columns if c not in cols_primeiro]
df_final = df_final.select(cols_primeiro + outras_cols) # <- Agora sem o símbolo '綁'

# 5. Gravar de volta
df_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_Lakehouse.dbo.Fact_Macro_Indicators")

# 6. Refresh e Diagnóstico
spark.catalog.refreshTable("Gold_Lakehouse.dbo.Fact_Macro_Indicators")
df_display = spark.read.table("Gold_Lakehouse.dbo.Fact_Macro_Indicators")

print("🧹 Limpeza concluída!")
print(f"✅ Nome da coluna mantido: 'Country_Code_Iso3'")
print(f"📅 Registos a partir de: {df_display.agg(F.min('Year')).collect()[0][0]}")

display(df_display.sort("Country_Code_Iso3", "Year").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar a tua dimensão atual
# Usamos Spark para ler a tabela de geografia da Gold
df_geo = spark.read.table("gold_lakehouse.dbo.dim_geography")

# 2. Mapeamento das Coordenadas das Regiões Intermédias
# Adicionamos as colunas de latitude e longitude baseadas no nome da região
df_final = df_geo.withColumn(
    "intermediate_lat",
    F.when(F.col("intermediate_region_name") == "Antarctica", -75.0)
     .when(F.col("intermediate_region_name") == "Australia and New Zealand", -30.0)
     .when(F.col("intermediate_region_name") == "Caribbean", 15.0)
     .when(F.col("intermediate_region_name") == "Central America", 13.0)
     .when(F.col("intermediate_region_name") == "Central Asia", 45.0)
     .when(F.col("intermediate_region_name") == "Eastern Africa", 1.0)
     .when(F.col("intermediate_region_name") == "Eastern Asia", 35.0)
     .when(F.col("intermediate_region_name") == "Eastern Europe", 50.0)
     .when(F.col("intermediate_region_name") == "Melanesia", -9.0)
     .when(F.col("intermediate_region_name") == "Micronesia", 7.0)
     .when(F.col("intermediate_region_name") == "Middle Africa", -1.0)
     .when(F.col("intermediate_region_name") == "Northern Africa", 25.0)
     .when(F.col("intermediate_region_name") == "Northern America", 45.0)
     .when(F.col("intermediate_region_name") == "Northern Europe", 60.0)
     .when(F.col("intermediate_region_name") == "Polynesia", -18.0)
     .when(F.col("intermediate_region_name") == "South America", -15.0)
     .when(F.col("intermediate_region_name") == "South-eastern Asia", 5.0)
     .when(F.col("intermediate_region_name") == "Southern Africa", -29.0)
     .when(F.col("intermediate_region_name") == "Southern Asia", 25.0)
     .when(F.col("intermediate_region_name") == "Southern Europe", 41.0)
     .when(F.col("intermediate_region_name") == "Western Africa", 14.0)
     .when(F.col("intermediate_region_name") == "Western Asia", 33.0)
     .when(F.col("intermediate_region_name") == "Western Europe", 48.0)
     .otherwise(0.0)
).withColumn(
    "intermediate_long",
    F.when(F.col("intermediate_region_name") == "Antarctica", 0.0)
     .when(F.col("intermediate_region_name") == "Australia and New Zealand", 140.0)
     .when(F.col("intermediate_region_name") == "Caribbean", -75.0)
     .when(F.col("intermediate_region_name") == "Central America", -85.0)
     .when(F.col("intermediate_region_name") == "Central Asia", 65.0)
     .when(F.col("intermediate_region_name") == "Eastern Africa", 38.0)
     .when(F.col("intermediate_region_name") == "Eastern Asia", 110.0)
     .when(F.col("intermediate_region_name") == "Eastern Europe", 35.0)
     .when(F.col("intermediate_region_name") == "Melanesia", 150.0)
     .when(F.col("intermediate_region_name") == "Micronesia", 155.0)
     .when(F.col("intermediate_region_name") == "Middle Africa", 18.0)
     .when(F.col("intermediate_region_name") == "Northern Africa", 15.0)
     .when(F.col("intermediate_region_name") == "Northern America", -100.0)
     .when(F.col("intermediate_region_name") == "Northern Europe", 15.0)
     .when(F.col("intermediate_region_name") == "Polynesia", -150.0)
     .when(F.col("intermediate_region_name") == "South America", -60.0)
     .when(F.col("intermediate_region_name") == "South-eastern Asia", 110.0)
     .when(F.col("intermediate_region_name") == "Southern Africa", 25.0)
     .when(F.col("intermediate_region_name") == "Southern Asia", 75.0)
     .when(F.col("intermediate_region_name") == "Southern Europe", 15.0)
     .when(F.col("intermediate_region_name") == "Western Africa", 1.0)
     .when(F.col("intermediate_region_name") == "Western Asia", 40.0)
     .when(F.col("intermediate_region_name") == "Western Europe", 6.0)
     .otherwise(0.0)
)

# 3. Gravar na Tabela Gold (Overwrite para atualizar as colunas)
df_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lakehouse.dbo.dim_geography")

print("✅ Dim_Geography atualizada com as coordenadas das regiões intermédias!")
display(df_final.select("intermediate_region_name", "intermediate_lat", "intermediate_long").distinct().limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar a tabela da camada Gold
df_gold = spark.read.table("gold_lakehouse.dbo.fact_wealth_distribution")

# 2. Diagnóstico: Ver exatamente o que existe para a Islândia
print("Verificando dados da Islândia:")
df_gold.filter(F.col("country_code_iso3") == "ISL") \
       .select("country_code_iso3", "Year", "Monthly_Employee_Earnings") \
       .orderBy("Year") \
       .show()

# 3. Solução: Filtrar outliers ou valores que parecem ser anuais/moeda errada
# O Luxemburgo (topo real) anda pelos 8.000, logo 15.000 é um limite seguro.
df_gold_final = df_gold.filter((F.col("Monthly_Employee_Earnings") < 15000) | (F.col("Monthly_Employee_Earnings").isNull()))

# 4. Gravar a tabela limpa ou usar este df_gold_final para o Power BI
# df_gold_final.write.mode("overwrite").saveAsTable("fact_wealth_distribution_clean")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar a tabela (ajusta o nome se necessário)
df_gold = spark.read.table("fact_wealth_distribution")

# 2. Aplicar a correção: se o valor for > 15000, vira NULL
df_gold_corrigido = df_gold.withColumn(
    "Monthly_Employee_Earnings",
    F.when(F.col("Monthly_Employee_Earnings") > 15000, F.lit(None))
     .otherwise(F.col("Monthly_Employee_Earnings"))
)

# 3. Dar OVERWRITE na tabela Gold
# ATENÇÃO: Isto vai atualizar a tabela original
df_gold_corrigido.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_wealth_distribution")

print("Sucesso: Valor da Islândia (2016) anulado e tabela Gold atualizada!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
