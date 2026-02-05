# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "83e7b47e-7c74-45e9-a96b-b66ae0bf51aa",
# META       "default_lakehouse_name": "Bronze_LakeHouse",
# META       "default_lakehouse_workspace_id": "32338175-e0e6-4c7a-b3cf-225d1b46c410",
# META       "known_lakehouses": [
# META         {
# META           "id": "83e7b47e-7c74-45e9-a96b-b66ae0bf51aa"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # (1) Create Delta Tables for the World Bank data for the Bronze Layer

# MARKDOWN ********************

# # (1.1) Educational Data

# MARKDOWN ********************

# ### (1.1.1) High-Level Educational Attainment across the global population aged 25 and older. Specifically, it tracks the "highest level of schooling completed" for three distinct tiers of tertiary (higher) education.

# CELL ********************

%pip install wbgapi

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.2) Development Data
# The indicators you've listed fall into two primary categories: Socio-economic Development and Infrastructure Performance.
# Specifically, these are used by organizations like the World Bank and the UN to measure a country's energy profile. Here is the breakdown of how they are classified:

# MARKDOWN ********************

# ## (1.3) Demographic Data
# ### 1. Component Breakdown
# #### The Population Pillars (Static Counts)
# - **Total Population:** The baseline count of all residents. This serves as the "denominator" for nearly every social and economic KPI (Key Performance Indicator).
# - **Gender Distribution (Male/Female):** Essential for identifying demographic imbalances. In data analysis, this is used to calculate gender-specific literacy, employment, and health outcomes.
# 
# #### The Migration Dynamics (Movement & Stock)
# International Migrant Stock: Represents the total number of people living in a country who were born elsewhere. This is a measure of a country's diversity and attractiveness as a destination.
# - **Net Migration:** The "Pulse" of a country's borders. It calculates (In-migration - Out-migration).
# - **Positive Net Migration:** Indicates a "Pull" factor (economic opportunity, safety).
# - **Negative Net Migration:** Indicates a "Push" factor (economic hardship, brain drain).


# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Target all countries
countries = 'all'

# 2. Demographic indicators mapping
# SP.POP.TOTL: Population, total
# SP.POP.TOTL.MA.IN: Population, male
# SP.POP.TOTL.FE.IN: Population, female
# SM.POP.TOTL: International migrant stock, total
# SM.MET.NETM: Net migration
demo_indicators = {
    'SP.POP.TOTL': 'Pop_Total_Count',
    'SP.POP.TOTL.MA.IN': 'Pop_Male_Count',
    'SP.POP.TOTL.FE.IN': 'Pop_Female_Count',
    'SM.POP.TOTL': 'Migrant_Stock_Total_Count',
    'SM.MET.NETM': 'Net_Migration_Flow'
}

# 3. Fetch the data
# mrv=40 to capture historical migration trends (often reported in 5-year gaps)
df_demo = wb.data.DataFrame(
    list(demo_indicators.keys()), 
    economy='all', 
    time=range(2010, 2026), 
    columns='series'
).reset_index()

# 4. Cleaning and Formatting
df_demo = df_demo.rename(columns=demo_indicators).reset_index()

# Clean Year column (World Bank API returns 'YR2020', we want 2020)
df_demo['time'] = df_demo['time'].str.replace('YR', '').astype(int)

df_demo = (
    df_demo
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 5. Save to Spark Delta Table
schema = "world_bank"
table_name = "population_migration"

# Create Spark DataFrame
df_spark = spark.createDataFrame(df_demo)

# Write to Delta
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Global population counts and migration flows sourced from World Bank API") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Demographic data successfully saved to {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### (1.3.1) Fertility

# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Target all countries
countries = 'all'

# 2. Fertility indicator mapping
# SP.DYN.TFRT.IN: Fertility rate, total (births per woman)
fertility_indicators = {
    'SP.DYN.TFRT.IN': 'Fertility_Rate_Births_Per_Woman'
}

# 3. Fetch the data
# mrv=40 is great for seeing long-term demographic transitions
df_fertility = wb.data.DataFrame(
    list(fertility_indicators.keys()), 
    economy='all', 
    time=range(2010, 2026), 
    columns='series'
).reset_index()


# 4. Cleaning and Formatting
df_fertility = df_fertility.rename(columns=fertility_indicators).reset_index()

# Extract Year as integer
df_fertility['time'] = df_fertility['time'].str.replace('YR', '').astype(int)

df_fertility = (
    df_fertility
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 5. Save to Spark Delta Table
schema = "world_bank"
table_name = "fertility_rates"

# Create Spark DataFrame
df_spark = spark.createDataFrame(df_fertility)

# Write to Delta
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Total fertility rate (births per woman) sourced from World Bank API") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Fertility data successfully saved to {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.4) Economic Data

# MARKDOWN ********************

# ### (1.4.1) Economic Indicators

# MARKDOWN ********************

# ### (1.4.2) Employment Indicators

# CELL ********************

import wbgapi as wb
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Configuração
employment_indicators = {
    'SL.UEM.TOTL.ZS': 'Unemployment_Total',
    'SL.UEM.TOTL.FE.ZS': 'Unemployment_Female',
    'SL.UEM.TOTL.MA.ZS': 'Unemployment_Male'
}

print("1. A descarregar dados brutos da API para a Bronze...")
df_employment = wb.data.DataFrame(
    list(employment_indicators.keys()), 
    economy='all', 
    time=range(2010, 2026), 
    columns='series'
).reset_index()

# 2. Renomeação básica e cast de tempo (necessário para a estrutura de tabela)
df_employment = df_employment.rename(columns=employment_indicators)
df_employment['time'] = df_employment['time'].str.replace('YR', '').astype(int)
df_employment = df_employment.rename(columns={'economy': 'country_code_iso3', 'time': 'Year'})

# 3. Schema para garantir integridade na Bronze
schema_bronze = StructType([
    StructField("country_code_iso3", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Unemployment_Total", DoubleType(), True),
    StructField("Unemployment_Female", DoubleType(), True),
    StructField("Unemployment_Male", DoubleType(), True)
])

# 4. Gravação na Bronze
# Definimos o schema (world_bank) e o nome da tabela
schema_name = "world_bank"
table_name = "Unemployment"
full_table_path = f"{schema_name}.{table_name}"

print(f"2. A garantir que o schema '{schema_name}' existe...")
# Criamos o schema world_bank caso ele não exista
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# Criar o DataFrame Spark com o schema_bronze definido anteriormente
df_spark = spark.createDataFrame(df_employment, schema=schema_bronze)

print(f"3. A gravar tabela '{full_table_path}' no formato Delta...")

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(full_table_path)

print(f"✓ SUCESSO! Dados brutos guardados em: {full_table_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Tabelas para as perguntas nivel 2

# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Configuração dos indicadores
# Ajustei os nomes para serem compatíveis com SQL/Delta (sem espaços ou símbolos)
nivel_2_WB = {
    'SE.XPD.TOTL.GD.ZS' : 'Gov_Education_Exp_Pct_GDP',
    'SE.SEC.NENR' : 'School_Enrollment_Secondary_Net_Pct',
    'IT.NET.USER.ZS' : 'Internet_Usage_Pct_Pop',
    'SI.POV.DDAY' : 'Poverty_Headcount_Ratio_2_15_Day',
    'FX.OWN.TOTL.FE.ZS' : 'Account_Ownership_Female_Pct',
    'WP_time_01.2' : 'Digital_Payments_Past_Year_Female_Pct',
    'SL.TLF.CACT.FE.ZS' : 'Labor_Force_Participation_Female_Pct'
}

countries = 'all'

# 2. Recolha de Dados
print("A descarregar indicadores de desenvolvimento (mrv=40)...")
df_social = wb.data.DataFrame(list(nivel_2_WB.keys()), countries, mrv=40, columns='series')

# 3. Limpeza e Formatação
df_social = df_social.rename(columns=nivel_2_WB).reset_index()

# Transformar o ano em Inteiro
df_social['time'] = df_social['time'].str.replace('YR', '').astype(int)

df_social = (
    df_social
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 4. Configuração de Destino no Fabric
schema = "world_bank"
table_name = "social_development_indicators"

# Garantir que o esquema existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# 5. Criar Spark DataFrame e Gravar
print(f"A gravar a tabela {schema}.{table_name} no Lakehouse...")
df_spark = spark.createDataFrame(df_social)

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Social and education indicators (Gini, Poverty, Education) sourced from World Bank") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Dados sociais guardados com sucesso em {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import wbgapi as wb
import pandas as pd
from pyspark.sql import functions as F

spark.sql("DROP TABLE IF EXISTS bronze_lakehouse.world_bank.Fact_Social_Barriers")

# 1. Seleção Específica de Indicadores para Barreiras Sociais
indicadores_social = {
    'SE.PRM.TENR': 'School_Attendance',      # Adjusted to Primary Adjusted Net Attendance/Enrollment
    'SE.ADT.LITR.ZS': 'Literacy_Rate',
    'IT.NET.USER.ZS': 'Internet_Access',
    'FX.OWN.TOTL.FE.ZS': 'Female_Account_Ownership',
    'SH.DYN.MORT': 'Child_Mortality_Rate',  
    'SP.DYN.LE00.IN': 'Life_Expectancy'
}

# 2. Configurações de recolha (Apenas 2010 a 2024 conforme pedido)
anos_range = range(2010, 2025)

print("🚀 A recolher dados para Fact_Social_Barriers...")

# 3. Extração via API
df_raw = wb.data.DataFrame(list(indicadores_social.keys()), 'all', time=anos_range, columns='series')

# 4. Transformação (Data Engineering)
df_clean = df_raw.rename(columns=indicadores_social).reset_index()

# Limpeza do Ano e Renomeação
df_clean['time'] = df_clean['time'].str.replace('YR', '').astype(int)
df_clean = df_clean.rename(columns={'economy': 'Country_Code', 'time': 'Year'})

# 5. Conversão para Spark e Gravação
df_spark = spark.createDataFrame(df_clean)

table_name = "bronze_lakehouse.world_bank.Fact_Social_Barriers"

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✅ Sucesso! A tabela '{table_name}' foi criada com os 6 indicadores de 2010-2024.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import wbgapi as wb
import pandas as pd
from pyspark.sql import functions as F

# --- PASSO 1: Ingestão para a Bronze ---
indicators = {
    'NY.GDP.PCAP.PP.CD': 'GDP_Per_Capita',
    'FP.CPI.TOTL.ZG': 'Inflation_CPI_Pct',
    'NY.GDP.MKTP.KD.ZG': 'GDP_Growth_Annual_Pct'
}

print("A descarregar dados da API para a Bronze...")
df_raw_pd = wb.data.DataFrame(list(indicators.keys()), 'all', time=range(2010, 2025), columns='series')
df_raw_pd = df_raw_pd.rename(columns=indicators).reset_index()

# Converter para Spark DataFrame
spark_raw = spark.createDataFrame(df_raw_pd)

# Criar o Schema e a Tabela na Bronze
spark.sql("CREATE SCHEMA IF NOT EXISTS world_bank") # Ajusta se o nome do teu Lakehouse for outro
spark.sql("USE world_bank")

# Gravar como tabela física (Isto resolve o teu erro!)
spark_raw.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Economic_Indicators") # Se usares 3 níveis, seria catalog.schema.table

print("✅ Tabela Bronze criada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
