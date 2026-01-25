# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2040f8e7-720b-4901-acd5-9b9c700b12af",
# META       "default_lakehouse_name": "Bronze_LakeHouse",
# META       "default_lakehouse_workspace_id": "d83c184e-82f0-4705-952c-0e29c5cb5274",
# META       "known_lakehouses": [
# META         {
# META           "id": "2040f8e7-720b-4901-acd5-9b9c700b12af"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # (1) Create Delta Tables for the World Bank data for the Bronze Layer

# CELL ********************

%pip install wbgapi 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # (1.1) Educational Data

# MARKDOWN ********************

# ### (1.1.1) High-Level Educational Attainment across the global population aged 25 and older. Specifically, it tracks the "highest level of schooling completed" for three distinct tiers of tertiary (higher) education.

# MARKDOWN ********************


# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Target all countries instead of a specific list
countries = 'all'

# 2. Educational indicators (Mapping World Bank codes to readable names)
edu_indicators = {
    'SE.TER.CUAT.MS.ZS': 'Master_Total_Pct',
    'SE.TER.CUAT.MS.MA.ZS': 'Master_Male_Pct',
    'SE.TER.CUAT.ST.MA.ZS': 'Short_Cycle_Male_Pct',
    'SE.TER.CUAT.DO.FE.ZS': 'Doctoral_Female_Pct',
    'SE.TER.CUAT.BA.FE.ZS': 'Bachelor_Female_Pct',
    'SE.TER.CUAT.ST.FE.ZS': 'Short_Cycle_Female_Pct',
    'SE.TER.CUAT.DO.MA.ZS': 'Doctoral_Male_Pct',
    'SE.TER.CUAT.BA.MA.ZS': 'Bachelor_Male_Pct'
}

# 3. Fetch the data
# We'll keep mrv=40 because education surveys are often infrequent/sporadic
df_edu = wb.data.DataFrame(
    list(edu_indicators.keys()), 
    countries, 
    mrv=40, 
    columns='series'
)

# 4. Cleaning and Formatting
df_edu = df_edu.rename(columns=edu_indicators)

df_edu = (
    df_edu
    .reset_index()
    .rename(columns={'economy': 'Country', 'time': 'Year'})
    .sort_values(['Country', 'Year'])
    .reset_index(drop=True)
)

# Set display to 2 decimal places for percentages
pd.options.display.float_format = '{:,.2f}%'.format

print("Educational Attainment Indicators (Population 25+)")

schema = "World_Bank_Education_Statistics_Database"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# If using PySpark to save the Delta Table
df_spark = spark.createDataFrame(df_edu)

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("description", "Global educational attainment metrics for population 25+ sourced from World Bank API") \
    .saveAsTable(f"{schema}.educational_attainment_global_pct_by_sex")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.2) Development Data
# The indicators you've listed fall into two primary categories: Socio-economic Development and Infrastructure Performance.
# Specifically, these are used by organizations like the World Bank and the UN to measure a country's energy profile. Here is the breakdown of how they are classified:
# 
# ### 1. Energy Access Indicators
# The three "Access to electricity" metrics are social development indicators. They measure the reach of the power grid and the equity of service across different demographics.
# - Access to electricity (% of population): A macro-level indicator of national development.
# - Urban vs. Rural Access: These are distributional indicators. They highlight geographic inequality and are crucial for identifying where infrastructure investment is most needed (often referred to as "the last mile" problem).
# 
# ### 2. Energy Consumption Indicators
# Electric power consumption (kWh per capita): This is an economic intensity indicator. It measures the average amount of electrical energy used per person.
# - High kWh per capita usually correlates with high industrialization and higher standards of living.
# - Low kWh per capita often indicates a lack of industrial base or energy poverty.


# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Target all countries
countries = 'all'

# 2. Electricity indicators mapping
# EG.USE.ELEC.KH.PC: Electric power consumption (kWh per capita)
# EG.ELC.ACCS.ZS: Access to electricity (% of population)
# EG.ELC.ACCS.UR.ZS: Access to electricity, urban (% of urban population)
# EG.ELC.ACCS.RU.ZS: Access to electricity, rural (% of rural population)
elec_indicators = {
    'EG.USE.ELEC.KH.PC': 'KWh_Per_Capita',
    'EG.ELC.ACCS.ZS': 'Access_Total_Pct',
    'EG.ELC.ACCS.UR.ZS': 'Access_Urban_Pct',
    'EG.ELC.ACCS.RU.ZS': 'Access_Rural_Pct'
}

# 3. Fetch the data
# mrv=20 is usually sufficient for electricity trends
df_elec = wb.data.DataFrame(
    list(elec_indicators.keys()), 
    countries, 
    mrv=40, 
    columns='series'
)

# 4. Cleaning and Formatting
df_elec = df_elec.rename(columns=elec_indicators).reset_index()

# Extract Year as integer (removes 'YR' prefix)
df_elec['time'] = df_elec['time'].str.replace('YR', '').astype(int)

df_elec = (
    df_elec
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 5. Save to Spark Delta Table
schema = "World_Bank_Development_Statistics_Database"
table_name = "electricity_access_and_consumption"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Create Spark DataFrame
df_spark = spark.createDataFrame(df_elec)

# Write to Delta
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Global electricity access and consumption metrics sourced from World Bank API") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Data successfully saved to {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
    countries, 
    mrv=100, 
    columns='series'
)

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
schema = "World_Bank_Demographic_Statistics_Database"
table_name = "population_and_migration_global"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

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
    countries, 
    mrv=100, 
    columns='series'
)

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
schema = "World_Bank_Demographic_Statistics_Database"
table_name = "fertility_rates_global"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

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

# CELL ********************

import pandas as pd
import wbgapi as wb

# 1. Configuration: Map World Bank codes to readable names
econ_map = {
    'NY.GDP.MKTP.CD': 'GDP_USD',
    'NY.GDP.PCAP.CD': 'GDP_Per_Capita',
    'NY.GDP.MKTP.KD.ZG': 'GDP_Growth_Annual_%',
    'FP.CPI.TOTL.ZG': 'Inflation_CPI_%',
    'NY.GDP.MKTP.PP.CD': 'GDP_PPP'
}

# 2. Fetch and Clean Data
# mrv=100 pulls the 100 most recent years of data
df_econ = wb.data.DataFrame(list(econ_map.keys()), countries, mrv=100, columns='series')

df_econ = (
    df_econ
    .rename(columns=econ_map)
    .reset_index()
    .rename(columns={'economy': 'Country', 'time': 'Year'})
    .sort_values(['Country', 'Year'])
    .reset_index(drop=True)
)

# 3. Final Formatting
pd.options.display.float_format = '{:,.3f}'.format

# 5. Save to Spark Delta Table
schema = "World_Bank_Economic_Statistics_Database"
table_name = "country_economic_indicators"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Create Spark DataFrame
df_spark = spark.createDataFrame(df_econ)

# Write to Delta
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Economic indicators (GDP and inflation indicators) sourced from World Bank API") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Economic indicators data successfully saved to {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### (1.4.2) Employment Indicators

# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Target all countries
countries = 'all'

# 2. Employment Indicator Mapping (National Estimates)
# SL.UEM.TOTL.NE.ZS: Unemployment, total
# SL.UEM.MAINT.MA.NE.ZS: Unemployment, male
# SL.UEM.FEINT.FE.NE.ZS: Unemployment, female
employment_indicators = {
    # General Unemployment (National Estimates)
    'SL.UEM.TOTL.NE.ZS': 'Unemployment_Total_Pct',
    'SL.UEM.TOTL.MA.NE.ZS': 'Unemployment_Male_Pct',
    'SL.UEM.TOTL.FE.NE.ZS': 'Unemployment_Female_Pct',
    
    # Youth Unemployment (Ages 15-24, National Estimates)
    'SL.UEM.1524.NE.ZS': 'Youth_Unemployment_Total_Pct',
    'SL.UEM.1524.MA.NE.ZS': 'Youth_Unemployment_Male_Pct',
    'SL.UEM.1524.FE.NE.ZS': 'Youth_Unemployment_Female_Pct'
}

# 3. Fetch the data
# Using mrv=100 to capture recent decades of labor trends
df_employment = wb.data.DataFrame(
    list(employment_indicators.keys()), 
    countries, 
    mrv=100, 
    columns='series'
)

# 4. Cleaning and Formatting
df_employment = df_employment.rename(columns=employment_indicators).reset_index()

# Extract Year as integer and clean column names
df_employment['time'] = df_employment['time'].str.replace('YR', '').astype(int)

df_employment = (
    df_employment
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 5. Save to Spark Delta Table
schema = "World_Bank_Economic_Statistics_Database"
table_name = "unemployment_rates_global"

# Initialize Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Create Spark DataFrame
df_spark = spark.createDataFrame(df_employment)

# Write to Delta
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Unemployment rates (Total, Male, Female), (Ages 15-24, National Estimates) sourced from World Bank API via national estimates") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Employment data successfully saved to {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
