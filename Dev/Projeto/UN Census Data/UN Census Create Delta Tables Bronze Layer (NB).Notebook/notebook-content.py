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

# # (1) Create Delta Tables for the UN Census Data for the Bronze Layer

# MARKDOWN ********************

# ## (1.1) Educational Data

# CELL ********************

## -------------------------------------------------------------------------------------
## Population 15 years of age and over, by educational attainment, age and sex
## -------------------------------------------------------------------------------------

import pandas as pd # Just in case for reference, but we use Spark functions here
from pyspark.sql.functions import col

# Define the folder path
folder_path = "Files/Education Statistics Database/Population 15 years of age and over, by educational attainment, age and sex/*.csv"

# Load your data
df_merged = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(folder_path)

# Automatically replace spaces and invalid characters in ALL column names
new_columns = [col(c).alias(c.replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')) for c in df_merged.columns]
df_clean = df_merged.select(*new_columns)

# Now save it
target_table_name = "pop_attainment_demographics_15plus"
df_clean.write.format("delta").mode("overwrite").saveAsTable(target_table_name)

schema = "UN_Census_Education_Statistics_Database"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# 3. Save to a Delta Table
target_table_name = "pop_attainment_demographics_15plus"
df_clean.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{target_table_name}")

print(f"Merge complete! All files combined into table: {target_table_name}")


## -------------------------------------------------------------------------------------
## Population 5 to 24 years of age by school attendance, sex and urbanrural residence
## -------------------------------------------------------------------------------------

# Define the folder path
folder_path = "Files/Education Statistics Database/Population 5 to 24 years of age by school attendance, sex and urbanrural residence/*.csv"

# Load your data
df_merged = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(folder_path)

# Automatically replace spaces and invalid characters in ALL column names
new_columns = [col(c).alias(c.replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')) for c in df_merged.columns]
df_clean = df_merged.select(*new_columns)

schema = "UN_Census_Education_Statistics_Database"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# 3. Save to a Delta Table
target_table_name = "school_attendance_urban_rural_5to24"
df_clean.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{target_table_name}")

print(f"Merge complete! All files combined into table: {target_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.2) Demographic data

# CELL ********************

## -------------------------------------------------------------------------------------
## Population by age, sex and urbanrural residence
## -------------------------------------------------------------------------------------

import pandas as pd # Just in case for reference, but we use Spark functions here
from pyspark.sql.functions import col

# Define the folder path
folder_path = "Files/Demographic Statistics Database/Population by age, sex and urbanrural residence/*.csv"

# Load your data
df_merged = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(folder_path)

# Automatically replace spaces and invalid characters in ALL column names
new_columns = [col(c).alias(c.replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')) for c in df_merged.columns]
df_clean = df_merged.select(*new_columns)

schema = "UN_Census_Demographic_Statistics_Database"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# 3. Save to a Delta Table
target_table_name = "pop_age_sex_urban_rural"
df_clean.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{target_table_name}")

print(f"Merge complete! All files combined into table: {target_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.3) Economic data

# CELL ********************

## -------------------------------------------------------------------------------------
## (1) Employed population by status in employment, age and sex.csv
## (2) Gini index
## (3) Unemployment rate
## -------------------------------------------------------------------------------------

import os

# 1. Define the directory path
input_path = "Files/Economic Statistics Database"
schema = "UN_Census_Economic_Statistics_Database"

# Create the database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

# 2. List files using mssparkutils
# This returns a list of objects with .path and .name attributes
files = mssparkutils.fs.ls(input_path)

for file in files:
    if file.name.endswith(".csv"):
        base_name = file.name.rsplit('.', 1)[0]
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
        table_name = re.sub(r'_+', '_', clean_name).lower().strip('_')
        
        print(f"Processing: {file.name} -> Table: {table_name}")
        
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(file.path))
        
        # --- NEW: CLEAN COLUMN NAMES ---
        # This replaces spaces, dots, and other bad characters in column headers
        for col_name in df.columns:
            clean_col = re.sub(r'[ ,;{}()\n\t=]', '_', col_name)
            df = df.withColumnRenamed(col_name, clean_col)
        # -------------------------------
        
        df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{table_name}")

print("Success! All CSVs are now Delta tables with clean columns.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.4) Development Data (maybe)

# CELL ********************

## -------------------------------------------------------------------------------------
## Total eletricity
## -------------------------------------------------------------------------------------

import pandas as pd # Just in case for reference, but we use Spark functions here
from pyspark.sql.functions import col

# Define the folder path
folder_path = "Files/Development Statistics Database/Total Electricity/*.csv"

# Load your data
df_merged = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(folder_path)

# Automatically replace spaces and invalid characters in ALL column names
new_columns = [col(c).alias(c.replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')) for c in df_merged.columns]
df_clean = df_merged.select(*new_columns)

schema = "UN_Census_Development_Statistics_Database"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# 3. Save to a Delta Table
target_table_name = "global_total_eletricity"
df_clean.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{target_table_name}")

print(f"Merge complete! All files combined into table: {target_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
