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

# # (1) Reference Data

# MARKDOWN ********************

# ## (1.1) Standard Country or Area Codes for Statistical Use

# CELL ********************

## -------------------------------------------------------------------------------------
## Standard Country or Area Codes for Statistical Use
## -------------------------------------------------------------------------------------

import re
import pandas as pd # Just in case for reference, but we use Spark functions here
from pyspark.sql.functions import col

# Define the folder path
folder_path = "Files/Reference Data/Standard Country or Area Codes.csv"

# Load your data
# Updated Load Step
df_countries = (spark.read
                .format("csv")
                .option("header", "true")
                .option("sep", ";")        # This is the key fix
                .option("inferSchema", "true")
                .load(folder_path))

schema = "Reference_Database"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Function to clean column names
def clean_column_name(name):
    # Replace invalid characters with underscores and lowercase it
    return re.sub(r'[ ,;{}()\n\t=]+', '_', name).strip('_').lower()

# Apply the cleaning to all columns
df_cleaned = df_countries.toDF(*[clean_column_name(c) for c in df_countries.columns])

# 3. Save to a Delta Table
target_table_name = "dim_geography_m49"

# Added overwriteSchema to force the new, multi-column structure
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{schema}.{target_table_name}")

print(f"file saved into table: {target_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
