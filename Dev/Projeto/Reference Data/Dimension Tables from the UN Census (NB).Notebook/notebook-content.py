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

# # (1) SQL code used to create Dimension Tables from the UN Census

# MARKDOWN ********************

# ## (1.1) Sex Dimension 

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Reference_Database.Sex_Dimension;
# MAGIC 
# MAGIC CREATE TABLE Reference_Database.Sex_Dimension AS
# MAGIC SELECT DISTINCT Sex_Code, Sex 
# MAGIC FROM UN_Census_Demographic_Statistics_Database.pop_age_sex_urban_rural
# MAGIC WHERE Sex_Code IS NOT NULL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.2) Age Dimension

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Reference_Database.Age_Dimension;
# MAGIC 
# MAGIC CREATE TABLE Reference_Database.Age_Dimension AS
# MAGIC SELECT DISTINCT 
# MAGIC     Code_of_Age, 
# MAGIC     Age
# MAGIC FROM UN_Census_Demographic_Statistics_Database.pop_age_sex_urban_rural
# MAGIC WHERE Code_of_Age IS NOT NULL 
# MAGIC   AND Age NOT LIKE '%-%' 
# MAGIC   AND Age IS NOT NULL
# MAGIC ORDER BY Code_of_Age DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.3) Area Dimension

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Reference_Database.Area_Dimension;
# MAGIC 
# MAGIC CREATE TABLE Reference_Database.Area_Dimension AS
# MAGIC SELECT DISTINCT 
# MAGIC     Area_Code, 
# MAGIC     Area
# MAGIC FROM UN_Census_Demographic_Statistics_Database.pop_age_sex_urban_rural
# MAGIC WHERE Area_Code IS NOT NULL
# MAGIC ORDER BY Area_Code ASC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.4) Employement Status Dimension

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Reference_Database.Employment_Status_Dimension;
# MAGIC 
# MAGIC CREATE TABLE Reference_Database.Employment_Status_Dimension AS
# MAGIC SELECT DISTINCT 
# MAGIC     Code_of_Status_in_employment, 
# MAGIC     Status_in_employment
# MAGIC FROM UN_Census_Economic_Statistics_Database.employed_population_by_status_in_employment_age_and_sex
# MAGIC WHERE Code_of_Status_in_employment IS NOT NULL 
# MAGIC   AND Status_in_employment IS NOT NULL
# MAGIC ORDER BY Code_of_Status_in_employment ASC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.5) Education Attainment Dimension

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Reference_Database.Education_Attainment_15plus_Dimension;
# MAGIC 
# MAGIC CREATE TABLE Reference_Database.Education_Attainment_15plus_Dimension AS
# MAGIC SELECT DISTINCT 
# MAGIC     Code_of_Educational_attainment, 
# MAGIC     Educational_attainment
# MAGIC FROM UN_Census_Education_Statistics_Database.pop_attainment_demographics_15plus
# MAGIC WHERE Code_of_Educational_attainment IS NOT NULL 
# MAGIC   AND Educational_attainment IS NOT NULL
# MAGIC ORDER BY Code_of_Educational_attainment ASC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.6) School Attendance Dimension

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS Reference_Database.School_Attendance_Dimension;
# MAGIC 
# MAGIC CREATE TABLE Reference_Database.School_Attendance_Dimension AS
# MAGIC SELECT DISTINCT 
# MAGIC     Code_of_School_attendance, 
# MAGIC     School_attendance
# MAGIC FROM UN_Census_Education_Statistics_Database.school_attendance_urban_rural_5to24
# MAGIC WHERE Code_of_School_attendance IS NOT NULL 
# MAGIC   AND School_attendance IS NOT NULL
# MAGIC ORDER BY Code_of_School_attendance ASC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
