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

# # Data Engineering Project: UN and  World Bank Data Analysis

# MARKDOWN ********************

# ## 1. Project Objectives
# The goal of this project is to build an end-to-end data pipeline that correlates national demographic trends with global economic indicators to provide a holistic view of a country's development trajectory. This project analyzes the socio-economic landscape by merging granular population data with high-level financial and social metrics.


# MARKDOWN ********************

# ## 2. Data Sources
# + **UN Census Data** (for demographic information): 
#   + This dataset provides the "human" foundation of the project. It contains detailed demographic information collected directly from national authorities, including age distribution, sex ratios, literacy rates, and urban vs. rural living 
#   conditions. It tells us who the people are and where they live (often provided in 10-year intervals).
#   + https://unstats.un.org/unsd/demographic-social/products/dyb/dybcensusdata.cshtml
# + **World Bank Global Development data**:
#   + This dataset provides the "contextual" layer. Known as World Development Indicators (WDI), it includes annual metrics on GDP growth, life expectancy, poverty headcount, and labor force participation. It tells us how a country is performing economically and socially on the global stage (often provided in 1-year intervals).
#   + https://data.worldbank.org/

# MARKDOWN ********************

# ## 3. Data Summary
# 
# ### 3.1. Demographics: 
# This section defines the "who" and "where." It highlights the 2.1 Fertility "Replacement Level"—the magic number for population stability—and tracks the "Brain Gain/Drain" via migration flows. It also categorizes the workforce not just by if they work, but how they work (e.g., as employers vs. contributing family members).
# 
# ### 3.2. Education: 
# Education is measured using the ISCED framework to ensure international comparability. It distinguishes between Attainment (what the adult population has already achieved) and Attendance (real-time enrollment of youth), which helps identify exactly where students drop out of the pipeline.
# 
# ### 3.3. Development & Economics: 
# Development focuses on "Energy Poverty" (the urban-rural electricity gap), while Economics looks at the "Quality of Life." Crucially, it notes that GDP PPP is often a more "honest" metric than raw USD because it accounts for local purchasing power. It also warns that official unemployment rates often hide the "informal" economy.


# MARKDOWN ********************

# | Category | Primary Datasets | Key Metrics to Watch | The "Story" It Tells |
# | :--- | :--- | :--- | :--- |
# | **Demographics** | Population, Employment Status, Migration, Fertility | Age/Sex cohorts, 2.1 TFR, Net Migration Flow | Predicts future labor supply and whether a country is attracting or losing talent. |
# | **Education** | ISCED Attainment, School Attendance, Higher Ed (25+) | ISCED Levels 1-8, Gender Parity Index | Measures the "educational stock" of the workforce and real-time school retention. |
# | **Development** | Electricity Access & Consumption | Access % (Urban vs. Rural), KWh per capita | Measures infrastructure maturity and the "energy gap" in rural development. |
# | **Economics** | GDP (USD & PPP), Growth, Inflation, Per Capita | GDP PPP, GDP Growth %, CPI | Tracks absolute economic power versus individual standard of living and stability. |
# | **Labor Market** | Unemployment Rates, National Estimates | Youth Unemployment (15-24), Gender Gap | Reveals how well the formal economy absorbs workers and identifies "hidden" unemployment. |
# 
# <p align="center"><i>Table 1: Summary of Global Statistical Indicators for Human and Economic Development</i></p>


# MARKDOWN ********************

# ## 4 Challenges 
# 
# ### 4.1 Challenges in Data Acquisition (UN Census)
# The most significant barrier encountered was the 100,000-record limit on CSV exports. For a report requiring longitudinal or multi-regional analysis, 100,000 rows is an insufficient threshold. This constraint necessitated a fragmented approach to data collection:
# 
# - **Manual Batching:** The dataset had to be manually partitioned into smaller subsets based on specific variables (e.g., individual years or specific sub-regions) to ensure each export stayed under the limit.
# 
# - **Workflow Inefficiency:** This "slicing" of data converted what should have been a single download into a repetitive, multi-stage process, significantly increasing the time spent on data ingestion rather than analysis.
