# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # 1. The Economy (Money) (Questions)

# MARKDOWN ********************

# - (1) How do GDP growth patterns differ between BRICS, EU, and the USA over time? (GDP growth (%); GDP per capita; Volatility (std dev of growth)) [x] [temos dados]
#     - Are emerging economies converging toward developed economies in GDP per capita? [x]
# 
# - (2) How does employment status differ across economic blocs? (Employees vs self-employed ; Contributing family workers) [x]
#     - **Data to get**: % of employees, % of self-employed, % of contributing family workers
# 
#     - How does labor participation differ by age and sex across blocs? [?]
#     - How does labor participation differ by age and sex across blocs? (Female labor force participation , Youth employment, Aging workforce (55+))
#         - **Data to get**: Employment status, Labour participation by sex, Labour participation by age
# 
# - (3) How does education (if available) relate to employment quality [não temos dados]
#     - **Data to get:**: Unemployment,Wages,Job stability
# 
# - (4) Do faster-growing economies reduce inequality faster?
#     - **Data to get:** Gini coefficient, GDP growth rate (%)
# 
# - (5) How resilient are different blocs to economic shocks? (Employment drop during crises (2008, COVID), Recovery speed).
#     - **Data to get:** Employment rate (%),Unemployment rate (%), GDP growth rate (%)


# MARKDOWN ********************

# # 2. Demographics (Questions)

# MARKDOWN ********************

# - (1) Qual é a tendência de crescimento da população [x]
#     - Which countries have the highest and lowest population growth rates?
#         - **Data to get:** Population data over time (core data), Population growth rate. **maybe**:Fertility rate,Mortality rate, Net migration
# 
#     - line plot comparing the population trends (growth or decline) of the European Union (EU), the BRICS countries (as a combined group), and the United States over the last 45 years (approximately 1980–2025). The plot should show how the total population in each group has evolved over time, ideally with absolute population numbers (in millions or billions) on the y-axis and years on the x-axis, so the differences in growth rates and directions are clearly visible."
# 
# - (2) How is the world’s population distributed by age and sex by continent and country?
#     - **Data to Get:** Population by age group, Population by sex
# 
# - (3) How many people in the country have access to electricity, and what is the average electricity consumption per capita?
#     - **Data to get:** Population with access to electricity,Total electricity consumption,Total population
# 
# - (4) What proportion of the population has access to secondary or higher education?
#     - **Data to get:** Population data by education level, Total population
# 
# - (5) How does electricity consumption relate to the country’s GDP growth?
#     - **Data to get:** per capita electricity consumption, GDP data
# 
# - (6) How does education level (especially female education) correlate with fertility rates?
#     - **Data to get:** Education data (Female education levels, male education opcional), Total fertility rate (TFR): average number of children born per woman.


# MARKDOWN ********************

# # 3. Necessary data

# MARKDOWN ********************

# ## 3.1. Economic & Employment Data
#  
# - Macroeconomic Indicators: [x]
#     - GDP growth (% annual) [x]
#     - GDP per capita [x]
#     - Standard deviation of GDP growth (volatility) [x]
#     - inflation  [x]
#     - GDP (PPP) [x]
# 
# - Wages (average or median) [?]
# 
# - Employment Table:
#     - employment rate (%) [x]
#     - employement rate male (%) [x]
#     - employement rate female (%) [x]
#     - % Self-employed [x]
#     - % Contributing family workers [x]
#     - Unemployment rate (%) [x]
#     - Unemployment rate male (%) [x]
#     - Unemployment rate female (%) [x]
# 
# - Labor participation by: [x]
#     - Sex (male/female) [x]
#     - Age groups (youth, 55+ etc.) [x]
# 
# - Job stability (if available)
# 
# - Gini coefficient (inequality measure) [x]
# 
# - Economic shocks / crisis impact: 
#     - Employment rate during crises (2008, COVID)
#     - Recovery speed (employment and GDP growth after crisis)
# 
# ## 3.2. Population & Demographics
# 
# - Demographic Indicators: [x]
#     - Total population over time [x]
#     - Population growth rate [x]
#     - Immigrants [x]
#     - Emigrants [x] 
# 
# - Age,sex demographics: [x]
#     - Population by age group [x]
#     - Population by sex [x]
#     
# -Fertility rate [x]
# 
# -Mortality rate (optional) 
# 
# 
# ## 3.3 Education Data
# - Population by education level (secondary or higher) [x]
# 
# Education by sex [x]
# - Female education levels (specific) [x]
# - Male education levels (optional) [x]
# 
# ## 3.4 Energy / Electricity Data
# 
# - Eletricity: [x]
#     - Population with access to electricity [x]
#     - Total electricity consumption [x]
#     - Per capita electricity consumption (derived from above) [x]


# MARKDOWN ********************

# # 4. Explanataion and summary of data selected

# MARKDOWN ********************

# ## (4) Data
# 
# ### (4.1) Demographics Statistics Data
# 
# #### (1) Population by age, sex and urban or rural residence
# 
# The data is derived from the UN Demographic Yearbook, specifically the table tracking Population by Age, Sex, and Urban/Rural Residence. This dataset represents a comprehensive snapshot of a country’s human landscape, categorized by three primary demographic pillars.
# 
# #### (2) Employed population by status in employment, age and sex
# This dataset provides a detailed breakdown of the economically active population currently in employment. It categorizes the workforce using three primary dimensions to offer a comprehensive view of a nation’s labor landscape.
# 
# 1. Classification of Employment Status
# The data organizes individuals into specific roles based on the type of economic risk and authority they hold. The primary categories include:
#     - **Employees:** Individuals working for a public or private employer and receiving remuneration (wages/salary).
#     - **Employers:** Individuals who operate their own economic enterprise (or are self-employed) and engage one or more employees.
#     - **Own-Account Workers:** Self-employed individuals who do not engage any employees (e.g., freelancers, independent artisans).
#     - **Contributing Family Workers:** Individuals who work in a market-oriented establishment operated by a relative but do not receive a formal wage.
# 
# 2. Demographic Dimensions
# The table cross-references employment status with two vital demographic markers:
#     - **Age Cohorts:** Data is typically presented in five-year intervals (e.g., 15–19, 20–24). This allows for the identification of trends in youth employment versus older workers reaching retirement.
#     - **Sex Disaggregation:** By separating data for males and females, the table highlights gender-based participation rates in different sectors of the economy.
# 
# #### (3) Unemployment Rate
# provides a standardized measure of the percentage of the labor force that is currently without work, but available and seeking employment.This specific table is designed to show disparities in the job market through three main lenses:
# 
# 1. Primary Dimensions (Columns)
#     - **Sex:** The data is strictly disaggregated by Female and Male. This allows for the calculation of the "gender unemployment gap," which is essential for understanding which sex faces higher barriers to entry in the economy.
# 
#     - **Age Groups:** The table typically highlights two critical cohorts:
#         - **15+ Years:** The total adult working population.
#         - **15–24 Years (Youth):** A specialized category that reveals "Youth Unemployment," often used to measure the economic stability and future potential of a region.
#     
#     - **Temporal Coverage:** It provides historical data (often spanning several decades), allowing you to track how unemployment has fluctuated over time in response to global or local economic shifts.
# 
# #### (4) Global Demographic & Migration Summary
# This dataset tracks the raw size of the population, its gender distribution, and the movement of people across international borders.
# 
# 1. Population Foundations (The Counts)
# These metrics provide the scale necessary to turn percentages from your other datasets into real-world numbers.
# 
#     - **Pop_Total_Count:** The total number of residents regardless of legal status or citizenship.
#     - **Pop_Male_Count / Pop_Female_Count:** The raw gender split. This is vital for calculating "Absolute Achievement"—for example, how many individual women hold Doctoral degrees in a specific region.
# 
# 2. Migration Dynamics (The Movement)
# This is where your analysis gets a "story." Migration indicators help distinguish between a country’s home-grown talent and the talent it attracts from abroad.
# 
#     - **Migrant_Stock_Total_Count:** The total number of people born in a country other than the one in which they currently live. This represents the "diversity" or "immigrant footprint" of a nation.
#     - **Net_Migration_Flow:** The net total of people arriving minus those departing.
#     - **Positive value:** More people entering (potential "Brain Gain").
#     - **Negative value:** More people leaving (potential "Brain Drain").
# 
# #### (5) Global Fertility Rate Summary
# This indicator measures the average number of children that would be born to a woman if she were to live to the end of her childbearing years and bear children in accordance with current age-specific fertility rates.
# 
# 1. The 2.1 "Replacement Level"
# The Benchmark: A TFR of 2.1 is considered "replacement level"—the rate at which a population exactly replaces itself from one generation to the next without migration.
# 
# - **Below 2.1:** Typical of highly industrialized nations with high levels of Bachelor's and Doctoral attainment.
# - **Above 2.1:** Typical of developing nations where the population is growing rapidly, often putting a strain on Electricity Access and school infrastructure.
# 
# ### (4.2) Education Statistics Data
# 
# #### (1) Population 15 years of age and over, by educational attainment, age and sex
# This dataset serves as a primary benchmark for assessing the global "educational stock" of a population. It categorizes individuals based on the highest level of schooling they have successfully completed, typically mapped to the International Standard Classification of Education (ISCED) levels by age, sex and geographic split between city and countryside residents.
# 
# 1. Key Dimensions (The Columns)
# When you query or view this table, you are looking at four primary variables:
# 
#     - **Educational Attainment:** This is the most critical column. It classifies the population based on the highest level of education successfully completed. It uses the ISCED (International Standard Classification of Education) framework:
#         - **No Schooling:** Persons who never attended or completed any formal education.
#         - **Primary (ISCED 1):** Basic reading, writing, and math.
#         - **Lower Secondary (ISCED 2):** The first stage of secondary education.
#         - **Upper Secondary (ISCED 3):** High school or equivalent.
#         - **Post-Secondary Non-Tertiary (ISCED 4):** Vocational or technical training.
#         - **Tertiary (ISCED 5-8):** Includes Bachelor’s, Master’s, and Doctoral levels.
# 
# 2. **Note on Comparability:** Because different countries have different school systems, the UN maps every country's specific degrees to the ISCED levels to make sure that a "High School Diploma" in one country is comparable to a "Level 3" in another.
# 
# #### (2) Population 5 to 24 years of age by school attendance, sex and urbanrural residence
# Is a high-frequency demographic dataset used to track real-time enrollment and out-of-school populations during the critical "learning years."
# 
# 1. Core Demographic Dimensions
# This table follows the standard UN Census structure but with a specific focus on the younger population:
#     - **Age Groups:** Uniquely, this table often provides single-year age data from age 5 through 24. This is vital for pinpointing exactly at what age children start dropping out of school (e.g., the transition from primary to secondary).
#     - **Sex:** Data is disaggregated by Male, Female, and Total. This is the primary tool for measuring the "Gender Parity Index" in school attendance.
#     - **Urban/Rural:** Many versions include a geographic split, which is crucial for identifying if rural areas lack the infrastructure (schools/transport) to keep students enrolled compared to urban centers.
# 
# 2. **Key Indicator:** School Attendance
# The "Status" in this table is usually divided into three categories:
#     - **Attending:** Currently enrolled in a regular accredited educational institution (public or private).
#     - **Not Attending:** Not currently enrolled (this includes those who have already graduated and those who dropped out).
#     - **Unknown:** Cases where attendance status was not recorded during the census.
# 
# #### (3) Global High-Level Educational Attainment (Aged 25+)
# This dataset tracks the percentage of the population that has completed various levels of tertiary education. It is categorized into three primary tiers:
# 
# 1. Doctoral Level (The Specialists)
# The highest level of academic achievement. Comparing these two indicators allows you to see the "gender gap" in research and high-level academia.
# 
#     - **Doctoral_Female_Pct:** % of females (25+) who completed a PhD.
#     - **Doctoral_Male_Pct:** % of males (25+) who completed a PhD.
# 
# 2. Master’s & Bachelor’s Levels (The Professionals)
# These indicators represent the core of the global professional workforce.
# 
#     - **Master_Total_Pct:** The overall percentage of the population with a Master’s degree.
#     - **Master_Male_Pct:** Completion rate specifically for males at the Master's level.
#     - **Bachelor_Female_Pct:** % of females who completed a Bachelor’s degree.
#     - **Bachelor_Male_Pct:** % of males who completed a Bachelor’s degree.
# 
# 3. Short-Cycle Tertiary (The Technical Experts)
# These are often practical, occupation-specific programs (like Associate degrees or advanced vocational diplomas) that are shorter than a Bachelor’s.
# 
#     - **Short_Cycle_Female_Pct:** % of females with short-cycle tertiary qualifications.
#     - **Short_Cycle_Male_Pct:** % of males with short-cycle tertiary qualifications.
# 
# ### (4.3) Development Statistics Data
# 
# #### (1) Global Electricity Access & Consumption Summary. 
# This dataset tracks the availability of electricity and the intensity of its use across different demographic landscapes.
# 
# 1. The Urban-Rural Divide (Access)
# These indicators are critical for identifying "energy poverty." In many developing regions, there is a significant gap between city infrastructure and remote village connectivity.
# 
#     - **Access_Total_Pct:** The baseline percentage of the total population with a consistent connection to the electrical grid.
#     - **Access_Urban_Pct:** Connectivity within developed city centers and metropolitan areas.
#     - **Access_Rural_Pct:** Connectivity in countryside or remote areas; often the primary metric for tracking national infrastructure development goals.
# 
# 2. Energy Intensity (Consumption)
# This provides a window into the economic activity and standard of living within a country.
# 
#     - **KWh_Per_Capita:** The average amount of electrical energy used per person.
# 
# **Note:** High consumption typically correlates with high industrialization and higher household income, but it also reflects the energy efficiency (or lack thereof) in a nation’s grid.
# 
# ### (4.4) Economic Statistics Data
# 
# #### (1) Global Economic Performance Summary
# This dataset tracks the total economic output, individual prosperity, and the stability of purchasing power.
# 
# 1. The Output Metrics (The "Size")
# 
#     - **GDP_USD:** The raw market value of all goods and services. This tells you the absolute economic power of a country.
#     - **GDP_PPP:** GDP at Purchasing Power Parity. This is often the more "honest" metric for your study because it adjusts for the cost of living.
# 
# **Example:** $1,000 USD buys much more electricity or tuition in India than in Switzerland. PPP accounts for that difference.
# 
# 2. The Standard of Living (The "Quality")
# 
#     - **GDP_Per_Capita:** The total GDP divided by the Pop_Total_Count from your demographic set. This is the primary indicator of individual economic well-being and is highly correlated with Tertiary Education levels.
# 
# 3. The Stability & Growth (The "Trend")
# 
#     - **GDP_Growth_Annual_%:** How fast the economy is expanding. Rapid growth is often seen in countries with high Fertility_Rate and expanding Access_Total_Pct to electricity.
#     - **Inflation_CPI_%:** The rate at which prices rise. High inflation can erode the value of an education (as tuition costs rise) and make infrastructure projects (like power plants) much more expensive.
# 
# #### (2) Global Employment (National Estimates) Summary
# This dataset tracks the "Underutilization" of the labor supply. It tells us which countries are failing to absorb their workers into the formal economy.
# 
# 1. The Core Metrics
# 
#     - **Unemployment_Total_Pct:** The percentage of the labor force that is currently without work but is actively seeking and available for employment.
#     - **Unemployment_Male_Pct / Unemployment_Female_Pct:** These indicators allow you to calculate the Gender Unemployment Gap.
# 
# 2. The "Hidden" Narrative
# It is important to remember that these percentages only include the "Labor Force" (people who want a job). They do not include:
# 
#     - **Discouraged Workers:** People who have given up looking.
#     - **Informal Workers:** In many high-fertility or low-GDP countries, people may not be "unemployed" but are working in survival-level informal jobs that aren't captured here.

