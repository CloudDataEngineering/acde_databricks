# Databricks notebook source
print('vinay')

# COMMAND ----------

# Install the necessary libraries
# %pip install pandas

# Import the necessary libraries
import pandas as pd

# URL of the Delta data on GitHub
data_url = "https://raw.githubusercontent.com/cloudboxacademy/covid19/main/raw/main/ecdc_data/case_deaths_uk_ind_only.csv"

# Read the CSV data using pandas
df_pandas = pd.read_csv(data_url)

# Convert the pandas DataFrame to a Spark DataFrame
df = spark.createDataFrame(df_pandas)

# Display the DataFrame
display(df)
