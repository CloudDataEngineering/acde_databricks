# Databricks notebook source
# MAGIC %run "/Workspace/Users/azureclouddataengineering@gmail.com/Formula1-HandsOn/03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Inner Join

# COMMAND ----------

circuit_df_inner = spark.read.parquet(f'{processed_folder_path}/circuits')\
                .withColumnRenamed('name', 'circuit_name')
display(circuit_df_inner.limit(10))

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').filter('race_year == 2019')\
            .withColumnRenamed('name', 'race_name')
display(races_df.limit(10))

# COMMAND ----------

races_circute_df_inner = circuit_df_inner.join(races_df, circuit_df_inner.circuit_id == races_df.circuit_id, 'inner')\
    .select(circuit_df_inner.circuit_id, circuit_df_inner.circuit_name,circuit_df_inner.location, circuit_df_inner.country, races_df.race_name, races_df.round) \
    .orderBy('circuit_id')
display(races_circute_df_inner.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Outer Join

# COMMAND ----------

# MAGIC %md
# MAGIC #### Left Outer Join

# COMMAND ----------

circuit_df_left = spark.read.parquet(f'{processed_folder_path}/circuits')\
                .withColumnRenamed('name', 'circuit_name')\
                .filter('circuit_id < 70')
display(circuit_df_left.limit(10))

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').filter('race_year == 2019')\
            .withColumnRenamed('name', 'race_name')
display(races_df.count())

# COMMAND ----------

races_circute_df_left = circuit_df_left.join(races_df, circuit_df_left.circuit_id == races_df.circuit_id, 'left')\
    .select(circuit_df_left.circuit_id, circuit_df_left.circuit_name,circuit_df_left.location, circuit_df_left.country, races_df.race_name, races_df.round) \
    .orderBy('circuit_id')
display(races_circute_df_left.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Righr Outer Join

# COMMAND ----------

races_circute_df_right = circuit_df_left.join(races_df, circuit_df_left.circuit_id == races_df.circuit_id, 'right')\
    .select(circuit_df_left.circuit_id, circuit_df_left.circuit_name,circuit_df_left.location, circuit_df_left.country, races_df.race_name, races_df.round) \
    .orderBy('circuit_id')
display(races_circute_df_right.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Full Outer Join

# COMMAND ----------

races_circute_df_full = circuit_df_left.join(races_df, circuit_df_left.circuit_id == races_df.circuit_id, 'right')\
    .select(circuit_df_left.circuit_id, circuit_df_left.circuit_name,circuit_df_left.location, circuit_df_left.country, races_df.race_name, races_df.round) \
    .orderBy('circuit_id')
display(races_circute_df_full.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Semi Join

# COMMAND ----------

races_circute_df_semi = circuit_df_left.join(races_df, circuit_df_left.circuit_id == races_df.circuit_id, 'semi')\
    .select(circuit_df_left.circuit_id, circuit_df_left.circuit_name,circuit_df_left.location, circuit_df_left.country) \
    .orderBy('circuit_id')
display(races_circute_df_semi.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Anti Join

# COMMAND ----------

races_circute_df_anti = circuit_df_left.join(races_df, circuit_df_left.circuit_id == races_df.circuit_id, 'anti')\
    .select(circuit_df_left.circuit_id, circuit_df_left.circuit_name,circuit_df_left.location, circuit_df_left.country) \
    .orderBy('circuit_id')
display(races_circute_df_anti.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cross Join

# COMMAND ----------

races_circute_df_anti = circuit_df_left.join(races_df, circuit_df_left.circuit_id == races_df.circuit_id, 'cross')\
    .select(circuit_df_left.circuit_id, circuit_df_left.circuit_name,circuit_df_left.location, circuit_df_left.country, races_df.race_name, races_df.round) \
    .orderBy('circuit_id')
display(races_circute_df_anti.limit(10))

# COMMAND ----------


