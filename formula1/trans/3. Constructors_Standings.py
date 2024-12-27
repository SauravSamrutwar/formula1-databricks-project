# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
p_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_result") \
    .filter(f"results_file_date = '{p_file_date}'") \
        .select("race_year") \
            .distinct() \
                .collect()

# COMMAND ----------

race_year_list = []
for race_result in race_results_list:
    race_year_list.append(race_result.race_year)

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, count

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_result") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standing_df = race_results_df.groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

#overite_partition_column(final_df, 'f1_presentation', 'constructor_standing', 'race_year')

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standing', presentation_folder_path, merge_condition, 'race_year') 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  race_year, COUNT(1)
# MAGIC FROM f1_presentation.constructor_standing
# MAGIC GROUP BY race_year
# MAGIC ORDER BY  race_year DESC;

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standing")