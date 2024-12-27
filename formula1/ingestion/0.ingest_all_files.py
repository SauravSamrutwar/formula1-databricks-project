# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races.csv_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_Drivers_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_results_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("injest_lap_times_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})    

# COMMAND ----------

v_result = dbutils.notebook.run("injest_pit_stop_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

v_result = dbutils.notebook.run("Injest_Qualifying_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})