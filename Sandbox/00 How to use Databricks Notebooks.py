# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC %md
# MAGIC this is comment 
# MAGIC
# MAGIC # This is some header text
# MAGIC ## This is subheader

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Hello World")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls "/"

# COMMAND ----------


