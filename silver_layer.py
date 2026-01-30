# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Layer Script##

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access ##

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.mark1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mark1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mark1dl.dfs.core.windows.net", "b913ab03-d8e6-4027-8a61-07aa2d258f7c")
spark.conf.set("fs.azure.account.oauth2.client.secret.mark1dl.dfs.core.windows.net", "krB8Q~r6yVUsdiELMoj4whbDCbd414~tl5_8Ec~5")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mark1dl.dfs.core.windows.net", "https://login.microsoftonline.com/3cac0e47-4147-4b6c-bb24-ea6671ef3a86/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Data Loading**##

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data ###

# COMMAND ----------

df_cal = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@mark1dl.dfs.core.windows.net/AdventureWorks_Calendar")

df_custs = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@mark1dl.dfs.core.windows.net/AdventureWorks_Customers")

df_prod_catg = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@mark1dl.dfs.core.windows.net/AdventureWorks_Product_Categories")

df_sub_catg = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@mark1dl.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

df_prodts = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@mark1dl.dfs.core.windows.net/AdventureWorks_Products")

df_returns = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@mark1dl.dfs.core.windows.net/AdventureWorks_Returns")

df_sales = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@mark1dl.dfs.core.windows.net/AdventureWorks_Sales*")

df_terr = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@mark1dl.dfs.core.windows.net/AdventureWorks_Territories")



# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform ##

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df.withColumn("Month",month("Date"))\
    .withColumn("Year",year("Date"))

df_cal.show()

# COMMAND ----------

df_cal.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@mark1dl.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

df_custs.withColumn("full_name",concat("Prefix", lit(" "),col("FirstName"),lit(" "),col("LastName"))).display()

# COMMAND ----------

df_custs = df_custs.withColumn("full_name",concat_ws(" ",col("Prefix"),col("FirstName"),col("LastName")))
df_custs.show()

# COMMAND ----------

df_custs.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@mark1dl.dfs.core.windows.net/AdventureWorks_Customer")\
    .save()

# COMMAND ----------

df_sub_catg.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@mark1dl.dfs.core.windows.net/AdventureWorks_sub_category")\
    .save()

# COMMAND ----------

df_prodts.display()

# COMMAND ----------

df_prodts = df_prodts.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),' ')[0])
df_prodts.show()

# COMMAND ----------

df_prodts.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@mark1dl.dfs.core.windows.net/AdventureWorks_products")\
    .save()

# COMMAND ----------

df_returns.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@mark1dl.dfs.core.windows.net/AdventureWorks_returns")\
    .save()

# COMMAND ----------

df_terr.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@mark1dl.dfs.core.windows.net/AdventureWorks_territories")\
    .save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------


df_sales.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@mark1dl.dfs.core.windows.net/AdventureWorks_sales")\
    .save()

# COMMAND ----------

