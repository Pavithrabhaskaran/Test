# Databricks notebook source
simpledata = [("James","Sales",3000),
             ("Michael","Sales",4600),
             ("Robert","Sales",4100),
             ("Maria","Finance",3000),
             ("James","Sales",3000),
             ("Scott","Finance",3300),
             ("Jen","Finance",3900),
             ("Jeff","Marketing",3000),
             ("Kumar","Marketing",2000),
             ("Saif","Sales",4100)]
columns = ["Employee_name","Department","Salary"]
df = spark.createDataFrame(data=simpledata,schema=columns)
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
wind = Window.partitionBy("Department").orderBy("Salary")
df1 = df.withColumn("rank", row_number().over(wind))
df1.show()

# COMMAND ----------

df1.sort(df1"Department")

# COMMAND ----------

grouped = df1.groupBy("Department")

# COMMAND ----------

grouped.select("Department","Employee_name", "Salary").agg(collect_list(struct("Employee_name", "Department", "Salary", "rank")).alias("tmp"))

# COMMAND ----------

df1.sort(df1.Department.desc(),df1.rank.asc()).show()

# COMMAND ----------

complexdata = (("i-101","p110",23,1),\
              ("i-102","p111",50,1),\
              ("i-103","p111",50,3),\
              ("i-104","p112",75,1),\
              ("i-105","p114",125,1),\
              ("i-106","p115",100,1),\
              ("i-107","p115",100,1),\
              ("i-108","p114",125,2),\
              ("i-109","p113",100,1),\
              ("i-110","p111",50,2))
col = ["Invoice_no","Product_ID","Unit_price","Quantity"]
df2 = spark.createDataFrame(data=complexdata,schema=col)
df2.show()

# COMMAND ----------

df2.createOrReplaceTempView("ADD")

# COMMAND ----------

from pyspark.sql.functions import countDistinct
df3 = df2.select("Invoice_no","Quantity",count("Quantity")("Quantity","Invoice_no"))
df3.show()



# COMMAND ----------

df2.show()

# COMMAND ----------

df3 = spark.sql("select Invoice_no, count(Distinct Quantity) from ADD group by Invoice_no")
df3.show()

# COMMAND ----------

print(df2.select((distinct().count())

# COMMAND ----------


