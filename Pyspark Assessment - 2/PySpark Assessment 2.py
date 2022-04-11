# Databricks notebook source
simpleData = [("i-101", "85123A", "ABC", 150, 6, "2021-12-01 08:16:00", "c-1001"),
             ("i-102", "85124A", "XYZ", 110, 6, "2021-12-01 09:12:00", "c-1002"),
             ("i-103", "85125A", "MNO", 100, 4, "2021-12-01 10:00:00", "c-1003"),
             ("i-104", "85126A", "VWA", 102, 5, "2021-12-01 10:31:00", "c-1004"),
             ("i-105", "85127A", "AAS", 100, 7, "2021-12-01 10:45:00", "c-1005"),
             ("i-106", "85128A", "FAS", 130, 3, "2021-12-01 11:06:00", "c-1006"),
             ("i-107", "85129A", "AFA", 175, 6, "2021-12-01 11:15:00", "c-1007"),
             ("i-108", "85130A", "GAG", 150, 8, "2021-12-01 11:46:00", "c-1008"),
             ("i-109", "85131A", "AGG", 180, 8, "2021-12-01 12:56:00", "c-1009"),
             ("1-110", "85132A", "KKK", 200, 1, "2021-12-01 14:36:00", "c-1010")]
columns = ["Invoice_no", "Product_code", "Descr", "Unit_price", "Quantity", "Invoice_date", "Customer_ID"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col
df2 = df.withColumn('Sales', col("Quantity")*col("Unit_price"))
df2.show()

# COMMAND ----------

from pyspark.sql.functions import hour
n = df2.select(hour("Invoice_date").alias("Hour"),"Sales")
n.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df2.createOrReplaceTempView("New_tab")

# COMMAND ----------

df3 = df2.sort(col("Sales").desc())
df3.select("Customer_ID").show(3)

# COMMAND ----------

c = spark.sql("select date(Invoice_date) as Day, sum(Quantity) as Qty, sum(Sales) as Total_Sales_per_day from New_tab group by date(Invoice_date)")
c.show()

# COMMAND ----------

spark.sql("select year(Invoice_date) as Year, sum(Quantity) as Qty, sum(Sales) from New_tab group by year(Invoice_date)").show()

# COMMAND ----------

spark.sql("select month(Invoice_date) as Month, sum(Quantity) as Qty, sum(Sales) from New_tab group by month(Invoice_date)").show()

# COMMAND ----------

from pyspark.sql.functions import quarter, col
df4 = df2.withColumn('Quarter', quarter(df.Invoice_date))
df4.show()

# COMMAND ----------

df4.createOrReplaceTempView("Temp_tab")

# COMMAND ----------

spark.sql("select Quarter, sum(Quantity) as Qty, sum(Sales) from Temp_tab group by Quarter").show()

# COMMAND ----------

df4.sort(col("Sales").desc()).show()

# COMMAND ----------

df5 = spark.createDataFrame([['2022-04-08']],['Current_date'])
df5.show()

# COMMAND ----------

from pyspark.sql.functions import *
df5.withColumn("After 5 days", date_add(df5.Current_date,5)).show()

# COMMAND ----------

df5.withColumn("Before 5 days", date_sub(df5.Current_date,5)).show()

# COMMAND ----------

def endOfMonthDate(col: Column): Column = {
  last_day(col)
}

# COMMAND ----------

date_add(last_day(ADD_MONTHS(CAST(CURRENT_TIMESTAMP AS DATE),-2)), +1).show()

# COMMAND ----------

spark.sql("select current_date, last_day(Date(current_date))").show()

# COMMAND ----------

from pyspark.sql.functions import quarter
spark.sql("select quarter(current_date)").show()

# COMMAND ----------

from pyspark.sql.functions import *
spark.sql("select date_add(date_trunc(cast(current_date as date), 'quarter')) as end_dt").show()

# COMMAND ----------

from pyspark.sql.functions import *
spark.sql("SELECT current_date AS InputDate,DATEADD(q,DATEDIFF(q,0,current_date),0) AS QuarterStartDate,DATEADD(q,DATEDIFF(q,0,current_date)+1,0) AS NextQuarterStartDate").show()

# COMMAND ----------



# COMMAND ----------

from datetime import date

def get_quarter(date):
    return (date.month - 1) / 3 + 1

def get_first_day_of_the_quarter(date):
    quarter = get_quarter(date)
    return datetime.datetime(date.year, 3 * quarter - 2, 1)

def get_last_day_of_the_quarter(date):
    quarter = get_quarter(date)
    month = 3 * quarter
    remaining = month / 12
    return datetime.datetime(date.year + remaining, month % 12 + 1, 1) + datetime.timedelta(days=-1)

print((date.month - 1) / 3 + 1)

# COMMAND ----------

from datetime import datetime,timedelta
current_date=datetime.now()
currQuarter = (current_date.month - 1) // 3 + 1
dtFirstDay = datetime(current_date.year, 3 * currQuarter - 2, 1)
dtLastDay = datetime(current_date.year, 3 * currQuarter + 1, 1) + timedelta(days=-1)

# COMMAND ----------

print(dtFirstDay)
print(dtLastDay)
print(currQuarter)

# COMMAND ----------


