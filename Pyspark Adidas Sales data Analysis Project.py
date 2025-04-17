# Databricks notebook source
# MAGIC %md
# MAGIC # Adidas sales data Analysis
# MAGIC
# MAGIC #### The objective of this project is to analyze the Adidas sales database for the year 2020 and 2021 and identify key insights to help improve sales performance and optimize business strategies.
# MAGIC
# MAGIC #### By analyzing the sales data, we aim to understand factors influencing sales, identify trends, and uncover opportunities for growth. The analysis will be conducted using databricks Notebook to provide an interactive and insightful dashboard.
# MAGIC
# MAGIC
# MAGIC **Business Metrics requirements**
# MAGIC - Total Sales, Total Profit, Average Price per Unit, and Total Units Sold
# MAGIC - Total sales by month
# MAGIC - Total sales by state
# MAGIC - total sales by region
# MAGIC - Total sales by product
# MAGIC - Total sales by retailer
# MAGIC - Units Sold by Product Category and Gender Type
# MAGIC - Top Performing Cities by Profit

# COMMAND ----------

# MAGIC %md
# MAGIC /FileStore/tables/Adidas_US_Sales_Datasets.csv

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/Adidas_US_Sales_Datasets.csv",header=True,inferSchema=True)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("sales_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_analysis

# COMMAND ----------

# DBTITLE 1,Total Sales, Total Profit, Average Price per Unit, and Total Units Sold
# MAGIC %sql
# MAGIC select sum(`Total Sales`) as Total_sales ,
# MAGIC  sum(`Operating Profit`) as Total_profit, 
# MAGIC  avg(`Price per Unit`) as Average_price_per_unit,
# MAGIC  sum(`Units Sold`) as Total_unit_sold
# MAGIC from sales_analysis

# COMMAND ----------

# DBTITLE 1,Total sales by Month
# MAGIC %sql
# MAGIC SELECT 
# MAGIC SUM(`Total Sales`) AS Total_sales, 
# MAGIC DATE_FORMAT(`Invoice Date`, 'MMMM') AS Month_Name,
# MAGIC MONTH(`Invoice Date`) AS Month_Number
# MAGIC FROM sales_analysis
# MAGIC GROUP BY Month_Name, Month_Number
# MAGIC ORDER BY Month_Number

# COMMAND ----------

# DBTITLE 1,Total sales by state
# MAGIC %sql
# MAGIC select sum(`Total Sales`) as Total_sales , State
# MAGIC from sales_analysis
# MAGIC group by State
# MAGIC order by Total_sales desc limit 5

# COMMAND ----------

# DBTITLE 1,Total sales by Region
# MAGIC %sql
# MAGIC select sum(`Total Sales`) as Total_sales , Region
# MAGIC from sales_analysis
# MAGIC group by Region
# MAGIC order by Total_sales desc

# COMMAND ----------

# DBTITLE 1,Total sales by product
# MAGIC %sql
# MAGIC select sum(`Total Sales`) as Total_sales , Product
# MAGIC from sales_analysis
# MAGIC group by Product
# MAGIC order by Total_sales desc limit 5

# COMMAND ----------

# DBTITLE 1,Total Sales By Retailer
# MAGIC %sql
# MAGIC select sum(`Total Sales`) as Total_sales , Retailer
# MAGIC from sales_analysis
# MAGIC group by Retailer
# MAGIC order by Total_sales desc

# COMMAND ----------

# DBTITLE 1,Units Sold by Product Category and Gender Type
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SPLIT(Product, ' ')[0] AS Gender,
# MAGIC   SUBSTRING(Product, LENGTH(SPLIT(Product, ' ')[0]) + 2) AS Product_Category, 
# MAGIC   SUM(`Units Sold`) AS Total_Units_Sold
# MAGIC FROM sales_analysis
# MAGIC GROUP BY Gender,Product_Category
# MAGIC ORDER BY Total_Units_Sold;

# COMMAND ----------

# DBTITLE 1,Top Performing Cities by Profit
# MAGIC %sql
# MAGIC select sum(`Operating Profit`) as Total_Profit , city
# MAGIC from sales_analysis
# MAGIC group by City
# MAGIC order by Total_Profit desc limit 6

# COMMAND ----------

