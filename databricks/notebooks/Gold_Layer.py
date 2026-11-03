# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create Gold Schema
# MAGIC CREATE SCHEMA IF NOT EXISTS vamsidatabricks.ecommerce_gold;

# COMMAND ----------

GOLD_CATALOG = "vamsidatabricks"
GOLD_SCHEMA = "ecommerce_gold"

GOLD_BASE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}"
SILVER_BASE = "vamsidatabricks.ecommerce"

# COMMAND ----------

#create Dim Customer

customers_df = spark.read.table(
    f"{SILVER_BASE}.silver_customers"
)

dim_customer = (
    customers_df
    .select(
        "Customer_ID",
        "First_Name",
        "Last_Name",
        "Email",
        "Phone",
        "City",
        "State",
        "Country",
        "Postal_Code",
        "Is_Active"
    )
    .withColumn(
        "Customer_Name",
        F.concat_ws(" ", "First_Name", "Last_Name")
    )
    .withColumn(
        "Customer_Key",
        F.col("Customer_ID").cast("long")
    )
    .withColumn(
        "Gold_Created_At",
        F.current_timestamp()
    )
)

dim_customer.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_BASE}.DimCustomer")

# COMMAND ----------

#create Dim Products
products_df = spark.read.table(
    f"{SILVER_BASE}.silver_products"
)

dim_product = (
    products_df
    .select(
        "Product_ID",
        "Product_Name",
        "Category",
        "Sub_Category",
        "Supplier_ID",
        "Price",
        "Is_Active"
    )
    .withColumn(
        "Product_Key",
        F.col("Product_ID").cast("long")
    )
    .withColumn(
        "Gold_Created_At",
        F.current_timestamp()
    )
)

dim_product.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_BASE}.DimProduct")

# COMMAND ----------

#create Dim Date
orders_df = spark.read.table(
    f"{SILVER_BASE}.silver_orders"
)

payments_df = spark.read.table(
    f"{SILVER_BASE}.silver_payments"
)

all_dates = (
    orders_df.select(F.to_date("Order_Date").alias("Date"))
    .unionByName(
        payments_df.select(F.to_date("Payment_Date").alias("Date"))
    )
    .filter(F.col("Date").isNotNull())
)

date_bounds = all_dates.agg(
    F.min("Date").alias("min_date"),
    F.max("Date").alias("max_date")
).first()

if date_bounds["min_date"] is None:
    raise ValueError("No valid transaction dates found.")

date_dim = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(date_bounds["min_date"]),
                F.lit(date_bounds["max_date"]),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("Date")
    )
    .withColumn("Date_Key", F.date_format("Date", "yyyyMMdd").cast("int"))
    .withColumn("Year", F.year("Date"))
    .withColumn("Quarter", F.quarter("Date"))
    .withColumn("Month", F.month("Date"))
    .withColumn("Month_Name", F.date_format("Date", "MMMM"))
    .withColumn("Day", F.dayofmonth("Date"))
    .withColumn("Day_Name", F.date_format("Date", "EEEE"))
    .withColumn("Week_Of_Year", F.weekofyear("Date"))
)

date_dim.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_BASE}.DimDate")

# COMMAND ----------

#create Fact Orders
orders_df = spark.read.table(
    f"{SILVER_BASE}.silver_orders"
)

dim_customer = spark.read.table(
    f"{GOLD_BASE}.DimCustomer"
)

dim_date = spark.read.table(
    f"{GOLD_BASE}.DimDate"
)

fact_orders = (
    orders_df.alias("o")
    .join(
        dim_customer.alias("c"),
        F.col("o.Customer_ID") == F.col("c.Customer_ID"),
        "inner"
    )
    .join(
        dim_date.alias("d"),
        F.to_date(F.col("o.Order_Date")) == F.col("d.Date"),
        "left"
    )
    .select(
        F.col("o.Order_ID"),
        F.col("c.Customer_Key"),
        F.col("d.Date_Key").alias("Order_Date_Key"),
        F.col("o.Order_Status"),
        F.col("o.Payment_Method"),
        F.col("o.Shipping_City"),
        F.col("o.Shipping_State"),
        F.col("o.Total_Amount"),
        F.current_timestamp().alias("Gold_Created_At")
    )
)

fact_orders.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_BASE}.FactOrders")

# COMMAND ----------

#create Fact Order Details
order_details_df = spark.read.table(
    f"{SILVER_BASE}.silver_order_details"
)

orders_df = spark.read.table(
    f"{SILVER_BASE}.silver_orders"
)

dim_customer = spark.read.table(
    f"{GOLD_BASE}.DimCustomer"
)

dim_product = spark.read.table(
    f"{GOLD_BASE}.DimProduct"
)

dim_date = spark.read.table(
    f"{GOLD_BASE}.DimDate"
)

fact_order_details = (
    order_details_df.alias("od")
    .join(
        orders_df.alias("o"),
        F.col("od.Order_ID") == F.col("o.Order_ID"),
        "inner"
    )
    .join(
        dim_customer.alias("c"),
        F.col("o.Customer_ID") == F.col("c.Customer_ID"),
        "inner"
    )
    .join(
        dim_product.alias("p"),
        F.col("od.Product_ID") == F.col("p.Product_ID"),
        "inner"
    )
    .join(
        dim_date.alias("d"),
        F.to_date(F.col("o.Order_Date")) == F.col("d.Date"),
        "left"
    )
    .select(
        F.col("od.OrderDetail_ID"),
        F.col("od.Order_ID"),
        F.col("c.Customer_Key"),
        F.col("p.Product_Key"),
        F.col("d.Date_Key").alias("Order_Date_Key"),
        F.col("od.Quantity"),
        F.col("od.Unit_Price"),
        F.col("od.Discount"),
        F.col("od.Line_Total"),
        F.current_timestamp().alias("Gold_Created_At")
    )
)

fact_order_details.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_BASE}.FactOrderDetails")

# COMMAND ----------

#create Fact Payments
payments_df = spark.read.table(
    f"{SILVER_BASE}.silver_payments"
)

orders_df = spark.read.table(
    f"{SILVER_BASE}.silver_orders"
)

dim_customer = spark.read.table(
    f"{GOLD_BASE}.DimCustomer"
)

dim_date = spark.read.table(
    f"{GOLD_BASE}.DimDate"
)

fact_payments = (
    payments_df.alias("p")
    .join(
        orders_df.alias("o"),
        F.col("p.Order_ID") == F.col("o.Order_ID"),
        "inner"
    )
    .join(
        dim_customer.alias("c"),
        F.col("o.Customer_ID") == F.col("c.Customer_ID"),
        "inner"
    )
    .join(
        dim_date.alias("d"),
        F.to_date(F.col("p.Payment_Date")) == F.col("d.Date"),
        "left"
    )
    .select(
        F.col("p.Payment_ID"),
        F.col("p.Order_ID"),
        F.col("c.Customer_Key"),
        F.col("d.Date_Key").alias("Payment_Date_Key"),
        F.col("p.Payment_Method"),
        F.col("p.Payment_Status"),
        F.col("p.Amount"),
        F.col("p.Transaction_ID"),
        F.current_timestamp().alias("Gold_Created_At")
    )
)

fact_payments.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_BASE}.FactPayments")

# COMMAND ----------

# MAGIC %sql
# MAGIC --validate all Gold Layers
# MAGIC SHOW TABLES IN vamsidatabricks.ecommerce_gold;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 'DimCustomer' AS Table_Name, COUNT(*) AS Total_Records
# MAGIC FROM vamsidatabricks.ecommerce_gold.DimCustomer
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'DimProduct', COUNT(*)
# MAGIC FROM vamsidatabricks.ecommerce_gold.DimProduct
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'DimDate', COUNT(*)
# MAGIC FROM vamsidatabricks.ecommerce_gold.DimDate
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'FactOrders', COUNT(*)
# MAGIC FROM vamsidatabricks.ecommerce_gold.FactOrders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'FactOrderDetails', COUNT(*)
# MAGIC FROM vamsidatabricks.ecommerce_gold.FactOrderDetails
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'FactPayments', COUNT(*)
# MAGIC FROM vamsidatabricks.ecommerce_gold.FactPayments;

# COMMAND ----------

# display fact tables
display(
    spark.read.table(
        "vamsidatabricks.ecommerce_gold.FactOrderDetails"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total revenue by product

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.Product_Name,
# MAGIC     SUM(f.Line_Total) AS Total_Revenue,
# MAGIC     SUM(f.Quantity) AS Total_Quantity_Sold
# MAGIC FROM vamsidatabricks.ecommerce_gold.FactOrderDetails f
# MAGIC JOIN vamsidatabricks.ecommerce_gold.DimProduct p
# MAGIC     ON f.Product_Key = p.Product_Key
# MAGIC GROUP BY p.Product_Name
# MAGIC ORDER BY Total_Revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Monthly revenue:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.Year,
# MAGIC     d.Month_Name,
# MAGIC     SUM(f.Total_Amount) AS Total_Revenue
# MAGIC FROM vamsidatabricks.ecommerce_gold.FactOrders f
# MAGIC JOIN vamsidatabricks.ecommerce_gold.DimDate d
# MAGIC     ON f.Order_Date_Key = d.Date_Key
# MAGIC GROUP BY d.Year, d.Month, d.Month_Name
# MAGIC ORDER BY d.Year, MIN(d.Month);

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 customers by revenue:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.Customer_Name,
# MAGIC     SUM(f.Total_Amount) AS Total_Revenue,
# MAGIC     COUNT(DISTINCT f.Order_ID) AS Total_Orders
# MAGIC FROM vamsidatabricks.ecommerce_gold.FactOrders f
# MAGIC JOIN vamsidatabricks.ecommerce_gold.DimCustomer c
# MAGIC     ON f.Customer_Key = c.Customer_Key
# MAGIC GROUP BY c.Customer_Name
# MAGIC ORDER BY Total_Revenue DESC
# MAGIC LIMIT 10;