# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_BASE = "vamsidatabricks.ecommerce"
GOLD_BASE = "vamsidatabricks.ecommerce_gold"

CUSTOMER_DIM = f"{GOLD_BASE}.DimCustomer_SCD2"
PRODUCT_DIM = f"{GOLD_BASE}.DimProduct_SCD2"
DATE_DIM = f"{GOLD_BASE}.DimDate"

FACT_ORDERS = f"{GOLD_BASE}.FactOrders"
FACT_ORDER_DETAILS = f"{GOLD_BASE}.FactOrderDetails"
FACT_PAYMENTS = f"{GOLD_BASE}.FactPayments"

# COMMAND ----------

def lookup_customer_scd2(df, customer_id_col, event_date_col):

    customer_dim = spark.read.table(CUSTOMER_DIM)

    return (
        df.alias("src")
        .join(
            customer_dim.alias("cust"),
            (
                F.col(f"src.{customer_id_col}") ==
                F.col("cust.Customer_ID")
            ) &
            (
                F.col(f"src.{event_date_col}") >=
                F.col("cust.Effective_Start")
            ) &
            (
                F.col(f"src.{event_date_col}") <
                F.col("cust.Effective_End")
            ),
            "left"
        )
        .select(
            "src.*",
            F.col("cust.Customer_Key")
        )
    )

# COMMAND ----------

def lookup_product_scd2(df, product_id_col, event_date_col):

    product_dim = spark.read.table(PRODUCT_DIM)

    return (
        df.alias("src")
        .join(
            product_dim.alias("prod"),
            (
                F.col(f"src.{product_id_col}") ==
                F.col("prod.Product_ID")
            ) &
            (
                F.col(f"src.{event_date_col}") >=
                F.col("prod.Effective_Start")
            ) &
            (
                F.col(f"src.{event_date_col}") <
                F.col("prod.Effective_End")
            ),
            "left"
        )
        .select(
            "src.*",
            F.col("prod.Product_Key")
        )
    )

# COMMAND ----------

def lookup_date_key(df, event_date_col, output_col):

    date_dim = spark.read.table(DATE_DIM)

    return (
        df.alias("src")
        .join(
            date_dim.alias("dt"),
            F.to_date(
                F.col(f"src.{event_date_col}")
            ) == F.col("dt.Date"),
            "left"
        )
        .select(
            "src.*",
            F.col("dt.Date_Key").alias(output_col)
        )
    )

# COMMAND ----------

def incremental_fact_merge(
    source_df,
    target_table,
    primary_key
):

    # Add a hash to identify changes
    business_columns = [
        c for c in source_df.columns
        if c not in [
            "Gold_Created_At",
            "Gold_Updated_At",
            "Fact_Hash"
        ]
    ]

    source_df = (
        source_df
        .withColumn(
            "Fact_Hash",
            F.sha2(
                F.to_json(
                    F.struct(*[
                        F.col(c) for c in business_columns
                    ])
                ),
                256
            )
        )
        .withColumn(
            "Gold_Created_At",
            F.current_timestamp()
        )
        .withColumn(
            "Gold_Updated_At",
            F.current_timestamp()
        )
    )

    # Check for duplicate fact keys
    duplicate_keys = (
        source_df.groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicate_keys.limit(1).count() > 0:
        raise ValueError(
            f"Duplicate {primary_key} values in {target_table}"
        )

    # Create an empty Delta target on the first run
    if not spark.catalog.tableExists(target_table):
        (
            source_df.limit(0)
            .write.format("delta")
            .saveAsTable(target_table)
        )

    target = DeltaTable.forName(spark, target_table)

    update_columns = [
        c for c in source_df.columns
        if c != "Gold_Created_At"
    ]

    update_mapping = {
        c: F.col(f"s.{c}")
        for c in update_columns
    }

    (
        target.alias("t")
        .merge(
            source_df.alias("s"),
            f"t.{primary_key} = s.{primary_key}"
        )
        .whenMatchedUpdate(
            condition="t.Fact_Hash <> s.Fact_Hash",
            set=update_mapping
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Incremental MERGE completed: {target_table}")

# COMMAND ----------

def load_fact_orders():

    orders = spark.read.table(
        f"{SILVER_BASE}.silver_orders"
    )

    # Resolve historical customer version
    df = lookup_customer_scd2(
        orders,
        "Customer_ID",
        "Order_Date"
    )

    # Resolve order date key
    df = lookup_date_key(
        df,
        "Order_Date",
        "Order_Date_Key"
    )

    # Validate dimension lookups
    if df.filter(
        F.col("Customer_Key").isNull() |
        F.col("Order_Date_Key").isNull()
    ).limit(1).count() > 0:

        raise ValueError(
            "FactOrders contains unmatched customer or date keys."
        )

    fact_df = df.select(
        "Order_ID",
        "Customer_Key",
        "Order_Date_Key",
        "Order_Date",
        "Order_Status",
        "Payment_Method",
        "Shipping_City",
        "Shipping_State",
        "Total_Amount"
    )

    incremental_fact_merge(
        fact_df,
        FACT_ORDERS,
        "Order_ID"
    )

# COMMAND ----------

def load_fact_order_details():

    details = spark.read.table(
        f"{SILVER_BASE}.silver_order_details"
    )

    orders = spark.read.table(
        f"{SILVER_BASE}.silver_orders"
    )

    # Bring order information into the detail records
    df = (
        details.alias("od")
        .join(
            orders.alias("o"),
            F.col("od.Order_ID") == F.col("o.Order_ID"),
            "inner"
        )
        .select(
            F.col("od.OrderDetail_ID"),
            F.col("od.Order_ID"),
            F.col("od.Product_ID"),
            F.col("od.Quantity"),
            F.col("od.Unit_Price"),
            F.col("od.Discount"),
            F.col("od.Line_Total"),
            F.col("o.Customer_ID"),
            F.col("o.Order_Date")
        )
    )

    # Historical customer lookup
    df = lookup_customer_scd2(
        df,
        "Customer_ID",
        "Order_Date"
    )

    # Historical product lookup
    df = lookup_product_scd2(
        df,
        "Product_ID",
        "Order_Date"
    )

    # Date lookup
    df = lookup_date_key(
        df,
        "Order_Date",
        "Order_Date_Key"
    )

    # Validate dimension lookups
    if df.filter(
        F.col("Customer_Key").isNull() |
        F.col("Product_Key").isNull() |
        F.col("Order_Date_Key").isNull()
    ).limit(1).count() > 0:

        raise ValueError(
            "FactOrderDetails contains unmatched dimension keys."
        )

    fact_df = df.select(
        "OrderDetail_ID",
        "Order_ID",
        "Customer_Key",
        "Product_Key",
        "Order_Date_Key",
        "Order_Date",
        "Quantity",
        "Unit_Price",
        "Discount",
        "Line_Total"
    )

    incremental_fact_merge(
        fact_df,
        FACT_ORDER_DETAILS,
        "OrderDetail_ID"
    )

# COMMAND ----------

def load_fact_payments():

    payments = spark.read.table(
        f"{SILVER_BASE}.silver_payments"
    )

    orders = spark.read.table(
        f"{SILVER_BASE}.silver_orders"
    )

    # Get customer ID from the related order
    df = (
        payments.alias("p")
        .join(
            orders.alias("o"),
            F.col("p.Order_ID") == F.col("o.Order_ID"),
            "inner"
        )
        .select(
            F.col("p.Payment_ID"),
            F.col("p.Order_ID"),
            F.col("p.Payment_Date"),
            F.col("p.Payment_Method"),
            F.col("p.Payment_Status"),
            F.col("p.Amount"),
            F.col("p.Transaction_ID"),
            F.col("o.Customer_ID")
        )
    )

    # Resolve customer version at payment date
    df = lookup_customer_scd2(
        df,
        "Customer_ID",
        "Payment_Date"
    )

    # Resolve payment date key
    df = lookup_date_key(
        df,
        "Payment_Date",
        "Payment_Date_Key"
    )

    # Validate dimension lookups
    if df.filter(
        F.col("Customer_Key").isNull() |
        F.col("Payment_Date_Key").isNull()
    ).limit(1).count() > 0:

        raise ValueError(
            "FactPayments contains unmatched customer or date keys."
        )

    fact_df = df.select(
        "Payment_ID",
        "Order_ID",
        "Customer_Key",
        "Payment_Date_Key",
        "Payment_Date",
        "Payment_Method",
        "Payment_Status",
        "Amount",
        "Transaction_ID"
    )

    incremental_fact_merge(
        fact_df,
        FACT_PAYMENTS,
        "Payment_ID"
    )

# COMMAND ----------

load_fact_orders()

load_fact_order_details()

load_fact_payments()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'FactOrders' AS Table_Name, COUNT(*) AS Total_Records
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

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vamsidatabricks.ecommerce_gold.FactOrderDetails
# MAGIC WHERE Customer_Key IS NULL
# MAGIC    OR Product_Key IS NULL
# MAGIC    OR Order_Date_Key IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     f.Order_ID,
# MAGIC     f.Order_Date,
# MAGIC     c.Customer_ID,
# MAGIC     c.City AS Customer_City_At_Order,
# MAGIC     p.Product_ID,
# MAGIC     p.Product_Name,
# MAGIC     p.Price AS Product_Price_At_Order
# MAGIC FROM vamsidatabricks.ecommerce_gold.FactOrderDetails f
# MAGIC JOIN vamsidatabricks.ecommerce_gold.DimCustomer_SCD2 c
# MAGIC     ON f.Customer_Key = c.Customer_Key
# MAGIC JOIN vamsidatabricks.ecommerce_gold.DimProduct_SCD2 p
# MAGIC     ON f.Product_Key = p.Product_Key
# MAGIC LIMIT 20;