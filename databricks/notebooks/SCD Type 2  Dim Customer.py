# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 for customer Dimension

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CUSTOMER_SOURCE = "vamsidatabricks.ecommerce.silver_customers"
CUSTOMER_TARGET = "vamsidatabricks.ecommerce_gold.DimCustomer_SCD2"

TRACKED_COLUMNS = [
    "First_Name",
    "Last_Name",
    "Email",
    "Phone",
    "City",
    "State",
    "Country",
    "Postal_Code",
    "Is_Active"
]

# COMMAND ----------

# prepare customer data
def prepare_customer_source():

    df = spark.read.table(CUSTOMER_SOURCE)

    # Keep the latest source record per customer
    window = Window.partitionBy("Customer_ID").orderBy(
        F.col("Modified_Date").desc_nulls_last()
    )

    df = (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

    df = df.withColumn(
        "Attribute_Hash",
        F.sha2(
            F.to_json(F.struct(*[
                F.col(c) for c in TRACKED_COLUMNS
            ])),
            256
        )
    )

    return df

# COMMAND ----------

# DBTITLE 1,Initial SCD Type 2 Load
#initial SCD Type 2 Load
def initialize_customer_scd2():

    source_df = prepare_customer_source()

    window = Window.orderBy("Customer_ID")

    initial_df = (
        source_df
        .withColumn(
            "Customer_Key",
            F.row_number().over(window).cast("long")
        )
        .withColumn(
            "Effective_Start",
            # First version covers all history so past facts can match it
            F.to_timestamp(F.lit("1900-01-01 00:00:00"))
        )
        .withColumn(
            "Effective_End",
            F.to_timestamp(F.lit("9999-12-31 23:59:59"))
        )
        .withColumn("Is_Current", F.lit(True))
        .withColumn("Gold_Created_At", F.current_timestamp())
    )

    (
        initial_df.write
        .format("delta")
        .mode("error")
        .saveAsTable(CUSTOMER_TARGET)
    )

    print("Initial SCD Type 2 customer load completed.")

# COMMAND ----------

#incremental SCD Type 2 Load
def incremental_customer_scd2():

    source_df = prepare_customer_source()

    target_df = spark.read.table(CUSTOMER_TARGET)

    # Capture one consistent timestamp for this run
    run_ts = spark.sql(
        "SELECT current_timestamp() AS ts"
    ).first()["ts"]

    current_df = (
        target_df
        .filter(F.col("Is_Current") == True)
        .select(
            "Customer_ID",
            "Attribute_Hash"
        )
    )

    # Find new or changed customers
    staged_df = (
        source_df.alias("s")
        .join(
            current_df.alias("t"),
            F.col("s.Customer_ID") == F.col("t.Customer_ID"),
            "left"
        )
        .filter(
            F.col("t.Customer_ID").isNull() |
            (
                F.col("s.Attribute_Hash") !=
                F.col("t.Attribute_Hash")
            )
        )
        .select("s.*")
    )

    if staged_df.isEmpty():
        print("No new or changed customers.")
        return

    # Expire previous versions of changed customers
    changed_existing = (
        staged_df
        .select("Customer_ID", "Attribute_Hash")
    )

    target = DeltaTable.forName(spark, CUSTOMER_TARGET)

    (
        target.alias("t")
        .merge(
            changed_existing.alias("s"),
            """
            t.Customer_ID = s.Customer_ID
            AND t.Is_Current = true
            AND t.Attribute_Hash <> s.Attribute_Hash
            """
        )
        .whenMatchedUpdate(set={
            "Effective_End": F.lit(run_ts),
            "Is_Current": F.lit(False)
        })
        .execute()
    )

    # Generate new surrogate keys
    max_key = (
        spark.read.table(CUSTOMER_TARGET)
        .agg(F.max("Customer_Key").alias("max_key"))
        .first()["max_key"] or 0
    )

    key_window = Window.orderBy("Customer_ID")

    new_versions = (
        staged_df
        .withColumn(
            "Customer_Key",
            (F.row_number().over(key_window) + F.lit(max_key)).cast("long")
        )
        .withColumn("Effective_Start", F.lit(run_ts))
        .withColumn(
            "Effective_End",
            F.to_timestamp(F.lit("9999-12-31 23:59:59"))
        )
        .withColumn("Is_Current", F.lit(True))
        .withColumn("Gold_Created_At", F.lit(run_ts))
    )

    (
        new_versions.write
        .format("delta")
        .mode("append")
        .saveAsTable(CUSTOMER_TARGET)
    )

    print("Incremental SCD Type 2 customer load completed.")

# COMMAND ----------

#Execute
if not spark.catalog.tableExists(CUSTOMER_TARGET):
    initialize_customer_scd2()
else:
    incremental_customer_scd2()

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %sql
# MAGIC SELECT
# MAGIC     Customer_Key,
# MAGIC     Customer_ID,
# MAGIC     concat_ws(' ', First_Name, Last_Name) AS Customer_Name,
# MAGIC     City,
# MAGIC     Effective_Start,
# MAGIC     Effective_End,
# MAGIC     Is_Current
# MAGIC FROM vamsidatabricks.ecommerce_gold.DimCustomer_SCD2
# MAGIC ORDER BY Customer_ID, Effective_Start;

# COMMAND ----------

# MAGIC %sql
# MAGIC --check current customer records
# MAGIC SELECT *
# MAGIC FROM vamsidatabricks.ecommerce_gold.DimCustomer_SCD2
# MAGIC WHERE Is_Current = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check customers with multiple historical versions 
# MAGIC SELECT
# MAGIC     Customer_ID,
# MAGIC     COUNT(*) AS Total_Versions
# MAGIC FROM vamsidatabricks.ecommerce_gold.DimCustomer_SCD2
# MAGIC GROUP BY Customer_ID
# MAGIC HAVING COUNT(*) > 1;