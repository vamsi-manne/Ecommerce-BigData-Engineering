# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

PRODUCT_SOURCE = "vamsidatabricks.ecommerce.silver_products"
PRODUCT_TARGET = "vamsidatabricks.ecommerce_gold.DimProduct_SCD2"

PRODUCT_TRACKED_COLUMNS = [
    "Product_Name",
    "Category",
    "Sub_Category",
    "Supplier_ID",
    "Price",
    "Stock_Quantity",
    "Is_Active"
]

# COMMAND ----------

def prepare_product_source():

    df = spark.read.table(PRODUCT_SOURCE)

    window = Window.partitionBy("Product_ID").orderBy(
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
            F.to_json(
                F.struct(*[
                    F.col(c) for c in PRODUCT_TRACKED_COLUMNS
                ])
            ),
            256
        )
    )

    return df

# COMMAND ----------

# DBTITLE 1,Initial Product SCD Type 2 Load
def initialize_product_scd2():

    source_df = prepare_product_source()

    window = Window.orderBy("Product_ID")

    initial_df = (
        source_df
        .withColumn(
            "Product_Key",
            F.row_number().over(window).cast("long")
        )
        .withColumn(
            "Effective_Start",
            # First version covers all history so past facts can match it
            F.to_timestamp(F.lit("1900-01-01 00:00:00"))
        )
        .withColumn(
            "Effective_End",
            F.to_timestamp(
                F.lit("9999-12-31 23:59:59")
            )
        )
        .withColumn("Is_Current", F.lit(True))
        .withColumn("Gold_Created_At", F.current_timestamp())
    )

    (
        initial_df.write
        .format("delta")
        .mode("error")
        .saveAsTable(PRODUCT_TARGET)
    )

    print("Initial Product SCD Type 2 load completed.")

# COMMAND ----------

def incremental_product_scd2():

    source_df = prepare_product_source()

    target_df = spark.read.table(PRODUCT_TARGET)

    run_ts = spark.sql(
        "SELECT current_timestamp() AS ts"
    ).first()["ts"]

    current_df = (
        target_df
        .filter(F.col("Is_Current") == True)
        .select(
            "Product_ID",
            "Attribute_Hash"
        )
    )

    # Identify new and changed products
    staged_df = (
        source_df.alias("s")
        .join(
            current_df.alias("t"),
            F.col("s.Product_ID") == F.col("t.Product_ID"),
            "left"
        )
        .filter(
            F.col("t.Product_ID").isNull() |
            (
                F.col("s.Attribute_Hash") !=
                F.col("t.Attribute_Hash")
            )
        )
        .select("s.*")
    )

    if staged_df.isEmpty():
        print("No new or changed products.")
        return

    # Expire old versions
    changed_existing = staged_df.select(
        "Product_ID",
        "Attribute_Hash"
    )

    target = DeltaTable.forName(spark, PRODUCT_TARGET)

    (
        target.alias("t")
        .merge(
            changed_existing.alias("s"),
            """
            t.Product_ID = s.Product_ID
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
        spark.read.table(PRODUCT_TARGET)
        .agg(F.max("Product_Key").alias("max_key"))
        .first()["max_key"] or 0
    )

    key_window = Window.orderBy("Product_ID")

    new_versions = (
        staged_df
        .withColumn(
            "Product_Key",
            (
                F.row_number().over(key_window) +
                F.lit(max_key)
            ).cast("long")
        )
        .withColumn("Effective_Start", F.lit(run_ts))
        .withColumn(
            "Effective_End",
            F.to_timestamp(
                F.lit("9999-12-31 23:59:59")
            )
        )
        .withColumn("Is_Current", F.lit(True))
        .withColumn("Gold_Created_At", F.lit(run_ts))
    )

    (
        new_versions.write
        .format("delta")
        .mode("append")
        .saveAsTable(PRODUCT_TARGET)
    )

    print("Incremental Product SCD Type 2 load completed.")

# COMMAND ----------

if not spark.catalog.tableExists(PRODUCT_TARGET):
    initialize_product_scd2()
else:
    incremental_product_scd2()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     Product_Key,
# MAGIC     Product_ID,
# MAGIC     Product_Name,
# MAGIC     Category,
# MAGIC     Price,
# MAGIC     Stock_Quantity,
# MAGIC     Effective_Start,
# MAGIC     Effective_End,
# MAGIC     Is_Current
# MAGIC FROM vamsidatabricks.ecommerce_gold.DimProduct_SCD2
# MAGIC ORDER BY Product_ID, Effective_Start;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     Product_ID,
# MAGIC     COUNT(*) AS Total_Versions
# MAGIC FROM vamsidatabricks.ecommerce_gold.DimProduct_SCD2
# MAGIC GROUP BY Product_ID
# MAGIC HAVING COUNT(*) > 1;