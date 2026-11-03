# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from functools import reduce
from pyspark.sql.types import *

# COMMAND ----------

CATALOG = "vamsidatabricks"
SCHEMA = "ecommerce"

BASE_TABLE = f"{CATALOG}.{SCHEMA}"

PROCESSED_COLUMN = "_silver_processed_at"
SOURCE_COLUMN = "_source_system"

# COMMAND ----------

customer_schema = StructType([
    StructField("Customer_ID", IntegerType(), False),
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Postal_Code", StringType(), True),
    StructField("Created_Date", TimestampType(), True),
    StructField("Modified_Date", TimestampType(), True),
    StructField("Is_Active", BooleanType(), True)
])

product_schema = StructType([
    StructField("Product_ID", IntegerType(), False),
    StructField("Product_Name", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub_Category", StringType(), True),
    StructField("Supplier_ID", IntegerType(), True),
    StructField("Price", DecimalType(12, 2), False),
    StructField("Stock_Quantity", IntegerType(), False),
    StructField("Created_Date", TimestampType(), True),
    StructField("Modified_Date", TimestampType(), True),
    StructField("Is_Active", BooleanType(), True)
])

order_schema = StructType([
    StructField("Order_ID", LongType(), False),
    StructField("Customer_ID", IntegerType(), False),
    StructField("Order_Date", TimestampType(), False),
    StructField("Order_Status", StringType(), False),
    StructField("Payment_Method", StringType(), True),
    StructField("Shipping_City", StringType(), True),
    StructField("Shipping_State", StringType(), True),
    StructField("Total_Amount", DecimalType(12, 2), False),
    StructField("Created_Date", TimestampType(), True),
    StructField("Modified_Date", TimestampType(), True)
])

order_details_schema = StructType([
    StructField("OrderDetail_ID", LongType(), False),
    StructField("Order_ID", LongType(), False),
    StructField("Product_ID", IntegerType(), False),
    StructField("Quantity", IntegerType(), False),
    StructField("Unit_Price", DecimalType(12, 2), False),
    StructField("Discount", DecimalType(5, 2), False),
    StructField("Line_Total", DecimalType(12, 2), True)
])

payment_schema = StructType([
    StructField("Payment_ID", LongType(), False),
    StructField("Order_ID", LongType(), False),
    StructField("Payment_Date", TimestampType(), False),
    StructField("Payment_Method", StringType(), False),
    StructField("Payment_Status", StringType(), False),
    StructField("Amount", DecimalType(12, 2), False),
    StructField("Transaction_ID", StringType(), True)
])

# COMMAND ----------

schemas_dict = {
    "customers": customer_schema,
    "products": product_schema,
    "orders": order_schema,
    "order_details": order_details_schema,
    "payments": payment_schema
}

# COMMAND ----------

# DBTITLE 1,Read Latest Bronze File
# Method for reading bronze data
def read_bronze_table(table_name):

    storage_account = "manneadlsgen2"
    container_name = "ecommerce"

    # Get schema for the respective table
    final_schema = schemas_dict.get(table_name)

    if final_schema is None:
        raise ValueError(
            f"Schema not found for table: {table_name}"
        )
    spark.conf.set(
        "fs.azure.account.key.manneadlsgen2.dfs.core.windows.net",
        dbutils.secrets.get(
            scope="VamsiScope",
            key="adlsgen2"
        )
    )
    # ADLS Gen2 Bronze folder path
    folder_path = (
        f"abfss://ecommerce@manneadlsgen2.dfs.core.windows.net/"
        f"bronze/{table_name}/"
    )

    # Pick only the latest file: names look like {table_name}_yyyy-MM-dd HH:mm:ss.csv,
    # so the largest name is the newest file
    csv_files = [
        f for f in dbutils.fs.ls(folder_path)
        if f.name.startswith(f"{table_name}_") and f.name.endswith(".csv")
    ]

    if not csv_files:
        raise FileNotFoundError(
            f"No {table_name}_*.csv files found in {folder_path}"
        )

    source_path = max(csv_files, key=lambda f: f.name).path

    print(f"Reading latest Bronze CSV file: {source_path}")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(final_schema)
        .load(source_path)
    )

    return df

# COMMAND ----------

# standardize String columns to remove uncessary spaces and converts empty strings to null

def standardize_strings(df):
    string_columns = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    for column_name in string_columns:

        df = df.withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                None
            ).otherwise(F.trim(F.col(column_name)))
        )

    return df

# COMMAND ----------

# duplicate records

def deduplicate_latest(df, key_column, modified_column=None):
    if modified_column:
        window_spec = (
            Window
            .partitionBy(key_column)
            .orderBy(
                F.col(modified_column).desc_nulls_last()
            )
        )
        df = (
            df
            .withColumn(
                "_row_number",
                F.row_number().over(window_spec)
            )
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )
    else:
        df = df.dropDuplicates([key_column])
    return df

# COMMAND ----------

def split_valid_rejected(df, validation_rules):

    rejection_conditions = []

    for condition, reason in validation_rules:

        safe_condition = F.coalesce(
            condition,
            F.lit(False)
        )

        rejection_conditions.append(
            F.when(
                ~safe_condition,
                F.lit(reason)
            )
        )

    df = df.withColumn(
        "_reject_reason",
        F.concat_ws(
            " | ",
            *rejection_conditions
        )
    )

    valid_df = (
        df
        .filter(F.col("_reject_reason") == "")
        .drop("_reject_reason")
    )

    rejected_df = df.filter(
        F.col("_reject_reason") != ""
    )

    return valid_df, rejected_df

# COMMAND ----------

# adding audit columns 
def add_audit_columns(df):

    return (
        df
        .withColumn(
            PROCESSED_COLUMN,
            F.current_timestamp()
        )
        .withColumn(
            SOURCE_COLUMN,
            F.lit("AzureSQL")
        )
    )

# COMMAND ----------

# writing rejected records
def write_rejected_records(df, table_name):

    target_table = (
        f"{BASE_TABLE}.rejected_{table_name}"
    )

    if df.limit(1).count() > 0:

        (
            df
            .withColumn(
                "_rejected_at",
                F.current_timestamp()
            )
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

        print(f"Rejected records saved: {target_table}")

    else:

        print(f"No rejected records: {table_name}")

# COMMAND ----------

#Resuable delta MERGE function
def merge_to_silver(df, table_name, key_column):

    target_table = (
        f"{BASE_TABLE}.silver_{table_name}"
    )

    if not spark.catalog.tableExists(target_table):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        print(f"Created Silver table: {target_table}")

    else:

        delta_table = DeltaTable.forName(
            spark,
            target_table
        )

        update_columns = [
            column
            for column in df.columns
            if column != PROCESSED_COLUMN
        ]

        update_mapping = {
            column: f"source.`{column}`"
            for column in update_columns
        }

        change_conditions = [
            f"NOT (target.`{column}` <=> source.`{column}`)"
            for column in update_columns
            if column != key_column
        ]

        change_condition = " OR ".join(change_conditions)

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                f"target.`{key_column}` = source.`{key_column}`"
            )
            .whenMatchedUpdate(
                condition=change_condition,
                set=update_mapping
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Successfully merged: {target_table}")

# COMMAND ----------

# customers transformation
def transform_customers():

    df = read_bronze_table("customers")

    df = standardize_strings(df)

    df = deduplicate_latest(
        df,
        "Customer_ID",
        "Modified_Date"
    )

    df = (
        df
        .withColumn(
            "First_Name",
            F.initcap(F.col("First_Name"))
        )
        .withColumn(
            "Last_Name",
            F.initcap(F.col("Last_Name"))
        )
        .withColumn(
            "Email",
            F.lower(F.col("Email"))
        )
    )

    validation_rules = [

        (
            F.col("Customer_ID").isNotNull(),
            "Missing Customer ID"
        ),

        (
            F.col("First_Name").isNotNull(),
            "Missing First Name"
        ),

        (
            F.col("Last_Name").isNotNull(),
            "Missing Last Name"
        ),

        (
            F.col("Email").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            ),
            "Invalid Email"
        )
    ]

    valid_df, rejected_df = split_valid_rejected(
        df,
        validation_rules
    )

    valid_df = add_audit_columns(valid_df)

    write_rejected_records(
        rejected_df,
        "customers"
    )

    merge_to_silver(
        valid_df,
        "customers",
        "Customer_ID"
    )

    return valid_df

# COMMAND ----------

#Products Transformation
def transform_products():

    df = read_bronze_table("products")

    df = standardize_strings(df)

    df = deduplicate_latest(
        df,
        "Product_ID",
        "Modified_Date"
    )

    df = (
        df
        .withColumn(
            "Product_Name",
            F.initcap(F.col("Product_Name"))
        )
        .withColumn(
            "Category",
            F.initcap(F.col("Category"))
        )
        .withColumn(
            "Sub_Category",
            F.initcap(F.col("Sub_Category"))
        )
        .withColumn(
            "Price",
            F.col("Price").cast("decimal(12,2)")
        )
    )

    validation_rules = [

        (
            F.col("Product_ID").isNotNull(),
            "Missing Product ID"
        ),

        (
            F.col("Product_Name").isNotNull(),
            "Missing Product Name"
        ),

        (
            F.col("Category").isNotNull(),
            "Missing Category"
        ),

        (
            F.col("Price") > 0,
            "Invalid Product Price"
        ),

        (
            F.col("Stock_Quantity") >= 0,
            "Invalid Stock Quantity"
        )
    ]

    valid_df, rejected_df = split_valid_rejected(
        df,
        validation_rules
    )

    valid_df = add_audit_columns(valid_df)

    write_rejected_records(
        rejected_df,
        "products"
    )

    merge_to_silver(
        valid_df,
        "products",
        "Product_ID"
    )

    return valid_df

# COMMAND ----------

#Orders Transformation
def transform_orders():

    df = read_bronze_table("orders")

    df = standardize_strings(df)

    df = deduplicate_latest(
        df,
        "Order_ID",
        "Modified_Date"
    )

    df = (
        df
        .withColumn(
            "Order_Status",
            F.upper(F.col("Order_Status"))
        )
        .withColumn(
            "Total_Amount",
            F.col("Total_Amount").cast("decimal(12,2)")
        )
    )

    validation_rules = [

        (
            F.col("Order_ID").isNotNull(),
            "Missing Order ID"
        ),

        (
            F.col("Customer_ID").isNotNull(),
            "Missing Customer ID"
        ),

        (
            F.col("Order_Date").isNotNull(),
            "Missing Order Date"
        ),

        (
            F.col("Order_Status").isNotNull(),
            "Missing Order Status"
        ),

        (
            F.col("Total_Amount") >= 0,
            "Invalid Order Amount"
        )
    ]

    valid_df, rejected_df = split_valid_rejected(
        df,
        validation_rules
    )

    customer_keys = (
        spark.table(
            f"{BASE_TABLE}.silver_customers"
        )
        .select("Customer_ID")
        .dropDuplicates()
    )

    valid_orders = valid_df.join(
        customer_keys,
        on="Customer_ID",
        how="left_semi"
    )

    invalid_fk_orders = (
        valid_df
        .join(
            customer_keys,
            on="Customer_ID",
            how="left_anti"
        )
        .withColumn(
            "_reject_reason",
            F.lit("Customer ID not found in Silver Customers")
        )
    )

    rejected_df = rejected_df.unionByName(
        invalid_fk_orders,
        allowMissingColumns=True
    )

    valid_orders = add_audit_columns(valid_orders)

    write_rejected_records(
        rejected_df,
        "orders"
    )

    merge_to_silver(
        valid_orders,
        "orders",
        "Order_ID"
    )

    return valid_orders

# COMMAND ----------

#Order Details Transformation
def transform_order_details():

    df = read_bronze_table("order_details")

    df = standardize_strings(df)

    df = deduplicate_latest(
        df,
        "OrderDetail_ID"
    )

    df = (
        df
        .withColumn(
            "Unit_Price",
            F.col("Unit_Price").cast("decimal(12,2)")
        )
        .withColumn(
            "Discount",
            F.col("Discount").cast("decimal(5,2)")
        )
    )

    validation_rules = [

        (
            F.col("OrderDetail_ID").isNotNull(),
            "Missing OrderDetail ID"
        ),

        (
            F.col("Order_ID").isNotNull(),
            "Missing Order ID"
        ),

        (
            F.col("Product_ID").isNotNull(),
            "Missing Product ID"
        ),

        (
            F.col("Quantity") > 0,
            "Invalid Quantity"
        ),

        (
            F.col("Unit_Price") >= 0,
            "Invalid Unit Price"
        ),

        (
            (F.col("Discount") >= 0) &
            (F.col("Discount") <= 100),
            "Invalid Discount"
        )
    ]

    valid_df, rejected_df = split_valid_rejected(
        df,
        validation_rules
    )

    # Check Order foreign key
    order_keys = (
        spark.table(f"{BASE_TABLE}.silver_orders")
        .select("Order_ID")
        .dropDuplicates()
    )

    # Check Product foreign key
    product_keys = (
        spark.table(f"{BASE_TABLE}.silver_products")
        .select("Product_ID")
        .dropDuplicates()
    )

    valid_df = valid_df.join(
        order_keys,
        on="Order_ID",
        how="left_semi"
    )

    valid_df = valid_df.join(
        product_keys,
        on="Product_ID",
        how="left_semi"
    )

    # Recalculate Line_Total
    valid_df = valid_df.withColumn(
        "Line_Total",
        F.round(
            F.col("Quantity") *
            F.col("Unit_Price") *
            (
                F.lit(1) -
                F.col("Discount") / F.lit(100)
            ),
            2
        ).cast("decimal(12,2)")
    )

    valid_df = add_audit_columns(valid_df)

    write_rejected_records(
        rejected_df,
        "order_details"
    )

    merge_to_silver(
        valid_df,
        "order_details",
        "OrderDetail_ID"
    )

    return valid_df 

# COMMAND ----------

# payments Transformation
def transform_payments():

    df = read_bronze_table("payments")

    df = standardize_strings(df)

    df = deduplicate_latest(
        df,
        "Payment_ID"
    )

    df = (
        df
        .withColumn(
            "Payment_Status",
            F.upper(F.col("Payment_Status"))
        )
        .withColumn(
            "Payment_Method",
            F.upper(F.col("Payment_Method"))
        )
        .withColumn(
            "Amount",
            F.col("Amount").cast("decimal(12,2)")
        )
    )

    validation_rules = [

        (
            F.col("Payment_ID").isNotNull(),
            "Missing Payment ID"
        ),

        (
            F.col("Order_ID").isNotNull(),
            "Missing Order ID"
        ),

        (
            F.col("Payment_Date").isNotNull(),
            "Missing Payment Date"
        ),

        (
            F.col("Payment_Status").isNotNull(),
            "Missing Payment Status"
        ),

        (
            F.col("Amount") >= 0,
            "Invalid Payment Amount"
        )
    ]

    valid_df, rejected_df = split_valid_rejected(
        df,
        validation_rules
    )

    order_keys = (
        spark.table(f"{BASE_TABLE}.silver_orders")
        .select("Order_ID")
        .dropDuplicates()
    )

    valid_payments = valid_df.join(
        order_keys,
        on="Order_ID",
        how="left_semi"
    )

    invalid_payments = (
        valid_df
        .join(
            order_keys,
            on="Order_ID",
            how="left_anti"
        )
        .withColumn(
            "_reject_reason",
            F.lit("Order ID not found in Silver Orders")
        )
    )

    rejected_df = rejected_df.unionByName(
        invalid_payments,
        allowMissingColumns=True
    )

    valid_payments = add_audit_columns(
        valid_payments
    )

    write_rejected_records(
        rejected_df,
        "payments"
    )

    merge_to_silver(
        valid_payments,
        "payments",
        "Payment_ID"
    )

    return valid_payments

# COMMAND ----------

# ============================================================
# SILVER LAYER EXECUTION
# ============================================================

print("Starting Silver Layer Transformation")

# 1. Customers
customers_silver_df = transform_customers()

print("Customers transformation completed")


# 2. Products
products_silver_df = transform_products()

print("Products transformation completed")


# 3. Orders
orders_silver_df = transform_orders()

print("Orders transformation completed")


# 4. Order Details
order_details_silver_df = transform_order_details()

print("Order Details transformation completed")


# 5. Payments
payments_silver_df = transform_payments()

print("Payments transformation completed")


print("All Silver transformations completed successfully")

# COMMAND ----------

# Customers
customers_silver_df = spark.read.table(
    "vamsidatabricks.ecommerce.silver_customers"
)
display(customers_silver_df)

# Products
products_silver_df = spark.read.table(
    "vamsidatabricks.ecommerce.silver_products"
)
display(products_silver_df)

# Orders
orders_silver_df = spark.read.table(
    "vamsidatabricks.ecommerce.silver_orders"
)
display(orders_silver_df)

# Order Details
order_details_silver_df = spark.read.table(
    "vamsidatabricks.ecommerce.silver_order_details"
)
display(order_details_silver_df)

# Payments
payments_silver_df = spark.read.table(
    "vamsidatabricks.ecommerce.silver_payments"
)
display(payments_silver_df)