# Azure E-Commerce Data Engineering Pipeline

An end-to-end cloud data engineering project built using **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, and Azure SQL Database**.

The project demonstrates automated data ingestion, metadata-driven pipelines, data cleansing, data quality validation, Medallion Architecture, Slowly Changing Dimensions (SCD Type 2), and analytical data modelling.

## Project Overview

This project implements a scalable data engineering solution to ingest e-commerce data from Azure SQL Database, process it through a Bronze-Silver-Gold architecture, and prepare curated datasets for analytical reporting.

The pipeline processes five business datasets:

- Customers
- Products
- Orders
- OrderDetails
- Payments

## Architecture

<img width="1247" height="1374" alt="image" src="https://github.com/user-attachments/assets/3659bd6a-bde0-4893-b3dd-87260dbef3d5" />


## Technology Stack

| Category | Technologies |
|---|---|
| Source Database | Azure SQL Database |
| Data Integration | Azure Data Factory |
| Data Lake | Azure Data Lake Storage Gen2 |
| Data Processing | Azure Databricks |
| Programming | Python, PySpark, SQL |
| Storage Format | CSV, Delta Lake |
| Data Architecture | Medallion Architecture |
| Data Modelling | Star Schema |
| Historical Tracking | SCD Type 2 |
| Orchestration | ADF Pipelines |
| Reporting | Power BI (planned integration) |
| Version Control | Git, GitHub |

## Source Database

The source database is `EcommerceDB`, hosted in Azure SQL Database.

### Source Tables

| Table | Description |
|---|---|
| Customers | Customer information and location details |
| Products | Product details, categories, pricing, and inventory |
| Orders | Customer orders and order status |
| OrderDetails | Individual products and quantities within orders |
| Payments | Payment transactions and payment status |

## Medallion Architecture

### 1. Bronze Layer – Raw Data Ingestion

**Technology:** Azure Data Factory and ADLS Gen2

- Ingest data from Azure SQL Database.
- Implement metadata-driven ingestion using Lookup and ForEach activities.
- Use parameterized datasets for dynamic source tables and destination folders.
- Store source data in CSV format.
- Organize data into table-specific folders.
- Preserve source extracts for downstream processing and reprocessing.

Example folder structure:

```text
ecommerce/
│
└── bronze/
    ├── customers/
    ├── products/
    ├── orders/
    ├── order_details/
    └── payments/
```

### 2. Silver Layer – Data Transformation

**Technology:** Azure Databricks and PySpark

- Read Bronze data using predefined PySpark schemas.
- Standardize string and date fields.
- Remove duplicate records.
- Apply data quality validations.
- Validate primary and foreign key relationships.
- Handle null and invalid records.
- Separate rejected records for further investigation.
- Add audit columns.
- Store cleansed datasets as Delta tables.

Silver tables:

```text
vamsidatabricks.ecommerce
│
├── silver_customers
├── silver_products
├── silver_orders
├── silver_order_details
└── silver_payments
```

### 3. Gold Layer – Business-Ready Data

**Technology:** Azure Databricks and Delta Lake

Implement a Star Schema to support analytical queries and reporting.

**Dimension Tables**

| Table | Description |
|---|---|
| DimCustomer | Customer attributes and historical versions |
| DimProduct | Product attributes and historical versions |
| DimDate | Calendar attributes for reporting |

**Fact Tables**

| Table | Grain |
|---|---|
| FactOrders | One record per order |
| FactOrderDetails | One record per order detail |
| FactPayments | One record per payment |

### SCD Type 2 Implementation

Implement historical tracking for customer and product dimensions.

Tracked customer attributes include:

- Customer name
- Email and phone
- City, state, and country
- Postal code
- Active status

Tracked product attributes include:

- Product name
- Category and subcategory
- Supplier ID
- Price and stock quantity
- Active status

SCD Type 2 columns:

- Surrogate Key
- Effective_Start
- Effective_End
- Is_Current
- Attribute_Hash

When tracked attributes change, the previous dimension version is expired and a new version is inserted.

### Incremental Fact Loading

Implement Delta Lake MERGE operations for the fact tables.

- Insert new records.
- Update existing records when tracked values change.
- Maintain audit timestamps.
- Resolve historical dimension keys using transaction dates.

Note: MERGE-based upserts provide idempotent target updates. True source-incremental extraction requires watermarking, CDC, or another change-detection mechanism.

## Azure Data Factory Orchestration

The master pipeline is designed to coordinate the complete workflow.

### Master Pipeline

`PL_Master_Ecommerce`

Execution sequence:

1. Lookup_Metadata – Retrieve active source table configurations.
2. Set_Run_ID – Capture the pipeline execution ID.
3. ForEach_Table – Iterate through metadata records.
4. Check_Load_Type – Select full or incremental extraction.
5. Copy_SQL_To_ADLS – Ingest data into Bronze.
6. NB_Silver_Transformations – Execute Silver transformations.
7. NB_Gold_Dimensions – Build and update dimension tables.
8. NB_Gold_Facts – Load analytical fact tables.
9. Update_Watermarks – Update extraction watermarks after successful processing.

Failure handling:

- Capture failed activity details.
- Record error messages and pipeline run IDs.
- Log execution failures.
- Mark the master pipeline as failed.

## Data Quality and Validation

The project includes data validation rules such as:

- Mandatory customer and product identifiers.
- Valid customer email formats.
- Positive product prices.
- Nonnegative stock quantities.
- Valid order and payment amounts.
- Valid order-detail quantities and discounts.
- Foreign key validation between customers, orders, products, and payments.
- Duplicate record detection.

Invalid records are separated into rejected datasets instead of being silently included in the Silver layer.

## Setup and Execution

### Prerequisites

- Azure subscription
- Azure SQL Database
- Azure Data Factory
- Azure Data Lake Storage Gen2 account
- Azure Databricks workspace
- Unity Catalog metastore and catalog
- Git and GitHub account

### Step 1 – Create the Source Database

Create the `EcommerceDB` database and execute the SQL scripts under `sql/`.

### Step 2 – Configure ADLS Gen2

Create the `ecommerce` filesystem and configure the Bronze, Silver, Gold, rejected, archive, checkpoint, and schema directories as needed.

### Step 3 – Configure Azure Data Factory

- Create Azure SQL and ADLS linked services.
- Configure parameterized datasets.
- Import or create metadata-driven pipelines.
- Configure Databricks linked service.
- Test source-to-Bronze ingestion.

### Step 4 – Configure Databricks

- Configure secure access to ADLS Gen2.
- Create the Unity Catalog catalog and schemas.
- Run the notebooks in dependency order.
- Validate Silver and Gold Delta tables.

### Step 5 – Execute the Master Pipeline

Run `PL_Master_Ecommerce` and verify successful completion of each stage.

<img width="622" height="123" alt="image" src="https://github.com/user-attachments/assets/a17fb85f-5f83-448d-bc04-4918b0f8b42e" />

## Project Outcomes

The project design covers:

- 5 source datasets ingested from Azure SQL Database.
- 3-layer Medallion Architecture.
- 5 cleansed Silver datasets.
- 3 dimension tables and 3 fact tables.
- SCD Type 2 historical tracking for customers and products.
- Metadata-driven ADF ingestion and Databricks processing.

These are implementation scope metrics, not independently benchmarked performance improvements.

## Future Enhancements

- Implement Azure SQL Change Data Capture (CDC).
- Add automated data quality testing.
- Implement Databricks Workflows for independent job orchestration where appropriate.
- Integrate Power BI dashboards.
- Add CI/CD using Azure DevOps.
- Optimize Delta tables using partitioning and file compaction where appropriate.
- Add automated notifications and operational alerting.
- Implement secure authentication using managed identities and Unity Catalog storage credentials.

