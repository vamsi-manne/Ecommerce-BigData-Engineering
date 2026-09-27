;WITH Numbers AS
(
    SELECT 1 AS n

    UNION ALL

    SELECT n + 1
    FROM Numbers
    WHERE n < 1000
)
INSERT INTO dbo.Products
(
    Product_Name,
    Category,
    Sub_Category,
    Supplier_ID,
    Price,
    Stock_Quantity,
    Created_Date,
    Modified_Date
)
SELECT
    CONCAT('Product ', n),

    CASE n % 6
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Home'
        WHEN 3 THEN 'Sports'
        WHEN 4 THEN 'Books'
        ELSE 'Beauty'
    END,

    CASE n % 6
        WHEN 0 THEN 'Mobile'
        WHEN 1 THEN 'Mens'
        WHEN 2 THEN 'Furniture'
        WHEN 3 THEN 'Fitness'
        WHEN 4 THEN 'Education'
        ELSE 'Personal Care'
    END,

    ((n - 1) % 100) + 1,

    CAST(10 + (n % 490) + ((n % 100) / 100.0) AS DECIMAL(12,2)),

    100 + (n % 900),

    DATEADD(DAY, -(n % 700), SYSUTCDATETIME()),

    DATEADD(DAY, -(n % 100), SYSUTCDATETIME())

FROM Numbers
OPTION (MAXRECURSION 0);