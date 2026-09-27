;WITH Numbers AS
(
    SELECT 1 AS n

    UNION ALL

    SELECT n + 1
    FROM Numbers
    WHERE n < 100000
)
INSERT INTO dbo.Orders
(
    Customer_ID,
    Order_Date,
    Order_Status,
    Payment_Method,
    Shipping_City,
    Shipping_State,
    Total_Amount,
    Created_Date,
    Modified_Date
)
SELECT
    ((n - 1) % 10000) + 1,

    DATEADD(
        MINUTE,
        -(n % 500000),
        SYSUTCDATETIME()
    ),

    CASE n % 5
        WHEN 0 THEN 'Completed'
        WHEN 1 THEN 'Completed'
        WHEN 2 THEN 'Completed'
        WHEN 3 THEN 'Shipped'
        ELSE 'Cancelled'
    END,

    CASE n % 4
        WHEN 0 THEN 'Credit Card'
        WHEN 1 THEN 'Debit Card'
        WHEN 2 THEN 'PayPal'
        ELSE 'Apple Pay'
    END,

    CASE n % 8
        WHEN 0 THEN 'Toronto'
        WHEN 1 THEN 'Windsor'
        WHEN 2 THEN 'Ottawa'
        WHEN 3 THEN 'Mississauga'
        WHEN 4 THEN 'Brampton'
        WHEN 5 THEN 'Hamilton'
        WHEN 6 THEN 'London'
        ELSE 'Vancouver'
    END,

    CASE n % 5
        WHEN 0 THEN 'Ontario'
        WHEN 1 THEN 'British Columbia'
        WHEN 2 THEN 'Alberta'
        WHEN 3 THEN 'Quebec'
        ELSE 'Manitoba'
    END,

    CAST(
        20 + ((n * 17) % 980) + ((n % 100) / 100.0)
        AS DECIMAL(12,2)
    ),

    DATEADD(
        MINUTE,
        -(n % 500000),
        SYSUTCDATETIME()
    ),

    DATEADD(
        MINUTE,
        -(n % 100),
        SYSUTCDATETIME()
    )

FROM Numbers
OPTION (MAXRECURSION 0);