;WITH Numbers AS
(
    SELECT 1 AS n

    UNION ALL

    SELECT n + 1
    FROM Numbers
    WHERE n < 10000
)
INSERT INTO dbo.Customers
(
    First_Name,
    Last_Name,
    Email,
    Phone,
    City,
    State,
    Country,
    Postal_Code,
    Created_Date,
    Modified_Date
)
SELECT
    CONCAT('Customer', n),
    CONCAT('Last', n),
    CONCAT('customer', n, '@example.com'),
    CONCAT('416555', RIGHT('0000' + CAST(n AS VARCHAR(4)), 4)),

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

    'Canada',

    CONCAT(
        CASE n % 5
            WHEN 0 THEN 'M'
            WHEN 1 THEN 'N'
            WHEN 2 THEN 'K'
            WHEN 3 THEN 'H'
            ELSE 'R'
        END,
        RIGHT('0000' + CAST(n AS VARCHAR(4)), 4)
    ),

    DATEADD(DAY, -(n % 1000), SYSUTCDATETIME()),

    DATEADD(DAY, -(n % 100), SYSUTCDATETIME())

FROM Numbers
OPTION (MAXRECURSION 0);