INSERT INTO dbo.Payments
(
    Order_ID,
    Payment_Date,
    Payment_Method,
    Payment_Status,
    Amount,
    Transaction_ID
)
SELECT
    o.Order_ID,

    DATEADD(MINUTE, 5, o.Order_Date),

    o.Payment_Method,

    CASE
        WHEN o.Order_Status = 'Cancelled'
            THEN 'Refunded'
        ELSE 'Completed'
    END,

    o.Total_Amount,

    CONCAT('TXN-', o.Order_ID)

FROM dbo.Orders o;