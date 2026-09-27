INSERT INTO dbo.OrderDetails
(
    Order_ID,
    Product_ID,
    Quantity,
    Unit_Price,
    Discount
)
SELECT
    Order_ID,

    ((Order_ID - 1000001) % 1000) + 1,

    ((Order_ID - 1000001) % 5) + 1,

    p.Price,

    CASE (Order_ID - 1000001) % 5
        WHEN 0 THEN 0
        WHEN 1 THEN 5
        WHEN 2 THEN 10
        WHEN 3 THEN 15
        ELSE 20
    END

FROM dbo.Orders o
JOIN dbo.Products p
    ON p.Product_ID =
       ((o.Order_ID - 1000001) % 1000) + 1;