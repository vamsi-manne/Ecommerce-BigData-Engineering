SELECT
    o.Order_ID,
    c.Customer_ID,
    CONCAT(c.First_Name, ' ', c.Last_Name) AS Customer_Name,
    p.Product_Name,
    od.Quantity,
    od.Unit_Price,
    od.Discount,
    od.Line_Total,
    o.Order_Status,
    o.Order_Date
FROM dbo.Orders o
JOIN dbo.Customers c
    ON o.Customer_ID = c.Customer_ID
JOIN dbo.OrderDetails od
    ON o.Order_ID = od.Order_ID
JOIN dbo.Products p
    ON od.Product_ID = p.Product_ID;