CREATE INDEX IX_Customers_ModifiedDate
ON dbo.Customers(Modified_Date);

CREATE INDEX IX_Products_ModifiedDate
ON dbo.Products(Modified_Date);

CREATE INDEX IX_Orders_ModifiedDate
ON dbo.Orders(Modified_Date);

CREATE INDEX IX_Orders_CustomerID
ON dbo.Orders(Customer_ID);

CREATE INDEX IX_OrderDetails_OrderID
ON dbo.OrderDetails(Order_ID);

CREATE INDEX IX_OrderDetails_ProductID
ON dbo.OrderDetails(Product_ID);

CREATE INDEX IX_Payments_OrderID
ON dbo.Payments(Order_ID);