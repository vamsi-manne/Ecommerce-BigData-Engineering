-- Create Customers Table
CREATE TABLE dbo.Customers
(
	Customer_ID INT Identity(1,1) PRIMARY KEY,
	First_Name VARCHAR(200) NOT NULL,
	Last_Name VARCHAR(200) NOT NULL,
	Email VARCHAR(200) NOT NULL,
	Phone VARCHAR(100),
	City VARCHAR(200),
	State VARCHAR(200),
	Country VARCHAR(100) DEFAULT 'Canada',
	Postal_Code VARCHAR(100),
	CREATED_DATE DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	MODIFIED_DATE DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	IS_ACTIVE BIT NOT NULL DEFAULT 1
);

-- Create Products Table
create TABLE dbo.Products
(
	Product_ID INT Identity(1,1) PRIMARY KEY,
	Product_Name VARCHAR(200) NOT NULL,
	Category VARCHAR(100) NOT NULL,
	Sub_Category varchar(100),
	Supplier_ID INT,
	Price Decimal(12,2) NOT NULL,
	Stock_Quantitiy INT NOT NULL DEFAULT 0,
	CREATED_DATE DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	MODIFIED_DATE DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	IS_ACTIVE BIT NOT NULL DEFAULT 1
);

-- Create Orders Table
CREATE TABLE dbo.Orders
(
	Order_ID BIGINT IDENTITY(1000001,1) PRIMARY KEY,
	Customer_ID INT NOT NULL,
	Order_Date DateTime2 NOT NULL,
	Order_Status    VARCHAR(30) NOT NULL,
	Payment_Method  VARCHAR(30),
    Shipping_City   VARCHAR(50),
    Shipping_State  VARCHAR(50),
    Total_Amount    DECIMAL(12,2) NOT NULL,
    Created_Date    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    Modified_Date   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

	CONSTRAINT FK_Orders_Customers FOREIGN KEY (Customer_ID) REFERENCES dbo.Customers(Customer_ID)
);

-- Create Order Details Table
CREATE TABLE dbo.OrderDetails
(
	OrderDetail_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
	Order_ID BIGINT NOT NULL,
	Product_ID INT NOT NULL,
	Quantity INT NOT NULL,
	Unit_Price DECIMAL(12,2) NOT NULL,
	Discount DECIMAL(5,2) NOT NULL DEFAULT 0,
	Line_Total AS (Quantity * Unit_Price * (1 - Discount / 100)) PERSISTED

	CONSTRAINT FK_OrderDetails_Orders FOREIGN KEY (Order_ID) REFERENCES dbo.Orders(Order_ID),
	CONSTRAINT FK_OrderDetails_Products FOREIGN KEY(Product_ID) REFERENCES dbo.Products(Product_ID)
);

-- Create Payments Table
CREATE TABLE dbo.Payments
(
	Payment_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
	Order_ID BIGINT NOT NULL,
	Payment_Date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	Payment_Method VARCHAR(100) NOT NULL,
	Payment_Status VARCHAR(100) NOT NULL,
	Amount Decimal(12,2) NOT NULL,
	Transaction_ID varchar(200),

	CONSTRAINT FK_Payments_Orders FOREIGN KEY (Order_ID) REFERENCES dbo.Orders(Order_ID)
);
