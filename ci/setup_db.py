import pyodbc
import time

def get_connection(database='master'):
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=127.0.0.1,1433;'
        'UID=sa;'
        'PWD=YourStrong@Passw0rd;'
        'TrustServerCertificate=yes;'
        'Autocommit=True;'
    )
    # Retry logic for connection
    for i in range(5):
        try:
            conn = pyodbc.connect(conn_str, autocommit=True)
            if database != 'master':
                conn.execute(f"USE {database}")
            return conn
        except Exception as e:
            print(f"Connection attempt {i+1} failed: {e}")
            time.sleep(2)
    raise Exception("Failed to connect to SQL Server")

def setup_database():
    print("Setting up CI database...")
    
    # 1. Create Database
    conn = get_connection('master')
    try:
        conn.execute("CREATE DATABASE AdventureWorks2014")
        print("Database AdventureWorks2014 created.")
    except Exception as e:
        print(f"Database creation skipped/failed: {e}")
    conn.close()
    
    # 2. Create Schemas and Tables
    conn = get_connection('AdventureWorks2014')
    cursor = conn.cursor()
    
    schemas = ['Sales', 'Production', 'Person']
    for schema in schemas:
        try:
            cursor.execute(f"CREATE SCHEMA {schema}")
            print(f"Schema {schema} created.")
        except Exception:
            pass # Schema might exist

    # Types (mock) - Create BEFORE tables that use them
    print("Creating Types...")
    try:
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.types WHERE name = 'NameStyle') CREATE TYPE dbo.NameStyle FROM bit")
        print("Type dbo.NameStyle created.")
    except Exception as e:
        print(f"Type creation failed: {e}")
            
    # Table: Person.Person
    print("Creating Person.Person...")
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Person.Person') AND type in (N'U'))
        CREATE TABLE Person.Person(
            BusinessEntityID int PRIMARY KEY,
            PersonType nchar(2),
            NameStyle dbo.NameStyle,
            Title nvarchar(8),
            FirstName nvarchar(50),
            MiddleName nvarchar(50),
            LastName nvarchar(50),
            Suffix nvarchar(10),
            EmailPromotion int,
            AdditionalContactInfo xml,
            Demographics xml,
            rowguid uniqueidentifier,
            ModifiedDate datetime
        )
    """)
    cursor.execute("INSERT INTO Person.Person (BusinessEntityID, FirstName, LastName, EmailPromotion, ModifiedDate) VALUES (1, 'Ken', 'Sanchez', 0, GETDATE())")

    # Table: Sales.Customer
    print("Creating Sales.Customer...")
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Sales.Customer') AND type in (N'U'))
        CREATE TABLE Sales.Customer(
            CustomerID int PRIMARY KEY,
            PersonID int,
            StoreID int,
            TerritoryID int,
            AccountNumber varchar(10),
            rowguid uniqueidentifier,
            ModifiedDate datetime
        )
    """)
    cursor.execute("INSERT INTO Sales.Customer (CustomerID, PersonID, StoreID, TerritoryID, ModifiedDate) VALUES (1, 1, 1, 1, GETDATE())")

    # Table: Production.Product
    print("Creating Production.Product...")
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Production.Product') AND type in (N'U'))
        CREATE TABLE Production.Product(
            ProductID int PRIMARY KEY,
            Name nvarchar(50),
            ProductNumber nvarchar(25),
            MakeFlag bit,
            FinishedGoodsFlag bit,
            Color nvarchar(15),
            SafetyStockLevel smallint,
            ReorderPoint smallint,
            StandardCost money,
            ListPrice money,
            Size nvarchar(5),
            SizeUnitMeasureCode nchar(3),
            WeightUnitMeasureCode nchar(3),
            Weight decimal(8, 2),
            DaysToManufacture int,
            ProductLine nchar(2),
            Class nchar(2),
            Style nchar(2),
            ProductSubcategoryID int,
            ProductModelID int,
            SellStartDate datetime,
            SellEndDate datetime,
            DiscontinuedDate datetime,
            rowguid uniqueidentifier,
            ModifiedDate datetime
        )
    """)
    cursor.execute("""
        INSERT INTO Production.Product (ProductID, Name, ProductNumber, MakeFlag, FinishedGoodsFlag, StandardCost, ListPrice, DaysToManufacture, ReorderPoint, SafetyStockLevel, SellStartDate, ModifiedDate) 
        VALUES (1, 'Adjustable Race', 'AR-5381', 0, 0, 0.00, 0.00, 1, 750, 1000, GETDATE(), GETDATE())
    """)

    # Table: Production.ProductSubcategory
    print("Creating Production.ProductSubcategory...")
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Production.ProductSubcategory') AND type in (N'U'))
        CREATE TABLE Production.ProductSubcategory(
            ProductSubcategoryID int PRIMARY KEY,
            ProductCategoryID int,
            Name nvarchar(50),
            rowguid uniqueidentifier,
            ModifiedDate datetime
        )
    """)
    cursor.execute("INSERT INTO Production.ProductSubcategory (ProductSubcategoryID, ProductCategoryID, Name, ModifiedDate) VALUES (1, 1, 'Mountain Bikes', GETDATE())")

    # Table: Sales.SalesOrderHeader
    print("Creating Sales.SalesOrderHeader...")
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Sales.SalesOrderHeader') AND type in (N'U'))
        CREATE TABLE Sales.SalesOrderHeader(
            SalesOrderID int PRIMARY KEY,
            RevisionNumber tinyint,
            OrderDate datetime,
            DueDate datetime,
            ShipDate datetime,
            Status tinyint,
            OnlineOrderFlag bit,
            SalesOrderNumber nvarchar(25),
            PurchaseOrderNumber nvarchar(25),
            AccountNumber nvarchar(15),
            CustomerID int,
            SalesPersonID int,
            TerritoryID int,
            BillToAddressID int,
            ShipToAddressID int,
            ShipMethodID int,
            CreditCardID int,
            CreditCardApprovalCode varchar(15),
            CurrencyRateID int,
            SubTotal money,
            TaxAmt money,
            Freight money,
            TotalDue money,
            Comment nvarchar(128),
            rowguid uniqueidentifier,
            ModifiedDate datetime
        )
    """)
    cursor.execute("""
        INSERT INTO Sales.SalesOrderHeader (SalesOrderID, OrderDate, DueDate, Status, OnlineOrderFlag, SalesOrderNumber, CustomerID, SubTotal, TaxAmt, Freight, TotalDue, ModifiedDate)
        VALUES (43659, GETDATE(), GETDATE(), 5, 0, 'SO43659', 1, 20565.6206, 1971.5149, 616.0984, 23153.2339, GETDATE())
    """)

    # Table: Sales.SalesOrderDetail
    print("Creating Sales.SalesOrderDetail...")
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Sales.SalesOrderDetail') AND type in (N'U'))
        CREATE TABLE Sales.SalesOrderDetail(
            SalesOrderID int,
            SalesOrderDetailID int PRIMARY KEY,
            CarrierTrackingNumber nvarchar(25),
            OrderQty smallint,
            ProductID int,
            SpecialOfferID int,
            UnitPrice money,
            UnitPriceDiscount money,
            LineTotal numeric(38, 6),
            rowguid uniqueidentifier,
            ModifiedDate datetime
        )
    """)
    cursor.execute("""
        INSERT INTO Sales.SalesOrderDetail (SalesOrderID, SalesOrderDetailID, OrderQty, ProductID, UnitPrice, UnitPriceDiscount, LineTotal, ModifiedDate)
        VALUES (43659, 1, 1, 1, 2024.9940, 0.00, 2024.994000, GETDATE())
    """)
    
    # Types (mock)
    try:
        cursor.execute("CREATE TYPE dbo.NameStyle FROM bit")
    except:
        pass

    conn.commit()
    conn.close()
    print("CI database setup complete.")

if __name__ == "__main__":
    setup_database()
