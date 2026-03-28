-- Azure Synapse Analytics Serverless SQL database - script
-- Create schema (run once)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name= 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO


-- EXTERNAL DATA SOURCE
IF EXISTS (
    SELECT * 
    from sys.external_data_sources 
    WHERE name='SilverData'
)
BEGIN
    DROP EXTERNAL DATA SOURCE SilverData;
END;
CREATE EXTERNAL DATA SOURCE SilverData
WITH (
    LOCATION= 'abfss://silver@adwhmk31.dfs.core.windows.net/'
);
GO


-- FACT TABLE (SALES)
CREATE OR ALTER VIEW gold.fact_sales AS
SELECT
    s.OrderDate,
    s.CustomerKey,
    s.ProductKey,
    s.TerritoryKey,
    s.OrderQuantity,
    s.Revenue
FROM
    OPENROWSET(
        BULK 'sales/',
        DATA_SOURCE ='SilverData',
        FORMAT='PARQUET'
    )as s;
GO


-- DIMENSION: CUSTOMERS
CREATE OR ALTER VIEW gold.dim_customers AS
select
    CustomerKey,
    FirstName,
    LastName,
    Gender,
    AnnualIncome,
    HomeOwner
from
    OPENROWSET(
        BULK 'customers/',
        DATA_SOURCE='SilverData',
        FORMAT='PARQUET'
    )as c;
GO


  
  
-- DIMENSION: PRODUCTS (FLATTENED)
CREATE OR ALTER VIEW gold.dim_products AS
SELECT
    ProductKey,
    ProductName,
    ProductPrice
FROM
    OPENROWSET(
        BULK 'products/',
        DATA_SOURCE='SilverData',
        FORMAT='PARQUET'
    )AS p;
GO


  
-- DIMENSION: TERRITORIES
CREATE or ALTER VIEW gold.dim_territories AS
SELECT
    TerritoryKey,
    Region,
    Country,
    Continent
FROM
    OPENROWSET(
        BULK 'territories/',
        DATA_SOURCE = 'SilverData',
        FORMAT = 'PARQUET'
    )as t;
GO


  
-- DIMENSION: CALENDAR
CREATE OR ALTER VIEW gold.dim_calendar AS
SELECT
    Date,
    Year,
    Month,
    MonthName,
    Quarter,
    DayOfWeek
FROM
    OPENROWSET(
        BULK 'calendar/',
        DATA_SOURCE= 'SilverData',
        FORMAT  = 'PARQUET'
    ) AS d;
GO



  
-- FACT: RETURNS (OPTIONAL)
  create or ALTER VIEW gold.fact_returns AS
SELECT
    ReturnDate,
    ProductKey,
    TerritoryKey,
    ReturnQuantity
FROM
    OPENROWSET(
        BULK 'returns/',
        DATA_SOURCE= 'SilverData',
        FORMAT= 'PARQUET'
    )aS r;
GO



-- BUSINESS VIEW (AGGREGATED ANALYTICS)
CREATE OR ALTER VIEW gold.vw_sales_analysis AS
SELECT
    d.Year,
    t.Country,
    SUM(s.Revenue) as TotalRevenue,
    SUM(s.OrderQuantity) as TotalQuantity
FROM gold.fact_sales s
JOIN gold.dim_products p 
    on s.ProductKey = p.ProductKey
JOIN gold.dim_territories t 
    on s.TerritoryKey = t.TerritoryKey
JOIN gold.dim_calendar d 
    on s.OrderDate = d.Date
GROUP by
    d.Year,
    t.Country;
GO
