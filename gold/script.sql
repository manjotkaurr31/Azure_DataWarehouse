-- Azure Synapse Analytics Serverless SQL database - script

-- 1. SCHEMA
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO


    
    -- 2. EXTERNAL DATA SOURCES
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'SilverData')
    CREATE EXTERNAL DATA SOURCE SilverData
    WITH (
        LOCATION = 'abfss://silver@adwhmk31.dfs.core.windows.net/'
    );
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'GoldData')
    CREATE EXTERNAL DATA SOURCE GoldData
    WITH (
        LOCATION = 'abfss://gold@adwhmk31.dfs.core.windows.net/'
    );
GO


    
    -- 3. FILE FORMAT
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFormat')
    CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH (
        FORMAT_TYPE = PARQUET
    );
GO


    
    
    -- 4. DROP EXISTING EXTERNAL TABLES
IF OBJECT_ID('gold.fact_sales')      IS NOT NULL DROP EXTERNAL TABLE gold.fact_sales;
GO
IF OBJECT_ID('gold.fact_returns')    IS NOT NULL DROP EXTERNAL TABLE gold.fact_returns;
GO
IF OBJECT_ID('gold.dim_customers')   IS NOT NULL DROP EXTERNAL TABLE gold.dim_customers;
GO
IF OBJECT_ID('gold.dim_products')    IS NOT NULL DROP EXTERNAL TABLE gold.dim_products;
GO
IF OBJECT_ID('gold.dim_territories') IS NOT NULL DROP EXTERNAL TABLE gold.dim_territories;
GO
IF OBJECT_ID('gold.dim_calendar')    IS NOT NULL DROP EXTERNAL TABLE gold.dim_calendar;
GO


    
    
    -- 5. FACT: SALES
CREATE EXTERNAL TABLE gold.fact_sales
WITH (
    LOCATION    = 'fact_sales/',
    DATA_SOURCE = GoldData,
    FILE_FORMAT = ParquetFormat) AS
SELECT
    CAST(s.OrderDate AS DATE) AS OrderDate,
    s.CustomerKey,
    s.ProductKey,
    s.TerritoryKey,
    s.OrderQuantity,
    s.Revenue
FROM OPENROWSET(
    BULK        'sales/',
    DATA_SOURCE = 'SilverData',
    FORMAT      = 'PARQUET'
) AS s;
GO


    
    -- 6. FACT: RETURNS
CREATE EXTERNAL TABLE gold.fact_returns
WITH (
    LOCATION    = 'fact_returns/',
    DATA_SOURCE = GoldData,
    FILE_FORMAT = ParquetFormat
)
AS
SELECT
    CAST(r.ReturnDate AS DATE) AS ReturnDate,
    r.ProductKey,
    r.TerritoryKey,
    r.ReturnQuantity
FROM OPENROWSET(
    BULK        'returns/',
    DATA_SOURCE = 'SilverData',
    FORMAT      = 'PARQUET'
) AS r;
GO


    
    
-- 7. DIMENSION: CUSTOMERS
CREATE EXTERNAL TABLE gold.dim_customers
WITH (
    LOCATION    = 'dim_customers/',
    DATA_SOURCE = GoldData,
    FILE_FORMAT = ParquetFormat) AS
SELECT
    c.CustomerKey,
    c.FirstName,
    c.LastName,
    c.Gender,
    c.AnnualIncome,
    c.HomeOwner
FROM OPENROWSET(
    BULK        'customers/',
    DATA_SOURCE = 'SilverData',
    FORMAT      = 'PARQUET') AS c;
GO


    
    
-- 8. DIMENSION: PRODUCTS
CREATE EXTERNAL TABLE gold.dim_products
WITH (
    LOCATION    = 'dim_products/',
    DATA_SOURCE = GoldData,
    FILE_FORMAT = ParquetFormat)AS
SELECT
    p.ProductKey,
    p.ProductName,
    p.ProductPrice
FROM OPENROWSET(
    BULK        'products/',
    DATA_SOURCE = 'SilverData',
    FORMAT      = 'PARQUET') AS p;
GO

-- =========================================
-- 9. DIMENSION: TERRITORIES
-- =========================================
CREATE EXTERNAL TABLE gold.dim_territories
WITH (
    LOCATION    = 'dim_territories/',
    DATA_SOURCE = GoldData,
    FILE_FORMAT = ParquetFormat)AS
SELECT
    t.TerritoryKey,
    t.Region,
    t.Country,
    t.Continent
FROM OPENROWSET(
    BULK        'territories/',
    DATA_SOURCE = 'SilverData',
    FORMAT      = 'PARQUET') AS t;
GO


    
    
-- 10. DIMENSION: CALENDAR
CREATE EXTERNAL TABLE gold.dim_calendar
WITH (
    LOCATION    = 'dim_calendar/',
    DATA_SOURCE = GoldData,
    FILE_FORMAT = ParquetFormat)AS
SELECT
    d.Date,
    d.Year,
    d.Month,
    d.MonthName,
    d.Quarter,
    d.DayOfWeek
FROM OPENROWSET(
    BULK        'calendar/',
    DATA_SOURCE = 'SilverData',
    FORMAT      = 'PARQUET') AS d;
GO
