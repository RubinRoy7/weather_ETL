/*
===============================================================================
Script 01: Create database, schemas and tables
===============================================================================
Run this first in SSMS. Layout:
    staging.*  -> raw rows from the latest API pull (one batch at a time)
    dw.*       -> final dimension and fact tables
    etl.*      -> run log and rejected rows (monitoring)
===============================================================================
*/

IF DB_ID('WeatherDW') IS NULL
    CREATE DATABASE WeatherDW;
GO

USE WeatherDW;
GO

IF SCHEMA_ID('staging') IS NULL EXEC('CREATE SCHEMA staging');
IF SCHEMA_ID('dw')      IS NULL EXEC('CREATE SCHEMA dw');
IF SCHEMA_ID('etl')     IS NULL EXEC('CREATE SCHEMA etl');
GO

-- ---------------------------------------------------------------------------
-- Dimension: cities
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dw.dim_city', 'U') IS NULL
CREATE TABLE dw.dim_city (
    city_id    INT IDENTITY(1,1) PRIMARY KEY,
    city_name  NVARCHAR(100) NOT NULL UNIQUE,
    latitude   DECIMAL(9,6)  NOT NULL,
    longitude  DECIMAL(9,6)  NOT NULL
);
GO

INSERT INTO dw.dim_city (city_name, latitude, longitude)
SELECT v.city_name, v.latitude, v.longitude
FROM (VALUES
    (N'Thiruvananthapuram', 8.5241, 76.9366),
    (N'Kochi',              9.9312, 76.2673),
    (N'Kozhikode',         11.2588, 75.7804),
    (N'Bengaluru',         12.9716, 77.5946),
    (N'Chennai',           13.0827, 80.2707)
) AS v(city_name, latitude, longitude)
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_city c WHERE c.city_name = v.city_name);
GO

-- ---------------------------------------------------------------------------
-- Staging: latest pull, tagged with a batch id
-- ---------------------------------------------------------------------------
IF OBJECT_ID('staging.weather_daily', 'U') IS NULL
CREATE TABLE staging.weather_daily (
    batch_id    UNIQUEIDENTIFIER NOT NULL,
    city_name   NVARCHAR(100)    NOT NULL,
    obs_date    DATE             NOT NULL,
    temp_max_c  DECIMAL(5,2)     NULL,
    temp_min_c  DECIMAL(5,2)     NULL,
    precip_mm   DECIMAL(7,2)     NULL,
    loaded_at   DATETIME2        NOT NULL DEFAULT SYSDATETIME()
);
GO

-- ---------------------------------------------------------------------------
-- Fact: one row per city per day
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dw.fact_weather_daily', 'U') IS NULL
CREATE TABLE dw.fact_weather_daily (
    city_id     INT          NOT NULL REFERENCES dw.dim_city(city_id),
    obs_date    DATE         NOT NULL,
    temp_max_c  DECIMAL(5,2) NULL,
    temp_min_c  DECIMAL(5,2) NULL,
    precip_mm   DECIMAL(7,2) NULL,
    created_at  DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
    updated_at  DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_fact_weather_daily PRIMARY KEY (city_id, obs_date)
);
GO

-- ---------------------------------------------------------------------------
-- ETL monitoring
-- ---------------------------------------------------------------------------
IF OBJECT_ID('etl.load_log', 'U') IS NULL
CREATE TABLE etl.load_log (
    log_id         INT IDENTITY(1,1) PRIMARY KEY,
    batch_id       UNIQUEIDENTIFIER NOT NULL,
    step           NVARCHAR(50)     NOT NULL,   -- extract / validate / stage / merge
    status         NVARCHAR(20)     NOT NULL,   -- SUCCESS / FAILED
    rows_affected  INT              NULL,
    message        NVARCHAR(1000)   NULL,
    logged_at      DATETIME2        NOT NULL DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('etl.rejected_rows', 'U') IS NULL
CREATE TABLE etl.rejected_rows (
    reject_id   INT IDENTITY(1,1) PRIMARY KEY,
    batch_id    UNIQUEIDENTIFIER NOT NULL,
    city_name   NVARCHAR(100)    NULL,
    obs_date    DATE             NULL,
    temp_max_c  DECIMAL(5,2)     NULL,
    temp_min_c  DECIMAL(5,2)     NULL,
    precip_mm   DECIMAL(7,2)     NULL,
    reason      NVARCHAR(200)    NOT NULL,
    rejected_at DATETIME2        NOT NULL DEFAULT SYSDATETIME()
);
GO
