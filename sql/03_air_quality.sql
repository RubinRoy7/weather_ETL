/*
===============================================================================
Script 03: Air quality extension
===============================================================================
Adds hourly Open-Meteo air-quality readings and a daily fact table:
    staging.air_quality_hourly
    dw.fact_air_quality_daily
    etl.rejected_air_quality_rows
    dw.usp_merge_air_quality
    dw.vw_city_daily_environment

Run after 01_schema.sql and 02_proc_merge_weather.sql.
Safe to re-run.
===============================================================================
*/
USE WeatherDW;
GO

IF OBJECT_ID('staging.air_quality_hourly', 'U') IS NULL
CREATE TABLE staging.air_quality_hourly (
    batch_id   UNIQUEIDENTIFIER NOT NULL,
    city_name  NVARCHAR(100)    NOT NULL,
    obs_time   DATETIME2(0)     NOT NULL,
    pm10       DECIMAL(7,2)     NULL,
    pm2_5      DECIMAL(7,2)     NULL,
    us_aqi     INT              NULL,
    loaded_at  DATETIME2        NOT NULL DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('etl.rejected_air_quality_rows', 'U') IS NULL
CREATE TABLE etl.rejected_air_quality_rows (
    reject_id  INT IDENTITY(1,1) PRIMARY KEY,
    batch_id   UNIQUEIDENTIFIER NOT NULL,
    city_name  NVARCHAR(100)    NULL,
    obs_time   DATETIME2(0)     NULL,
    obs_date   DATE             NULL,
    pm10       DECIMAL(7,2)     NULL,
    pm2_5      DECIMAL(7,2)     NULL,
    us_aqi     INT              NULL,
    reason     NVARCHAR(200)    NOT NULL,
    rejected_at DATETIME2       NOT NULL DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('dw.fact_air_quality_daily', 'U') IS NULL
CREATE TABLE dw.fact_air_quality_daily (
    city_id     INT          NOT NULL REFERENCES dw.dim_city(city_id),
    obs_date    DATE         NOT NULL,
    pm10_avg    DECIMAL(7,2) NULL,
    pm2_5_avg   DECIMAL(7,2) NULL,
    pm2_5_max   DECIMAL(7,2) NULL,
    aqi_max     INT          NULL,
    hours_count INT          NULL,
    created_at  DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
    updated_at  DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_fact_air_quality_daily PRIMARY KEY (city_id, obs_date)
);
GO

IF COL_LENGTH('dw.fact_air_quality_daily', 'hours_count') IS NULL
    ALTER TABLE dw.fact_air_quality_daily ADD hours_count INT NULL;
GO

CREATE OR ALTER PROCEDURE dw.usp_merge_air_quality
    @batch_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @inserted INT = 0, @updated INT = 0;
    DECLARE @merge_actions TABLE (merge_action NVARCHAR(10));

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE dw.fact_air_quality_daily AS tgt
        USING (
            SELECT
                c.city_id,
                CAST(s.obs_time AS DATE) AS obs_date,
                CAST(AVG(s.pm10) AS DECIMAL(7,2)) AS pm10_avg,
                CAST(AVG(s.pm2_5) AS DECIMAL(7,2)) AS pm2_5_avg,
                CAST(MAX(s.pm2_5) AS DECIMAL(7,2)) AS pm2_5_max,
                MAX(s.us_aqi) AS aqi_max,
                COUNT(*) AS hours_count
            FROM staging.air_quality_hourly AS s
            INNER JOIN dw.dim_city AS c
                ON c.city_name = s.city_name
            WHERE s.batch_id = @batch_id
            GROUP BY c.city_id, CAST(s.obs_time AS DATE)
        ) AS src
            ON  tgt.city_id = src.city_id
            AND tgt.obs_date = src.obs_date
        WHEN MATCHED AND EXISTS (
                SELECT src.pm10_avg, src.pm2_5_avg, src.pm2_5_max, src.aqi_max, src.hours_count
                EXCEPT
                SELECT tgt.pm10_avg, tgt.pm2_5_avg, tgt.pm2_5_max, tgt.aqi_max, tgt.hours_count
            )
            THEN UPDATE SET
                tgt.pm10_avg = src.pm10_avg,
                tgt.pm2_5_avg = src.pm2_5_avg,
                tgt.pm2_5_max = src.pm2_5_max,
                tgt.aqi_max = src.aqi_max,
                tgt.hours_count = src.hours_count,
                tgt.updated_at = SYSDATETIME()
        WHEN NOT MATCHED BY TARGET
            THEN INSERT (city_id, obs_date, pm10_avg, pm2_5_avg, pm2_5_max, aqi_max, hours_count)
                 VALUES (src.city_id, src.obs_date, src.pm10_avg, src.pm2_5_avg,
                         src.pm2_5_max, src.aqi_max, src.hours_count)
        OUTPUT $action INTO @merge_actions (merge_action);

        SELECT @inserted = COUNT(*) FROM @merge_actions WHERE merge_action = 'INSERT';
        SELECT @updated = COUNT(*) FROM @merge_actions WHERE merge_action = 'UPDATE';

        COMMIT TRANSACTION;

        INSERT INTO etl.load_log (batch_id, step, status, rows_affected, message)
        VALUES (@batch_id, 'merge_air', 'SUCCESS', @inserted + @updated,
                CONCAT('inserted=', @inserted, ', updated=', @updated));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

        INSERT INTO etl.load_log (batch_id, step, status, rows_affected, message)
        VALUES (@batch_id, 'merge_air', 'FAILED', NULL, LEFT(ERROR_MESSAGE(), 1000));

        THROW;
    END CATCH
END
GO

CREATE OR ALTER VIEW dw.vw_city_daily_environment AS
SELECT
    c.city_name,
    w.obs_date,
    w.temp_max_c,
    w.temp_min_c,
    w.precip_mm,
    a.pm10_avg,
    a.pm2_5_avg,
    a.pm2_5_max,
    a.aqi_max,
    a.hours_count
FROM dw.fact_weather_daily AS w
INNER JOIN dw.dim_city AS c
    ON c.city_id = w.city_id
LEFT JOIN dw.fact_air_quality_daily AS a
    ON  a.city_id = w.city_id
    AND a.obs_date = w.obs_date;
GO
