/*
===============================================================================
Stored Procedure: dw.usp_merge_weather  (staging -> fact, incremental upsert)
===============================================================================
Purpose:
    Upserts the rows of one batch from staging.weather_daily into
    dw.fact_weather_daily using MERGE:
      - new city/date       -> INSERT
      - changed values      -> UPDATE (forecasts get revised, so this happens)
      - unchanged rows      -> left alone
    Re-running the same batch is safe (idempotent).
    Writes a row to etl.load_log. On error: rolls back, logs, re-throws.

Usage:
    EXEC dw.usp_merge_weather @batch_id = '<guid>';
===============================================================================
*/
USE WeatherDW;
GO

CREATE OR ALTER PROCEDURE dw.usp_merge_weather
    @batch_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @inserted INT = 0, @updated INT = 0;
    DECLARE @merge_actions TABLE (merge_action NVARCHAR(10));

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE dw.fact_weather_daily AS tgt
        USING (
            SELECT c.city_id, s.obs_date, s.temp_max_c, s.temp_min_c, s.precip_mm
            FROM staging.weather_daily AS s
            INNER JOIN dw.dim_city AS c
                ON c.city_name = s.city_name
            WHERE s.batch_id = @batch_id
        ) AS src
            ON  tgt.city_id  = src.city_id
            AND tgt.obs_date = src.obs_date
        WHEN MATCHED AND EXISTS (   -- NULL-safe "values differ" check
                SELECT src.temp_max_c, src.temp_min_c, src.precip_mm
                EXCEPT
                SELECT tgt.temp_max_c, tgt.temp_min_c, tgt.precip_mm
            )
            THEN UPDATE SET
                tgt.temp_max_c = src.temp_max_c,
                tgt.temp_min_c = src.temp_min_c,
                tgt.precip_mm  = src.precip_mm,
                tgt.updated_at = SYSDATETIME()
        WHEN NOT MATCHED BY TARGET
            THEN INSERT (city_id, obs_date, temp_max_c, temp_min_c, precip_mm)
                 VALUES (src.city_id, src.obs_date, src.temp_max_c, src.temp_min_c, src.precip_mm)
        OUTPUT $action INTO @merge_actions (merge_action);

        SELECT @inserted = COUNT(*) FROM @merge_actions WHERE merge_action = 'INSERT';
        SELECT @updated  = COUNT(*) FROM @merge_actions WHERE merge_action = 'UPDATE';

        COMMIT TRANSACTION;

        INSERT INTO etl.load_log (batch_id, step, status, rows_affected, message)
        VALUES (@batch_id, 'merge', 'SUCCESS', @inserted + @updated,
                CONCAT('inserted=', @inserted, ', updated=', @updated));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

        INSERT INTO etl.load_log (batch_id, step, status, rows_affected, message)
        VALUES (@batch_id, 'merge', 'FAILED', NULL, LEFT(ERROR_MESSAGE(), 1000));

        THROW;   -- do not swallow the error: the caller must see the failure
    END CATCH
END
GO
