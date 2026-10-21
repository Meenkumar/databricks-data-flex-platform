-- Stored procedure to refresh materialized views
-- Note: Databricks uses REFRESH MATERIALIZED VIEW syntax

CREATE OR REPLACE PROCEDURE main.config.sp_refresh_materialized_views()
LANGUAGE SQL
AS
BEGIN
    -- Refresh all materialized views in sequence

    DECLARE view_count INT DEFAULT 0;

    -- Add your materialized views here
    -- Example:
    -- REFRESH MATERIALIZED VIEW main.gold.mv_member_summary;
    -- SET view_count = view_count + 1;

    -- Log completion
    INSERT INTO main.audit.procedure_log (
        procedure_name,
        execution_time,
        status,
        message
    ) VALUES (
        'sp_refresh_materialized_views',
        CURRENT_TIMESTAMP(),
        'SUCCESS',
        CONCAT('Refreshed ', CAST(view_count AS STRING), ' materialized views')
    );

END;

COMMENT ON PROCEDURE main.config.sp_refresh_materialized_views IS 'Refreshes all materialized views in the data platform';
