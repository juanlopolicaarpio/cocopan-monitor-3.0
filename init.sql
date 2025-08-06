-- CocoPan Monitor Database Initialization
-- This script sets up the PostgreSQL database with optimal settings

-- Create the database (if needed)
-- CREATE DATABASE cocopan_monitor;

-- Connect to the database
\c cocopan_monitor;

-- Enable extensions for better performance
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create tables
CREATE TABLE IF NOT EXISTS stores (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL UNIQUE,
    platform VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS status_checks (
    id SERIAL PRIMARY KEY,
    store_id INTEGER REFERENCES stores(id) ON DELETE CASCADE,
    is_online BOOLEAN NOT NULL,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    response_time_ms INTEGER,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS summary_reports (
    id SERIAL PRIMARY KEY,
    total_stores INTEGER NOT NULL,
    online_stores INTEGER NOT NULL,
    offline_stores INTEGER NOT NULL,
    online_percentage REAL NOT NULL,
    report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for optimal performance
CREATE INDEX IF NOT EXISTS idx_stores_platform ON stores(platform);
CREATE INDEX IF NOT EXISTS idx_stores_url ON stores(url);

CREATE INDEX IF NOT EXISTS idx_status_checks_store_id ON status_checks(store_id);
CREATE INDEX IF NOT EXISTS idx_status_checks_checked_at ON status_checks(checked_at);
CREATE INDEX IF NOT EXISTS idx_status_checks_is_online ON status_checks(is_online);
CREATE INDEX IF NOT EXISTS idx_status_checks_store_date ON status_checks(store_id, DATE(checked_at));

CREATE INDEX IF NOT EXISTS idx_summary_reports_time ON summary_reports(report_time);
CREATE INDEX IF NOT EXISTS idx_summary_reports_date ON summary_reports(DATE(report_time));

-- Create partial indexes for better performance on filtered queries
CREATE INDEX IF NOT EXISTS idx_status_checks_online_recent 
ON status_checks(store_id, checked_at) 
WHERE is_online = true AND checked_at > NOW() - INTERVAL '7 days';

CREATE INDEX IF NOT EXISTS idx_status_checks_offline_recent 
ON status_checks(store_id, checked_at) 
WHERE is_online = false AND checked_at > NOW() - INTERVAL '7 days';

-- Create a view for the latest store status (commonly used query)
CREATE OR REPLACE VIEW latest_store_status AS
SELECT 
    s.id,
    s.name,
    s.url,
    s.platform,
    sc.is_online,
    sc.checked_at,
    sc.response_time_ms,
    sc.error_message
FROM stores s
INNER JOIN status_checks sc ON s.id = sc.store_id
INNER JOIN (
    SELECT store_id, MAX(checked_at) as latest_check
    FROM status_checks
    GROUP BY store_id
) latest ON sc.store_id = latest.store_id AND sc.checked_at = latest.latest_check;

-- Create a function to get daily uptime for a store
CREATE OR REPLACE FUNCTION get_store_daily_uptime(store_id_param INTEGER, date_param DATE DEFAULT CURRENT_DATE)
RETURNS RECORD AS $$
DECLARE
    result RECORD;
BEGIN
    SELECT 
        COUNT(*) as total_checks,
        SUM(CASE WHEN is_online THEN 1 ELSE 0 END) as online_checks,
        ROUND(
            (SUM(CASE WHEN is_online THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 
            2
        ) as uptime_percentage
    INTO result
    FROM status_checks 
    WHERE store_id = store_id_param 
    AND DATE(checked_at AT TIME ZONE 'Asia/Manila') = date_param;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Create a function to clean old data (for maintenance)
CREATE OR REPLACE FUNCTION cleanup_old_data(days_to_keep INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- Delete old status checks
    DELETE FROM status_checks 
    WHERE checked_at < NOW() - INTERVAL '1 day' * days_to_keep;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- Delete old summary reports
    DELETE FROM summary_reports 
    WHERE report_time < NOW() - INTERVAL '1 day' * days_to_keep;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions to the cocopan user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cocopan;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cocopan;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO cocopan;

-- Set up automatic statistics collection
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET pg_stat_statements.track = 'all';

-- Insert sample data for testing (optional)
-- This will be populated by the monitoring service
INSERT INTO stores (name, url, platform) VALUES 
('Cocopan - Test Store', 'https://example.com/test', 'test')
ON CONFLICT (url) DO NOTHING;

-- Create a notification function for monitoring
CREATE OR REPLACE FUNCTION notify_store_status_change()
RETURNS TRIGGER AS $$
BEGIN
    -- This can be extended to send notifications
    -- For now, it just logs the change
    RAISE NOTICE 'Store % status changed to %', NEW.store_id, NEW.is_online;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for status changes (disabled by default)
-- CREATE TRIGGER status_change_trigger
--     AFTER INSERT ON status_checks
--     FOR EACH ROW
--     EXECUTE FUNCTION notify_store_status_change();

-- Performance tuning settings (commented out - apply manually if needed)
-- ALTER SYSTEM SET work_mem = '256MB';
-- ALTER SYSTEM SET maintenance_work_mem = '512MB';
-- ALTER SYSTEM SET effective_cache_size = '2GB';
-- ALTER SYSTEM SET random_page_cost = 1.1;

COMMIT;

-- Display setup completion message
\echo 'CocoPan Monitor database initialized successfully!'
\echo 'Tables created: stores, status_checks, summary_reports'
\echo 'Indexes and functions created for optimal performance'