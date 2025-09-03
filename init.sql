-- CocoPan Monitor Database Initialization (PRODUCTION READY)
-- This script sets up the PostgreSQL database with optimal settings
-- FIXED: No test stores - production ready

-- Connect to the database
\c cocopan_monitor;

-- Enable extensions for better performance
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create tables with proper constraints and indexes
CREATE TABLE IF NOT EXISTS stores (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL UNIQUE,
    platform VARCHAR(50) NOT NULL CHECK (platform IN ('foodpanda', 'grabfood', 'unknown')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    name_override VARCHAR(255),
    last_manual_check TIMESTAMP
);

CREATE TABLE IF NOT EXISTS status_checks (
    id SERIAL PRIMARY KEY,
    store_id INTEGER REFERENCES stores(id) ON DELETE CASCADE,
    is_online BOOLEAN NOT NULL,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    response_time_ms INTEGER CHECK (response_time_ms >= 0),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS summary_reports (
    id SERIAL PRIMARY KEY,
    total_stores INTEGER NOT NULL CHECK (total_stores >= 0),
    online_stores INTEGER NOT NULL CHECK (online_stores >= 0),
    offline_stores INTEGER NOT NULL CHECK (offline_stores >= 0),
    online_percentage REAL NOT NULL CHECK (online_percentage >= 0 AND online_percentage <= 100),
    report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- NEW: Hourly snapshot tables for production analytics
CREATE TABLE IF NOT EXISTS store_status_hourly (
    effective_at  timestamptz NOT NULL,
    platform      text        NOT NULL,
    store_id      integer     NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
    status        text        NOT NULL CHECK (status IN ('ONLINE', 'OFFLINE', 'BLOCKED', 'ERROR', 'UNKNOWN')),
    confidence    real        NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    response_ms   integer     NULL CHECK (response_ms >= 0),
    evidence      text        NULL,
    probe_time    timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_id        uuid        NOT NULL,
    PRIMARY KEY (platform, store_id, effective_at)
);

CREATE TABLE IF NOT EXISTS status_summary_hourly (
    effective_at  timestamptz PRIMARY KEY,
    total         integer NOT NULL CHECK (total >= 0),
    online        integer NOT NULL CHECK (online >= 0),
    offline       integer NOT NULL CHECK (offline >= 0),
    blocked       integer NOT NULL CHECK (blocked >= 0),
    errors        integer NOT NULL CHECK (errors >= 0),
    unknown       integer NOT NULL CHECK (unknown >= 0),
    last_probe_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT summary_totals_match CHECK (total = online + offline + blocked + errors + unknown)
);

-- Create indexes for optimal performance
CREATE INDEX IF NOT EXISTS idx_stores_platform ON stores(platform);
CREATE INDEX IF NOT EXISTS idx_stores_url ON stores(url);
CREATE INDEX IF NOT EXISTS idx_stores_name ON stores(name);

CREATE INDEX IF NOT EXISTS idx_status_checks_store_id ON status_checks(store_id);
CREATE INDEX IF NOT EXISTS idx_status_checks_checked_at ON status_checks(checked_at);
CREATE INDEX IF NOT EXISTS idx_status_checks_is_online ON status_checks(is_online);
CREATE INDEX IF NOT EXISTS idx_status_checks_store_date ON status_checks(store_id, DATE(checked_at));
CREATE INDEX IF NOT EXISTS idx_status_checks_latest ON status_checks(store_id, checked_at DESC);

CREATE INDEX IF NOT EXISTS idx_summary_reports_time ON summary_reports(report_time);
CREATE INDEX IF NOT EXISTS idx_summary_reports_date ON summary_reports(DATE(report_time));

-- Indexes for hourly tables
CREATE INDEX IF NOT EXISTS idx_store_status_hourly_effective_at ON store_status_hourly(effective_at);
CREATE INDEX IF NOT EXISTS idx_store_status_hourly_platform ON store_status_hourly(platform);
CREATE INDEX IF NOT EXISTS idx_store_status_hourly_status ON store_status_hourly(status);
CREATE INDEX IF NOT EXISTS idx_store_status_hourly_store_time ON store_status_hourly(store_id, effective_at DESC);

CREATE INDEX IF NOT EXISTS idx_status_summary_hourly_date ON status_summary_hourly(DATE(effective_at));

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
    COALESCE(s.name_override, s.name) as display_name,
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

-- Create a view for stores needing attention (admin dashboard)
CREATE OR REPLACE VIEW stores_needing_attention AS
SELECT 
    s.id,
    COALESCE(s.name_override, s.name) as name,
    s.url,
    s.platform,
    sc.is_online,
    sc.checked_at,
    sc.response_time_ms,
    sc.error_message,
    CASE 
        WHEN sc.error_message LIKE '[BLOCKED]%' THEN 'BLOCKED'
        WHEN sc.error_message LIKE '[UNKNOWN]%' THEN 'UNKNOWN'  
        WHEN sc.error_message LIKE '[ERROR]%' THEN 'ERROR'
        WHEN sc.is_online = false AND sc.error_message IS NOT NULL THEN 'NEEDS_CHECK'
        ELSE 'OK'
    END as problem_status
FROM stores s
INNER JOIN (
    SELECT DISTINCT ON (store_id) 
        store_id, is_online, checked_at, response_time_ms, error_message
    FROM status_checks 
    ORDER BY store_id, checked_at DESC
) sc ON s.id = sc.store_id
WHERE (
    sc.error_message LIKE '[BLOCKED]%' OR 
    sc.error_message LIKE '[UNKNOWN]%' OR 
    sc.error_message LIKE '[ERROR]%'
)
AND DATE(sc.checked_at) = CURRENT_DATE
ORDER BY sc.checked_at DESC;

-- Create a function to get daily uptime for a store
CREATE OR REPLACE FUNCTION get_store_daily_uptime(store_id_param INTEGER, date_param DATE DEFAULT CURRENT_DATE)
RETURNS TABLE(
    total_checks BIGINT,
    online_checks BIGINT,
    uptime_percentage NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_checks,
        SUM(CASE WHEN is_online THEN 1 ELSE 0 END) as online_checks,
        ROUND(
            (SUM(CASE WHEN is_online THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 
            2
        ) as uptime_percentage
    FROM status_checks 
    WHERE status_checks.store_id = store_id_param 
    AND DATE(checked_at AT TIME ZONE 'Asia/Manila') = date_param;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get platform statistics
CREATE OR REPLACE FUNCTION get_platform_statistics(date_param DATE DEFAULT CURRENT_DATE)
RETURNS TABLE(
    platform TEXT,
    total_stores BIGINT,
    online_stores BIGINT,
    offline_stores BIGINT,
    blocked_stores BIGINT,
    error_stores BIGINT,
    unknown_stores BIGINT,
    uptime_percentage NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH latest_status AS (
        SELECT 
            s.platform,
            sc.is_online,
            sc.error_message,
            ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY sc.checked_at DESC) as rn
        FROM stores s
        INNER JOIN status_checks sc ON s.id = sc.store_id
        WHERE DATE(sc.checked_at AT TIME ZONE 'Asia/Manila') = date_param
    ),
    status_counts AS (
        SELECT 
            platform,
            COUNT(*) as total_stores,
            SUM(CASE WHEN is_online = true THEN 1 ELSE 0 END) as online_stores,
            SUM(CASE WHEN is_online = false AND error_message NOT LIKE '[BLOCKED]%' 
                        AND error_message NOT LIKE '[UNKNOWN]%' 
                        AND error_message NOT LIKE '[ERROR]%' THEN 1 ELSE 0 END) as offline_stores,
            SUM(CASE WHEN error_message LIKE '[BLOCKED]%' THEN 1 ELSE 0 END) as blocked_stores,
            SUM(CASE WHEN error_message LIKE '[ERROR]%' THEN 1 ELSE 0 END) as error_stores,
            SUM(CASE WHEN error_message LIKE '[UNKNOWN]%' THEN 1 ELSE 0 END) as unknown_stores
        FROM latest_status
        WHERE rn = 1
        GROUP BY platform
    )
    SELECT 
        sc.platform,
        sc.total_stores,
        sc.online_stores,
        sc.offline_stores,
        sc.blocked_stores,
        sc.error_stores,
        sc.unknown_stores,
        ROUND((sc.online_stores * 100.0 / GREATEST(sc.total_stores, 1)), 2) as uptime_percentage
    FROM status_counts sc
    ORDER BY sc.platform;
END;
$$ LANGUAGE plpgsql;

-- Create a function to clean old data (for maintenance)
CREATE OR REPLACE FUNCTION cleanup_old_data(days_to_keep INTEGER DEFAULT 90)
RETURNS TABLE(
    deleted_status_checks INTEGER,
    deleted_summary_reports INTEGER,
    deleted_hourly_status INTEGER,
    deleted_hourly_summaries INTEGER
) AS $$
DECLARE
    deleted_count INTEGER;
    total_status_checks INTEGER := 0;
    total_summary_reports INTEGER := 0;
    total_hourly_status INTEGER := 0;
    total_hourly_summaries INTEGER := 0;
BEGIN
    -- Delete old status checks
    DELETE FROM status_checks 
    WHERE checked_at < NOW() - INTERVAL '1 day' * days_to_keep;
    GET DIAGNOSTICS total_status_checks = ROW_COUNT;
    
    -- Delete old summary reports
    DELETE FROM summary_reports 
    WHERE report_time < NOW() - INTERVAL '1 day' * days_to_keep;
    GET DIAGNOSTICS total_summary_reports = ROW_COUNT;
    
    -- Delete old hourly status records
    DELETE FROM store_status_hourly 
    WHERE effective_at < NOW() - INTERVAL '1 day' * days_to_keep;
    GET DIAGNOSTICS total_hourly_status = ROW_COUNT;
    
    -- Delete old hourly summaries
    DELETE FROM status_summary_hourly 
    WHERE effective_at < NOW() - INTERVAL '1 day' * days_to_keep;
    GET DIAGNOSTICS total_hourly_summaries = ROW_COUNT;
    
    RETURN QUERY SELECT total_status_checks, total_summary_reports, total_hourly_status, total_hourly_summaries;
END;
$$ LANGUAGE plpgsql;

-- Create a function to validate database consistency
CREATE OR REPLACE FUNCTION validate_database_consistency()
RETURNS TABLE(
    check_name TEXT,
    status TEXT,
    details TEXT
) AS $$
BEGIN
    -- Check 1: Verify we have exactly 66 stores
    RETURN QUERY
    SELECT 
        'Store Count'::TEXT,
        CASE WHEN COUNT(*) = 66 THEN '✅ PASS' ELSE '❌ FAIL' END::TEXT,
        ('Expected: 66, Actual: ' || COUNT(*))::TEXT
    FROM stores;
    
    -- Check 2: Verify platform distribution
    RETURN QUERY
    SELECT 
        'Platform Distribution'::TEXT,
        CASE WHEN COUNT(DISTINCT platform) >= 2 AND COUNT(*) = 66 THEN '✅ PASS' ELSE '⚠️ WARN' END::TEXT,
        ('Platforms: ' || STRING_AGG(platform || '=' || cnt::TEXT, ', '))::TEXT
    FROM (
        SELECT platform, COUNT(*) as cnt 
        FROM stores 
        GROUP BY platform
    ) t;
    
    -- Check 3: Check for generic store names
    RETURN QUERY
    SELECT 
        'Generic Store Names'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END::TEXT,
        ('Generic names found: ' || COUNT(*))::TEXT
    FROM stores 
    WHERE name LIKE '%Store%' OR name = 'stores' OR name LIKE 'Cocopan Store (%';
    
    -- Check 4: Check for duplicate URLs
    RETURN QUERY
    SELECT 
        'Duplicate URLs'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END::TEXT,
        ('Duplicates found: ' || COUNT(*))::TEXT
    FROM (
        SELECT url, COUNT(*) as cnt 
        FROM stores 
        GROUP BY url 
        HAVING COUNT(*) > 1
    ) t;
    
    -- Check 5: Recent status checks
    RETURN QUERY
    SELECT 
        'Recent Status Checks'::TEXT,
        CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '⚠️ WARN' END::TEXT,
        ('Status checks in last 24h: ' || COUNT(*))::TEXT
    FROM status_checks 
    WHERE checked_at > NOW() - INTERVAL '24 hours';
    
END;
$$ LANGUAGE plpgsql;

-- Grant permissions to the cocopan user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cocopan;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cocopan;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO cocopan;

-- Set up automatic statistics collection
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET pg_stat_statements.track = 'all';

-- PRODUCTION READY: No test data inserted
-- The monitoring service will populate the stores table from branch_urls.json
-- This ensures exactly 66 production stores with proper names

-- Create a notification function for monitoring (optional)
CREATE OR REPLACE FUNCTION notify_store_status_change()
RETURNS TRIGGER AS $$
BEGIN
    -- This can be extended to send notifications
    -- For now, it just logs the change
    IF NEW.is_online != OLD.is_online THEN
        RAISE NOTICE 'Store % status changed from % to %', NEW.store_id, OLD.is_online, NEW.is_online;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Performance tuning settings recommendations (apply manually if needed)
-- These are commented out as they require superuser privileges
-- and should be tuned based on your specific hardware

-- For optimal performance on a 2GB+ RAM system:
-- ALTER SYSTEM SET shared_buffers = '512MB';
-- ALTER SYSTEM SET effective_cache_size = '1536MB';
-- ALTER SYSTEM SET maintenance_work_mem = '128MB';
-- ALTER SYSTEM SET work_mem = '8MB';
-- ALTER SYSTEM SET random_page_cost = 1.1;
-- ALTER SYSTEM SET effective_io_concurrency = 200;

-- For SSD storage:
-- ALTER SYSTEM SET seq_page_cost = 1.0;
-- ALTER SYSTEM SET random_page_cost = 1.0;

-- Connection and logging:
-- ALTER SYSTEM SET max_connections = 100;
-- ALTER SYSTEM SET log_statement = 'mod';
-- ALTER SYSTEM SET log_min_duration_statement = 1000;

COMMIT;

-- Display setup completion message
\echo '════════════════════════════════════════════════════════════════════════'
\echo '🎉 CocoPan Monitor database initialized successfully!'
\echo '✅ Production-ready schema created:'
\echo '   • Tables: stores, status_checks, summary_reports'
\echo '   • Hourly Tables: store_status_hourly, status_summary_hourly'
\echo '   • Views: latest_store_status, stores_needing_attention'
\echo '   • Functions: Platform stats, uptime calculation, cleanup, validation'
\echo '   • Indexes: Optimized for dashboard queries and reporting'
\echo '⚠️  NO test data inserted - monitor service will create exactly 66 stores'
\echo '🔧 Run validate_database_consistency() to check system health'
\echo '════════════════════════════════════════════════════════════════════════'