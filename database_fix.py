# Add this to your existing database.py file, or replace the problematic methods

def get_hourly_data(self) -> pd.DataFrame:
    """Get hourly summaries for today"""
    with self.get_connection() as conn:
        if self.db_type == "postgresql":
            query = '''
                SELECT 
                    EXTRACT(HOUR FROM report_time AT TIME ZONE 'Asia/Manila')::integer as hour,
                    ROUND(AVG(online_percentage)::numeric, 0)::integer as online_pct,
                    ROUND(AVG(100 - online_percentage)::numeric, 0)::integer as offline_pct,
                    COUNT(*) as data_points
                FROM summary_reports
                WHERE DATE(report_time AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                GROUP BY EXTRACT(HOUR FROM report_time AT TIME ZONE 'Asia/Manila')
                ORDER BY hour
            '''
        else:
            query = '''
                SELECT 
                    strftime('%H', report_time) as hour,
                    ROUND(AVG(online_percentage), 0) as online_pct,
                    ROUND(AVG(100 - online_percentage), 0) as offline_pct,
                    COUNT(*) as data_points
                FROM summary_reports
                WHERE DATE(report_time, '+8 hours') = DATE('now', '+8 hours')
                GROUP BY strftime('%H', report_time)
                ORDER BY hour
            '''
        return pd.read_sql_query(query, conn)

def get_daily_uptime(self) -> pd.DataFrame:
    """Get daily uptime per store"""
    with self.get_connection() as conn:
        if self.db_type == "postgresql":
            query = '''
                SELECT 
                    s.name,
                    s.platform,
                    COUNT(sc.id) as total_checks,
                    SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) as online_checks,
                    ROUND(
                        (SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id))::numeric, 
                        0
                    )::integer as uptime_percentage
                FROM stores s
                INNER JOIN status_checks sc ON s.id = sc.store_id
                WHERE DATE(sc.checked_at AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                GROUP BY s.id, s.name, s.platform
                ORDER BY uptime_percentage DESC
            '''
        else:
            query = '''
                SELECT 
                    s.name,
                    s.platform,
                    COUNT(sc.id) as total_checks,
                    SUM(CASE WHEN sc.is_online = 1 THEN 1 ELSE 0 END) as online_checks,
                    ROUND(
                        (SUM(CASE WHEN sc.is_online = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id)), 
                        0
                    ) as uptime_percentage
                FROM stores s
                INNER JOIN status_checks sc ON s.id = sc.store_id
                WHERE DATE(sc.checked_at, '+8 hours') = DATE('now', '+8 hours')
                GROUP BY s.id, s.name, s.platform
                ORDER BY uptime_percentage DESC
            '''
        return pd.read_sql_query(query, conn)
