-- Calculate batsmen strike rates for the year 2019
WITH batter_stats AS (
    SELECT
        i.batter,
        COUNT(*) as balls_faced,
        SUM(i.runs_batter) as runs_scored,
        COUNT(*) - COUNT(CASE
            WHEN i.extras_wides > 0 THEN 1  -- Subtract wides from the total count to get actual balls faced
            END) as actual_balls_faced  -- Calculate actual balls faced by excluding wides
    FROM innings i
    JOIN matches m ON i.match_id = m.match_id
    WHERE m.date >= '2019-01-01'
        AND m.date <= '2019-12-31'
        AND i.extras_wides IS NULL  -- Ensure wides are excluded from the count of balls faced
    GROUP BY i.batter  -- Group results by batter to calculate stats per individual
)
SELECT
    batter,
    actual_balls_faced as balls_faced,
    runs_scored,
    ROUND(CAST(runs_scored AS FLOAT) / actual_balls_faced * 100, 2) as strike_rate
FROM batter_stats
ORDER BY strike_rate DESC
LIMIT 10;
