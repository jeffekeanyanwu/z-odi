WITH batter_stats AS (
    SELECT
        i.batter,
        COUNT(*) as balls_faced,
        SUM(i.runs_batter) as runs_scored,
        COUNT(*) - COUNT(CASE
            WHEN i.extras_wides > 0 THEN 1
            END) as actual_balls_faced
    FROM innings i
    JOIN matches m ON i.match_id = m.match_id
    WHERE m.date >= '2019-01-01'
        AND m.date <= '2019-12-31'
        AND i.extras_wides IS NULL
    GROUP BY i.batter
)
SELECT
    batter,
    actual_balls_faced as balls_faced,
    runs_scored,
    ROUND(CAST(runs_scored AS FLOAT) / actual_balls_faced * 100, 2) as strike_rate
FROM batter_stats
ORDER BY strike_rate DESC
LIMIT 10;
