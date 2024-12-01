WITH match_results AS (
    SELECT
        gender,
        date,
        team_1,
        team_2,
        outcome_winner,
        outcome_result,
        outcome_method
    FROM matches
    WHERE date >= '2019-01-01'
        AND date <= '2019-12-31'
        AND outcome_result IS NULL
        AND (outcome_method IS NULL OR outcome_method NOT LIKE '%D/L%')
),
team_matches AS (
    SELECT
        gender,
        team_1 as team,
        CASE WHEN outcome_winner = team_1 THEN 1 ELSE 0 END as won
    FROM match_results

    UNION ALL

    SELECT
        gender,
        team_2 as team,
        CASE WHEN outcome_winner = team_2 THEN 1 ELSE 0 END as won
    FROM match_results
),
team_stats AS (
    SELECT
        gender,
        team,
        COUNT(*) as total_matches,
        SUM(won) as total_wins,
        ROUND(CAST(SUM(won) AS FLOAT) / COUNT(*) * 100, 2) as win_percentage,
        ROW_NUMBER() OVER (PARTITION BY gender ORDER BY CAST(SUM(won) AS FLOAT) / COUNT(*) DESC) as rank
    FROM team_matches
    GROUP BY gender, team
    HAVING COUNT(*) > 1
)
SELECT
    gender,
    team,
    total_matches,
    total_wins,
    win_percentage
FROM team_stats
WHERE rank = 1
ORDER BY gender;
