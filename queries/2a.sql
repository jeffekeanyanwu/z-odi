-- win records for every team excluding DLS, ties, no results
WITH match_results AS (
    SELECT
        EXTRACT(YEAR FROM date::DATE) as year,
        gender,
        team_1,
        team_2,
        outcome_winner,
        outcome_result,
        outcome_method
    FROM matches
    WHERE outcome_result IS NULL  -- Exclude ties and no results
        AND (outcome_method IS NULL OR outcome_method NOT LIKE '%D/L%')  -- Exclude DLS matches
),
team_matches AS (
    -- Get matches where team was team_1
    SELECT
        year,
        gender,
        team_1 as team,
        CASE WHEN outcome_winner = team_1 THEN 1 ELSE 0 END as won
    FROM match_results

    UNION ALL

    -- Get matches where team was team_2
    SELECT
        year,
        gender,
        team_2 as team,
        CASE WHEN outcome_winner = team_2 THEN 1 ELSE 0 END as won
    FROM match_results
)
SELECT
    year,
    gender,
    team,
    COUNT(*) as total_matches,
    SUM(won) as total_wins,
    ROUND(CAST(SUM(won) AS FLOAT) / COUNT(*) * 100, 2) as win_percentage
FROM team_matches
GROUP BY year, gender, team
ORDER BY
    year DESC,
    gender,
    win_percentage DESC;