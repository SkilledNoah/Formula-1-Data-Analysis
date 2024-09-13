-- Databricks notebook source
SELECT team_name,
       SUM(calculated_points) AS total_points,
       COUNT(*) AS total_races,
       (total_points / COUNT(*)) AS avg_points_per_race
FROM f1_presentation.calculated_team_results
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points_per_race DESC;

-- COMMAND ----------

SELECT team_name,
       SUM(calculated_points) AS total_points,
       COUNT(*) AS total_races,
       (total_points / COUNT(*)) AS avg_points_per_race
FROM f1_presentation.calculated_team_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points_per_race DESC;

-- COMMAND ----------

SELECT team_name,
       SUM(calculated_points) AS total_points,
       COUNT(*) AS total_races,
       (total_points / COUNT(*)) AS avg_points_per_race
FROM f1_presentation.calculated_team_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points_per_race DESC;
