-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_team_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name, 
       results.position,
       results.points,
       11 - results.position AS calculated_points

FROM f1_processed.results
JOIN f1_processed.races ON (results.race_id = races.race_id)
JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
WHERE results.position <= 10
