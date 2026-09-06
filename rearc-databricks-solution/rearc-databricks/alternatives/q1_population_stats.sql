-- Population SD is primary: these six years are the complete requested set.
SELECT COUNT(*) AS year_count, MIN(year) AS start_year, MAX(year) AS end_year,
       AVG(population) AS mean_population,
       STDDEV_POP(population) AS stddev_population,
       STDDEV_SAMP(population) AS stddev_sample
FROM silver_population
WHERE year BETWEEN 2013 AND 2018
