SELECT o.series_id, o.year, o.period, o.value, p.population
FROM silver_observations o
LEFT JOIN silver_population p ON o.year = p.year
WHERE o.series_id = 'PRS30006032' AND o.period = 'Q01'
