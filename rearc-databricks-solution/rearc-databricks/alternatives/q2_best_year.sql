WITH annual AS (
 SELECT series_id, year, SUM(value) AS summed_value, COUNT(value) AS quarters_available
 FROM silver_observations WHERE period IN ('Q01','Q02','Q03','Q04')
 GROUP BY series_id, year HAVING COUNT(value) > 0
), ranked AS (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY summed_value DESC, year ASC) AS rank
 FROM annual
), universe AS (
 SELECT series_id FROM silver_observations UNION SELECT series_id FROM silver_series
)
SELECT u.series_id, s.series_label, r.year AS best_year, r.summed_value, r.quarters_available,
 CASE WHEN r.year IS NULL THEN 'no_quarterly_observations' ELSE 'ok' END AS status
FROM universe u LEFT JOIN silver_series s ON u.series_id = s.series_id
LEFT JOIN ranked r ON u.series_id = r.series_id AND r.rank = 1
