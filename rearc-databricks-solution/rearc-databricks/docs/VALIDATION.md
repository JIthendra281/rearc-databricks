# Validation performed

Date: 6 September 2026. Runtime: local Apache Spark 4.1.0.

## Completed

- 10 standard-library ingestion tests passed, including discovery boundaries,
  unchanged reruns, hash fallback, additions/revisions/deletions, failed-download
  recovery, field-width checks, population pagination guard, contact validation,
  seasonal-header capitalization and repair of a missing parse cache.
- 3 local Spark tests passed. They check SQL/PySpark parity for all three answers,
  annual-only series, Q05 exclusion, ties, partial years, negative values, all-null
  values, metadata-only series, unmatched population, duplicate conflicts, malformed
  values and population integrality.
- Real-source ingestion fetched 12 BLS files and the supplied population JSON.
- Real-source Spark validation produced 77,126 distinct observations, 282 labeled
  series and 11 national population years, with zero quarantined records.
- All three real-source SQL answers matched their PySpark versions.
- Gold reference sizes: Q1 = 1 row, Q2 = 282 rows, Q3 = 39 rows.
- Every Python source parsed successfully and the bundle YAML parsed successfully.

Q1 local reference values:

| Measure | Value |
|---|---:|
| Year count | 6 |
| Mean population | 322,069,808 |
| Population standard deviation | 3,796,119.936934378 |
| Sample standard deviation | 4,158,441.040908095 |

Full reference CSVs, a machine-readable validation summary and source digests are
in `reference-results/`. They are a dated comparison baseline, not hardcoded expected
outputs: provider revisions can legitimately change them.

## Not completed here

- Authenticated Databricks bundle validation or deployment.
- Managed Spark Declarative Pipeline execution and Databricks expectation metrics.
- Databricks screenshots.
- GitHub remote repository creation or publication.

The local environment initially failed to launch Spark Python workers. The tests use
native JSON fixture readers and Spark's native expression engine, the same approach
used by the pipeline's input readers; no Python UDFs or worker-dependent transformations
are present. This resolved local test execution without changing the analytical logic.
