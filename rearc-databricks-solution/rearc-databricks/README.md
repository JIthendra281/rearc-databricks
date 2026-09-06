# Rearc Data Quest — Databricks solution

A rerunnable BLS + Data USA ingestion and Spark Declarative Pipeline with Bronze,
Silver and Gold Delta tables. PySpark feeds Gold; all three questions also have
standalone Spark SQL implementations and parity checks.

## Start here

1. Read **docs/RUNBOOK.md** for the Databricks UI route and optional bundle deployment.
2. Run **notebooks/01_ingest.py** with your real contact email.
3. Configure and run **pipelines/quest_pipeline.py** as a serverless declarative pipeline.
4. Run **notebooks/02_validate.py** and capture the required screenshots.
5. Review **PROCESS.md**, especially the AI disclosure and deployment status, before submitting.

Databricks execution and screenshots are the owner's remaining steps. Local test
results are not evidence of a successful managed Databricks pipeline run.

## Files

| Path | Purpose |
|---|---|
| `rearc_quest/ingest.py` | HTTP retries, dynamic listing, raw storage, immutable caches and snapshot commits |
| `rearc_quest/transforms.py` | Typed Silver transformations and three PySpark answers |
| `pipelines/quest_pipeline.py` | Managed Bronze/Silver/Gold definitions and expectations |
| `alternatives/*.sql` | Equivalent Spark SQL answers, executable against Silver |
| `notebooks/01_ingest.py` | Volume setup and ingestion |
| `notebooks/02_validate.py` | Gold comparison, quality gates and output displays |
| `databricks.yml` | Optional bundle: serverless pipeline plus ingest → pipeline → validate job |
| `tests/` | Ingestion failures/reruns and Spark analytical edge cases |
| `docs/RUNBOOK.md` | Run, troubleshoot, publish and screenshot instructions |
| `docs/INTERVIEW_GUIDE.md` | Code explanation and design questions |

## Verified locally

13 tests passed (10 ingestion, 3 Spark). The live-source run and all three SQL/PySpark
comparisons passed; see **docs/VALIDATION.md** and **reference-results/**.

## Local tests

Python 3.11+ and Java 17+:

```bash
python -m pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
```

Optional real-provider verification (external HTTP requests):

```bash
python -m rearc_quest.ingest --root ./local-data --contact YOUR_REAL_EMAIL
python -m rearc_quest.verify_local --root ./local-data --output ./local-results
```

Do not commit raw data, credentials or local configuration. Raw provider files
belong in the Volume. Use a private/shareable solution repository and give Rearc
reviewer access; the assignment says not to redistribute the quest.

## Analytical conventions

- Q1: 2013–2018 inclusive; require all six distinct US years. Report population
  standard deviation as primary, with sample standard deviation alongside it.
- Q2: sum Q01–Q04 only. Include partial years as the question specifies all
  available quarters; show `quarters_available`. Earliest year wins ties.
  Annual-only and metadata-only series remain visible with null best year.
- Q3: BLS is the left side; missing population stays null.
- Identical Current/AllData overlap counts once. Conflicting values fail the
  pipeline instead of silently selecting a winner. Revisions across snapshots
  replace the previous active file version.
