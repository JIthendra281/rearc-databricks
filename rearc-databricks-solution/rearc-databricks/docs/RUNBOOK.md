# Run and submit

## Databricks UI route

The project does not need an AWS access key, account identifier or PAT in its code.
Use your existing Free Edition workspace and normal browser authentication.

1. Download and unzip this project. Upload/import the entire folder into Workspace,
   preserving `notebooks`, `pipelines`, `rearc_quest` and `alternatives` as siblings.
   Alternatively, create a Databricks Git folder from your shareable GitHub repo.
   Python files in `notebooks/` carry Databricks source-notebook headers; import them
   as notebooks. Keep the files in `rearc_quest/` and `pipelines/` as Python files.
2. Open `notebooks/01_ingest`. Select serverless notebook compute. Set widgets:
   `catalog=workspace`, `schema=rearc_quest`, `volume=raw`, and `contact_email` to your
   real email. If `workspace` is not an existing writable catalog, choose one in Catalog
   Explorer. Do not create a paid workspace or provision classic clusters for this quest.
3. Run all cells. It creates the schema/Volume and prints the result and raw root:
   `/Volumes/workspace/rearc_quest/raw/rearc`. The first successful run must report
   `changed=True`. A later unchanged pull reports `changed=False`.
4. Under Jobs & Pipelines, create a Spark Declarative Pipeline (the UI may label
   this Lakeflow). Select serverless, triggered execution, catalog `workspace`,
   schema `rearc_quest`. Add `pipelines/quest_pipeline.py` as its Python source.
   Set configuration `quest.raw_root` to the path printed by the ingestion notebook.
   Keep the project folder hierarchy intact so imports resolve.
5. Run the pipeline. Review the graph and dataset Data quality panels. All three Gold
   datasets must succeed. Failure of one expectation can leave other tables refreshed;
   do not treat a partially failed update as an accepted publication.
6. Run `notebooks/02_validate` using the same catalog/schema. Every assertion must pass.
   Its displays show the output for all three analytical questions.
7. Run ingestion again to demonstrate idempotency. If `changed=False`, no new raw
   objects, parse-cache objects or manifest are written. Skip a pipeline update if
   no source changed and the current pipeline/code is already validated. Rerun it when
   code changes even if ingestion is unchanged.

Notebook imports assume current working directory is the notebook's directory,
which is the supported modern Databricks behavior. If you moved a notebook outside
`notebooks/`, restore the hierarchy or set `PROJECT` explicitly to the project root.

## Optional bundle route

Install the current Databricks CLI using its official instructions, then authenticate
with your exact workspace URL copied from the browser (the numeric workspace ID alone
is not a hostname). Never paste tokens into chat or commit them.

```bash
databricks auth login --host YOUR_WORKSPACE_URL --profile rearc
# Run these from the directory containing databricks.yml:
databricks bundle validate --profile rearc --var contact_email=YOUR_REAL_EMAIL
databricks bundle deploy --profile rearc --var contact_email=YOUR_REAL_EMAIL
databricks bundle run quest_workflow --profile rearc --var contact_email=YOUR_REAL_EMAIL
```

Override `catalog` or `schema` with additional `--var` values if needed. The bundle's
job runs ingestion, pipeline and validation sequentially; it has one concurrent run.
The pipeline task intentionally runs after every successful job ingestion, supporting
code-only changes and recovery from a prior failed pipeline run. Dataset refreshes
may recompute even when source ingestion is unchanged.

The YAML has been prepared against the official bundle configuration reference.
Authenticated `bundle validate`, deployment and workspace execution remain required;
they have not been represented as completed. Use the UI route if your Free Edition
workspace restricts bundle/job features.

## If something fails

| Symptom | Action |
|---|---|
| HTTP 403 | Confirm contact email reaches the User-Agent. If it persists, use BLS contacts/support; do not rotate proxies or bypass controls. |
| HTTP 429 | Respect Retry-After. Long provider delays stop the run so it can be retried later. |
| Empty/changed listing | No candidate snapshot is committed. Rerun after the provider stabilizes. |
| Schema drift | Inspect the retained/returned provider format and update its contract; never force malformed data through. |
| Conflicting BLS overlap | Inspect duplicate source rows. Retry a fresh ingestion after BLS finishes publishing; otherwise resolve with documented provider guidance. |
| Missing label | Inspect the series/lookup codes and source version before changing the expectation. |
| Nonzero quarantine | Review rejected records and the source; do not submit until explained or corrected. |
| Missing one of 2013–2018 | Q1 intentionally fails its six-year expectation; investigate API coverage. |
| Module not found | Confirm the whole folder was imported, not only the pipeline file. |
| Volume permissions | Choose a writable catalog and ensure schema/Volume creation rights. |
| Incomplete manifest after interrupted write | Stop all writers/pipeline updates. Inspect the latest JSON. Restore the last complete manifest by moving only the incomplete file out of `manifests`, then rerun ingestion. Never silently skip corruption. |

A manifest is written only after all file objects exist. Start the pipeline only after
ingestion reports success. Volume FUSE is not a multi-file transaction; an interrupted
manifest write fails closed. There is no cross-process lock: do not run independent
manual ingestions concurrently with the bundle job.

## Required screenshots

Capture real workspace evidence after success. Do not substitute local Spark output.

- `01_pipeline.png`: successful pipeline graph with Bronze, Silver and Gold.
- `02_tables.png`: Catalog Explorer showing the resulting tables.
- `03_population_stats.png`: all columns of `gold_population_stats`.
- `04_best_year.png`: readable labels, best year, sum and quarters in `gold_best_year`.
- `05_series_population.png`: `gold_series_population`, sorted by year; show nulls where population is unavailable.
- Optional `06_quality.png`: expectation metrics; optional `07_rerun.png`: unchanged ingestion result.

Put screenshots in `evidence/` in your solution repository. Include the update time
and enough rows/columns to make each result understandable; hide unrelated account data.

## GitHub publication

The connected GitHub integration could read the source assignment but exposed no
repository creation/fork action, and no user repositories were accessible. No remote
solution repository has been created or populated by this work.

Create an empty private repository named `rearc-databricks` under your own account.
Do not initialize it with another README. Then use GitHub's upload UI, or run:

```bash
git init
git add .
git commit -m "Implement Rearc Databricks data quest"
git branch -M main
git remote add origin YOUR_NEW_REPOSITORY_URL
git push -u origin main
```

The package contains the solution, not a copy of Rearc's assignment text/history.
For an actual fork, fork the upstream separately and add the solution files in it;
remember public forks expose their contents. A private, shareable solution repo is
consistent with the submission requirements. Add the reviewers explicitly when Rearc
provides their accounts, verify access, and submit that URL plus the screenshots.

## Submission checklist

- [ ] Owner has read and can explain every submitted code path.
- [ ] Databricks pipeline succeeded and validation notebook passed.
- [ ] Actual screenshots added; no fabricated execution claims.
- [ ] Repository link is reachable by reviewers.
- [ ] `PROCESS.md` reflects the owner's understanding and actual experience.
- [ ] AI assistance is disclosed accurately.
