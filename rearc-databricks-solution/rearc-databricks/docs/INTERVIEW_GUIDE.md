# Explain the implementation

Read the source alongside this guide. Do not memorize answers without running the code.

1. **Why request a directory instead of known filenames?** The upstream inventory can
   change. The HTML parser discovers same-origin direct children and rejects parent,
   external and encoded traversal paths. Parsing contracts are separate from discovery.
2. **Why ETag and SHA-256?** ETag/Last-Modified avoid unnecessary transfers when supported.
   Content hashes avoid repeat parsing/storage when the server returns identical bytes.
3. **What happens if the fifth download fails?** New immutable objects might exist,
   but no new manifest activates them. The last committed dataset remains selected.
4. **What happens when a file disappears?** It is absent from the next manifest; the
   Bronze semi-join excludes it. Raw history remains for audit.
5. **Why aren't all files read as a stream?** This upstream revises history and removes
   files. A plain append-only Auto Loader design cannot propagate those removals by itself.
6. **Where is the Bronze boundary?** Volume files are raw. Cached JSON contains string
   records and provenance. The managed pipeline creates the first queryable Bronze tables.
7. **Why not use `dropDuplicates` blindly?** Identical records can collapse, but different
   values for the same series/year/period must raise a conflict instead of choosing at random.
8. **Why exclude Q05?** It is an annual average, not a fifth quarter.
9. **What if only two quarters exist?** Their values participate; the output says two
   quarters are available. This follows the requested arithmetic, not a four-quarter policy.
10. **Why two standard deviations?** The task leaves the definition ambiguous. Population
    SD treats the six years as the complete requested set; sample SD is supplied explicitly.
11. **How are labels made?** Join the series metadata to the provider's sector, class,
    measure, duration and seasonal lookup tables, including index base when relevant.
12. **Why a left join for Q3?** Preserve historical BLS observations even when the API
    has no population for that year.
13. **What is an expectation?** A managed dataset constraint that records, drops or fails
    invalid rows. This project drops malformed typed rows into a separately visible quarantine
    path and fails ambiguous duplicates, incomplete labels and missing Q1 coverage.
14. **What is still not production-grade?** Concurrent-writer protection, transactionally
    committed inventory, raw-history retention and avoiding full historical JSON scans.
15. **What did AI do?** Describe the actual assistance stated in PROCESS.md. Then explain
    your own review, changes and real Databricks test results.

Practice: alter one fixture to introduce a conflicting overlap, remove a population
fixture year, add an annual-only series, and explain what each expectation/test should do.
