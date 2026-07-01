# Phase 2 — Interview Scenario Tasks (add-on, do after Phase 1)

Format matches real interview prompts: a scenario + "write the PySpark code for this."
No solutions given here on purpose — say the word when you're ready for a scenario
and I'll give you just the prompt (like an interviewer would), then review your code.

Planned scenarios (fill in as you go):
1. Top-N per group (e.g. top 3 products by revenue per category) — window + filter
2. Sessionize events (group user events into sessions with a 30-min gap rule)
3. Slowly Changing Dimension (SCD Type 2) merge logic
4. Deduplicate "fuzzy" records (same person, different formatting) without a fixed key
5. Diagnose and fix a skewed join (one key has 90% of the data)
6. Convert a slow UDF-based transformation to built-in functions
7. Incremental/delta load: process only new records since last run
8. Debug a plan: given an `explain()` output, find the expensive operation

Tell me when you want to start one and I'll run it interview-style.
