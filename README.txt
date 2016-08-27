Steps
------
1. Run main.py (1 transaction => inserts data, creates indexes, refreshes materialized views)
2. Change the end date in experimental_gotvalue.py
3. Run experimental_gotvalue.py (creates a json)
4. Run insert_got_value.py
5. Run insert_experimental_sessions.py
6. (OPTIONAL) would be *really good* to run "VACUUM ANALYZE" in Postgres
7. Run run_analytics.py (specify if you want "csv" or "json" (default), and the dates and timedeltas)
8. If report is in json ("report.json"), run `json_to_csv.py` to generate all reports for each date range.
