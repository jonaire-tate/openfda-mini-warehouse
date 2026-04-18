# openFDA Drug Events Mini-Warehouse

A personal project exploring the raw → warehouse → gold data engineering pattern using public pharma data from the openFDA API.

## Status

Phase 0: Setup (in progress)

## Planned Architecture

- **Raw layer:** Unprocessed JSON from the openFDA `/drug/event` endpoint, loaded into DuckDB.
- **Warehouse layer:** A single cleaned, flattened table. Dates parsed, strings standardized, nested fields expanded. Built entirely in SQL.
- **Gold layer:** Normalized tables (anchor drugs table, time-sensitive reports table, reactions table) plus an analysis view. Pandas reads from the view for final analysis.

## Tech Stack

- Python 3.11+
- DuckDB (embedded local database)
- pandas, requests, Jupyter
- Git / GitHub for version control

## Focus Area

Narcolepsy and sleep medicine adverse event reports.

## More to come

Full documentation, architecture diagram, and reproduction instructions will be added in Phase 5.
