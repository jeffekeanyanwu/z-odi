
# Cricket Match Data Ingestion

This program ingests ODI cricket match data from JSON files into a DuckDB database.

## Prerequisites

- Docker
- Make

## Usage


### Run Full Pipeline
```bash
make all
```
This will:
- Build the Docker container
- Initialize the database
- Run the full ingestion
- Clean up JSON files after successful ingestion
- Create database in project root as `odi_data.db`. You can connect/view that database in PyCharm's Database Viewer or use the [DuckDB Python API](https://duckdb.org/docs/api/python/overview).

### Run Test Pipeline

```bash
make test
````
This will run the same pipeline as above but only process the first 10 matches. Useful for testing the pipeline works correctly before running the full ingestion.


### Clean Up
```bash
make clean
```
This will remove:
- The database file
- Log files
- JSON files in data directory

## Database Structure

The database contains the following tables:
- matches: Main match information
- innings: Ball-by-ball data
- players: Player information
- match_players: Junction table for players in matches
- officials: Match officials (left empty, not needed for this task)

## Notes

- Data is sourced from Cricsheet.org
- Database is created in project root directory
- JSON files are stored in /data directory during processing
- Logs are created in project root