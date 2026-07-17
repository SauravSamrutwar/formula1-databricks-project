# Formula 1 Data Engineering Pipeline on Azure Databricks

An end-to-end Lakehouse pipeline built on Azure Databricks that ingests, transforms, and analyzes historical Formula 1 racing data — implemented using the Medallion (Bronze/Silver/Gold) architecture with Delta Lake.

## Architecture

```
Raw CSV/JSON (Open F1 API/dataset)
        │
        ▼
   BRONZE LAYER   →  Schema-enforced raw ingestion, stored as Parquet/Delta
        │
        ▼
   SILVER LAYER   →  Cleaned, conformed, joined tables (PySpark + Spark SQL)
        │
        ▼
   GOLD LAYER     →  Aggregated business-level tables (driver/constructor standings,
                      dominant teams & drivers over time)
        │
        ▼
   Databricks SQL Dashboards → Visual insights & reporting
```

*(Replace this ASCII diagram with an exported PNG from draw.io/Excalidraw for the actual README — GitHub renders images better than ASCII for skimmers.)*

## What This Project Does

- Ingests raw Formula 1 CSV/JSON data with **schema enforcement** to catch malformed or unexpected data early
- Stores raw and intermediate data in **Parquet/Delta format** for reliability and performance
- Supports **incremental loads** so new race data can be appended without reprocessing the full history
- Uses **PySpark and Spark SQL** to clean, join, and transform raw tables into analysis-ready datasets
- Leverages **Delta Lake** for ACID transactions, schema evolution, time-travel queries, and audit history
- Surfaces insights through **Databricks SQL dashboards**: driver standings, constructor standings, and dominant teams/drivers across seasons
- Designed with **automatic scheduling** in mind for production-style orchestration

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | Azure Databricks, Apache Spark (PySpark, Spark SQL) |
| Storage | Azure Data Lake Storage (ADLS Gen2), Delta Lake |
| Orchestration | Databricks Jobs / Lakeflow Jobs |
| Governance | Unity Catalog |
| Reporting | Databricks SQL Dashboards |
| Language | Python |

## Project Structure

```
formula1/
├── bronze/          # Raw ingestion notebooks
├── silver/          # Cleaning & transformation notebooks
├── gold/             # Aggregation & business logic notebooks
└── dashboards/        # SQL queries powering the dashboards
```
*(Update this to match your actual folder layout inside `formula1/`.)*

## Sample Insights Produced

- Season-by-season driver and constructor standings
- Most dominant drivers/teams by era
- Race-by-race performance trends

*(Add 2-3 screenshots here — a dashboard view, a notebook cell output, and the ADLS folder structure. This is the single highest-impact addition: it turns "trust me" into visible proof.)*

## What I'd Extend Next

- Add data quality checks (e.g., Great Expectations or custom PySpark assertions) at the Silver layer
- Add a CI step to validate notebook runs on push
- Extend Gold layer with lap-time and pit-stop strategy analysis

## Notes

Built as a hands-on project while completing an Azure Databricks & Apache Spark data engineering course, then extended independently.
