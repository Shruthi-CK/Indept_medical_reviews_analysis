# Independent Medical Review (IMR) Analysis on Snowflake

## Overview
This project leverages **Snowflake** to analyze California Independent Medical Review (IMR) data, uncovering trends in insurance denials, treatment necessities, and patient demographics. It implements a **Medallion Architecture** and integrates **Snowflake Cortex (GenAI)** to enrich raw medical text with sentiment scores and summaries.

**Data Source:** The dataset is sourced from the **California Department of Managed Health Care (DMHC)**, available via Kaggle (uploaded by user `prasad22`).

## Architecture
The pipeline transforms raw CSV data into business insights across three layers:

* **🥉 Bronze Layer:** Ingests raw data and enriches unstructured text (`Findings`) using **Cortex AI** to generate `SENTIMENT` and `SUMMARY` fields. It also establishes a Cortex Search Service for natural language querying.
* **🥈 Silver Layer:** Normalizes data into a **Star Schema** comprising standard Dimensions (`DIM_DIAGNOSIS`, `DIM_TREATMENT`, `DIM_PATIENT`, `DIM_DATE`) and a central Fact table (`FACT_IMR`).
* **🥇 Gold Layer:** Provides aggregated tables for reporting on temporal trends, clinical outcomes, and demographic disparities.

## 📂 Repository Structure

| File | Description |
| :--- | :--- |
| `Bronze_layer_queries.sql` | Schema setup, CSV ingestion, AI enrichment, and Search Service configuration. |
| `SILVER_LAYER.sql` | Creation and population of Dimension and Fact tables (Star Schema). |
| `GOLD_LAYER.sql` | Generation of aggregated analytical tables for specific business use cases. |
| `Incremental_Load.sql` | Logic for delta loads, handling new dimension members, and processing new AI metrics. |

## Usage

1.  **Prerequisites:**
    * Snowflake Account with Cortex enabled.
    * `Cal_Independent_Medical_Reviews.csv` uploaded to the stage.

2.  **Deployment:**
    Run the SQL scripts in the following order:
    1.  `Bronze_layer_queries.sql` (Initial Setup)
    2.  `SILVER_LAYER.sql` (Modeling)
    3.  `GOLD_LAYER.sql` (Analytics)

3.  **Handling New Data:**
    * Upload `Incremental_Load.csv` to the Snowflake stage.
    * Run `Incremental_Load.sql`. This script identifies new records, updates dimensions dynamically, and only runs Cortex AI functions on new rows.

