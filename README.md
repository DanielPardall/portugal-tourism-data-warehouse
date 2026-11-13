# Tourism Analytics in Portugal

**An academic data analytics project combining ETL, dimensional modelling and interactive visualization to explore tourism activity across Portugal.**

The project brings together data on local accommodation, tourist overnight stays and resident population. A Databricks pipeline was developed to prepare the datasets and build a dimensional model for geographical and temporal analysis in Tableau.

## Interactive dashboard

[**Explore the Tableau Public dashboard**](https://public.tableau.com/app/profile/gustavo.sousa3053/viz/TAD_Dash/Descritiva)

The dashboard link was provided with the project. Its current availability could not be verified during preparation of this README.

## Project objectives

The analysis explores the relationship between tourism demand, local accommodation supply and the resident population in Portuguese regions. The project addresses questions such as:

- How did overnight stays and accommodation supply evolve over time?
- Where is accommodation capacity highest relative to the resident population?
- How does tourism demand vary across municipalities and regions?
- Which regions show stronger seasonal patterns?
- How might overnight stays evolve in the two years following the analysis period?

The study uses data for **2014–2024**, with monthly tourism indicators and annual population estimates. Results can be explored at municipality, NUTS III and NUTS II levels.

## Data sources

| Dataset | Source | Content |
| --- | --- | --- |
| Local accommodation | TravelBI / Turismo de Portugal | Municipality, geography, opening period, accommodation type and capacity |
| Tourist overnight stays | Instituto Nacional de Estatística (INE) | Monthly overnight stays by municipality and geographic hierarchy |
| Resident population | INE | Annual population estimates by municipality and geographic hierarchy |

The supplied repository contains the CSV files under [`data/`](data/). Consult the original providers for data definitions and terms of use.

## Architecture

According to the final project report, the data workflow was developed in **Databricks** using **PySpark, Delta Lake and Delta Live Tables**, with a medallion architecture:

1. **Bronze — ingestion:** load the source CSV datasets into Delta tables.
2. **Silver — preparation:** clean and standardize attributes, derive temporal fields and integrate sources by time and geography.
3. **Gold — analytical model:** aggregate data and calculate indicators for the fact and dimension tables.
4. **Tableau — exploration:** use the prepared model to build interactive dashboards and explore results by time and region.

The report describes a triggered Delta Live Tables pipeline. Incremental updates using newly arrived source files were not demonstrated in the supplied material.

## Data warehouse

The dimensional design uses two fact tables linked through shared dimensions of time and geography:

| Table | Role | Examples of information |
| --- | --- | --- |
| `fact_dormidas` | Tourism demand | Overnight stays, resident population, stays per resident, growth rate |
| `fact_alojamento` | Local accommodation supply | Number of accommodations, capacity, accommodation type |
| `dim_data` | Time | Year, month, quarter and season |
| `dim_geografia` | Geography | Municipality, NUTS III and NUTS II |

The report describes overnight-stay measures at monthly granularity and accommodation measures at monthly and accommodation-type granularity, with geographic analysis.

The included [`star_model.json`](star_model.json) is a **visual model export**, not an executable SQL schema. It represents relationships as diagram links; the fact-table fields in the export do not explicitly list corresponding foreign keys. The JSON should therefore be read as the supplied model design rather than a complete definition of the deployed tables.

## Tableau analysis

The final report describes four dashboards covering:

- **Descriptive analysis:** changes over time, accommodation capacity and geographic distribution.
- **Diagnostic analysis:** supply and demand, seasonal patterns and indicators relative to population.
- **Predictive analysis:** estimated monthly overnight stays for 2025 and 2026 based on the analysis period.

The dashboards use filters to explore different periods and geographic levels. Prescriptive questions are discussed in the report as interpretations to support planning; the analysis alone does not establish causal effects or validate policy outcomes.

## Technologies

- **ETL and processing:** Databricks, PySpark, Delta Live Tables.
- **Storage and governance:** Delta Lake, Unity Catalog.
- **Analytics:** Data Warehousing, dimensional modelling, star schema, OLAP and SQL concepts.
- **Visualization:** Tableau, Tableau Public.
- **Data sources:** INE and TravelBI.

## Repository contents

| Path | Description |
| --- | --- |
| [`data/alojamento_local_2014_2024.csv`](data/alojamento_local_2014_2024.csv) | Local accommodation data |
| [`data/dormidas_municipios_mensal_2014_2024_hierarquico.csv`](data/dormidas_municipios_mensal_2014_2024_hierarquico.csv) | Monthly overnight stays |
| [`data/populacao_municipios_2014_2024.csv`](data/populacao_municipios_2014_2024.csv) | Annual resident population |
| [`star_model.json`](star_model.json) | Dimensional model export |
| `requisitos.txt` | Analytical questions and requirements in Portuguese |

## Reproducibility

The input CSV files and visual model are included, but **the Databricks ETL notebook is not included in the supplied repository**. The project materials refer to a workspace notebook named `TAD_ETL_DLT.py`; its source, the deployed Delta tables and the Tableau workbook were not supplied.

As a result, the included files cannot independently recreate the original pipeline or dashboards. To reproduce the project, the original notebook and Databricks configuration must be restored, source paths adapted and the workflow validated in a compatible environment. The academic report documents the implementation; the code itself was not available for inspection during this README update.

## Academic context and authors

Developed for **Tecnologias e Análise de Dados**, University of Coimbra, 2026.

- Daniel Monteiro
- Gustavo Sousa
- Íris Sousa
