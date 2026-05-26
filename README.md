# Financial Complaints Analytics Pipeline

## Overview

Cidesus is an end-to-end Data Engineering project built on Apache Spark and Databricks that automates the ingestion, transformation, enrichment, and analysis of large-scale financial complaint datasets from the Consumer Financial Protection Bureau (CFPB).

The platform transforms raw complaint data into production-ready analytical assets through a modern Medallion Architecture (Bronze → Silver → Gold), enabling regulatory monitoring, anomaly detection, and executive-level reporting.

---

# Business Context & Challenges

## 📌 The Problem

Financial regulators and consumer protection agencies face several operational challenges:

- Massive datasets containing millions of complaint records
- Disconnected information systems preventing unified analytics
- Manual Excel-based reporting processes requiring days or weeks
- Lack of standardized KPIs for monitoring company compliance
- Difficulty identifying high-risk companies and products

---

## 🔍 The Solution

Cidesus automates the entire analytics lifecycle by:

- Ingesting raw CFPB complaint datasets
- Cleaning and standardizing records with PySpark
- Engineering quality and compliance indicators
- Generating Gold analytical tables for BI and SQL analytics
- Detecting regulatory anomalies automatically
- Delivering scalable Parquet-based analytical outputs

---

# Technical Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA PIPELINE ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  [CFPB CSV/ZIP] ─► [Bronze] ─► [Silver] ─► [Gold Analytics Layer]       │
│                                                                         │
│   Raw Data         Raw Files      Cleaned Data      KPIs & Metrics      │
│                                                                         │
│  ┌───────────┐   ┌───────────┐   ┌────────────┐   ┌────────────────┐    │
│  │ CSV/ZIP   │─► │ PySpark   │─► │ Feature    │─► │ Parquet + SQL  │    │
│  │ Ingestion │   │ Cleaning  │   │ Engineering│   │ Analytics      │    │
│  └───────────┘   └───────────┘   └────────────┘   └────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

# Data Architecture

| Layer | Technology | Description |
|---|---|---|
| Bronze | Raw CSV / ZIP | Original CFPB complaint data |
| Silver | PySpark DataFrames | Cleaned, standardized, enriched datasets |
| Gold | Parquet + SQL Views | Aggregated KPIs and executive analytics |

---

# Storage Architecture

The Gold layer is persisted inside a Unity Catalog Volume:

```text
/Volumes/complaints/my_schema/complaints_csv/
```

Main analytical outputs:

```text
/Volumes/complaints/my_schema/complaints_csv/company_summary
```

---

# Example KPI Aggregation

```python
from pyspark.sql.functions import *

company_summary = (
    df_analysis
    .groupBy("entreprise")
    .agg(
        count("*").alias("total_plaintes"),
        sum("reponse_qualite").alias("reponses_delai_respecte"),
        sum("a_conteste").alias("total_contestations"),
        round(avg("reponse_qualite") * 100, 2).alias("taux_conformite_pct"),
        round(avg("a_conteste") * 100, 2).alias("taux_contestation_pct"),
        round(avg("delai_reponse"), 1).alias("delai_moyen_jours"),
        round(sum("score_qualite"), 2).alias("score_global")
    )
    .filter(col("total_plaintes") >= 10)
    .orderBy(col("total_plaintes").desc())
)
```

---

# SQL Analytics Example

```python
company_summary.createOrReplaceTempView("company_summary")

sql_result = spark.sql(
    """
    SELECT
        entreprise,
        total_plaintes,
        taux_conformite_pct,
        taux_contestation_pct
    FROM company_summary
    WHERE total_plaintes > 50
    ORDER BY total_plaintes DESC
    LIMIT 10
    """
)
```

---

# Technologies Used

| Category | Technology |
|---|---|
| Processing | Apache Spark / PySpark |
| Storage | Parquet / Delta Lake |
| Platform | Databricks |
| Language | Python 3 |
| Governance | Unity Catalog |
| Version Control | Git / GitHub |

---

# License

MIT License
