# European Wind Energy Data Platform on Azure Databricks

An end-to-end, production-grade lakehouse platform built on **Azure Databricks** and **Azure Data Lake Storage Gen2 (ADLS Gen2)**. This project processes multi-year (2016–2024) high-frequency 10-minute operational SCADA data (~3.8 GB compressed, ~2.84 million turbine records) from the **Kelmarsh Wind Farm (UK)** using **Infrastructure as Code (Terraform)**, **Unity Catalog**, **Databricks Asset Bundles (DABs)**, and **GitHub Actions CI/CD**.

---

## 1. Architecture Overview

```text
+-----------------------+       HTTPS Stream (REST API + MD5 Check)
|  Zenodo Open Data     | -----------------------------------------------+
|  Record ID: 16807551  |                                                |
|  (Kelmarsh 2016-2024) |                                                v
+-----------------------+                                  +---------------------------+
                                                           | Azure Databricks Compute  |
+-----------------------+       Git Push (CI Quality)      | (Serverless Workflow Job) |
|  Local VS Code IDE    | ------------------------------>  | Deployed via Asset Bundle |
|  (Terraform, uv, DAB) |       GitHub Actions (Ruff/TF)   +---------------------------+
+-----------------------+                                                |
                                                                         | Writes to Unity Catalog
                                                                         | External Volume (/Volumes/...)
                                                                         v
+--------------------------------------------------------------------------------------------------+
| Azure Data Lake Storage Gen2 (stwinddatacnbg) - Managed via Unity Catalog (kelmarsh_wind)        |
|                                                                                                  |
|  +------------------------+     +--------------------+     +--------------------+     +--------+ |
|  | Landing Volume         | --> | Bronze Layer       | --> | Silver Layer       | --> | Gold   | |
|  | (kelmarsh_landing_vol) |     | (Auto Loader Raw)  |     | (Cleaned & Joined) |     | (KPIs) | |
|  +------------------------+     +--------------------+     +--------------------+     +--------+ |
+--------------------------------------------------------------------------------------------------+