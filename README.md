# DLT SQL Indexer

Full IDE support for **Databricks Delta Live Tables (DLT) SQL** projects — navigate, refactor, and validate your pipeline models without leaving VS Code.

Built for large DLT codebases where models are spread across hundreds of SQL files and dependencies are defined via `LIVE.model_name` and `{{ ref('model') }}` references.

---

## Features

### Go to Definition
Click any `LIVE.model_name` or `{{ ref('model') }}` reference and jump straight to the `CREATE` statement in the source file. Works across the entire workspace.

### Hover Info
Hover over any model reference to see its resolved column list, data types, layer, model kind, dependencies, and which other models use it — all without leaving the current file.

![Hover showing column list, layer, and dependencies]

### Column & Model Autocomplete
Get intelligent completions as you type:
- `LIVE.` triggers a list of all indexed models
- Column names are suggested based on what the upstream model actually exposes

### Find All References
`Shift+F12` on any model name shows every file and line that references it — across the full workspace.

### F2 Rename / Refactor
Rename a model or a column and have every occurrence updated automatically:

- **Model rename** — updates the `CREATE` statement, all `LIVE.xxx` references, all `{{ ref('xxx') }}` calls, and renames the `.sql` file
- **Column rename** — traces the column to its origin model and renames it across the full dependency chain (staging → intermediate → marts)

### Diagnostics (Warning Squiggles)
Real-time validation as you type:
- Unknown `LIVE.model` references — model doesn't exist in the index
- Unknown column names — column not found in the upstream model's resolved schema
- Duplicate model names — two files defining the same model name

### Dependency Explorer
`DLT: Show Model Dependencies` command shows what a model depends on and which models depend on it, with one-click navigation.

### Model Tree View
Sidebar panel listing all indexed models organized by layer (staging → intermediate → marts) and country.

### Status Bar
Shows the total number of indexed models. Click to rebuild the index.

---

## Context

DLT SQL projects use a medallion architecture where:
- **Staging** models read from external sources (e.g. `production.schema.table`)
- **Intermediate** models join and transform staging data using CTEs
- **Marts** models expose the final business-facing datasets

Navigating these dependencies manually — especially with hundreds of models — is painful. This extension indexes the full graph and makes the codebase feel like a typed language.

---

## Supported Syntax

All DLT `CREATE` statement variants:

```sql
CREATE LIVE TABLE model_name AS (...)
CREATE OR REFRESH LIVE TABLE model_name AS (...)
CREATE TEMPORARY LIVE VIEW model_name AS (...)
CREATE STREAMING TABLE model_name AS (...)
CREATE OR REPLACE MATERIALIZED VIEW model_name (...) AS (...)
```

References:

```sql
FROM LIVE.model_name
FROM LIVE.model_name AS alias
FROM {{ ref('model_name') }}
```

---

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `dltSqlIndexer.modelsPath` | `"models"` | Relative path to the models directory |
| `dltSqlIndexer.excludePatterns` | `[]` | Glob patterns to exclude from indexing |
| `dltSqlIndexer.enableDiagnostics` | `true` | Enable column validation squiggles |

---

## Commands

| Command | Description |
|---------|-------------|
| `DLT: Rebuild Model Index` | Re-scan all SQL files and rebuild the index |
| `DLT: Show Model Dependencies` | Show dependencies for the model in the active file |

---

## Requirements

- VS Code 1.85+
- A workspace containing DLT SQL model files (`.sql`)

Compatible with dbt, SQLFluff, and other SQL extensions — picks up their language IDs automatically.

---

## License

MIT
