# DLT SQL Indexer — Claude Memory

## What This Is

VS Code extension (`clara-team.dlt-sql-indexer`) that provides IDE features for Databricks DLT SQL models in the companion dbt project at `C:\Users\Sergio\repos\data-analytics-dbt`.

Scale: ~1581 SQL model files indexed.

---

## Project Structure

```
src/
  extension.ts              — Entry point, registers all providers
  indexer.ts                — DltModelIndexer: file scanning, model index building
  types.ts                  — Core types (see below)
  schema/
    types.ts                — Schema-specific types (ResolvedColumn, SelectItem, etc.)
    schemaResolver.ts       — Resolves output columns through CTE/dependency chain
    cteAnalyzer.ts          — CTE block parsing, paren matching, FROM clause parsing
    columnExtractor.ts      — SELECT clause → SelectItem[]
    typeInference.ts        — Infers data types from expressions
  providers/
    definitionProvider.ts   — Go to Definition (LIVE.x, ref('x'), CREATE statement)
    referenceProvider.ts    — Find All References
    hoverProvider.ts        — Hover (shows resolved columns)
    completionProvider.ts   — Autocomplete for model/column names
    renameProvider.ts       — F2 rename (model names + column names)
  diagnostics/
    diagnosticProvider.ts   — Real-time squiggles (unknown refs, unknown columns, duplicates)
  views/
    modelTreeProvider.ts    — Sidebar tree view of models
```

---

## Core Types (`src/types.ts`)

```typescript
interface IndexedModel {
  name: string;           // model name as in CREATE statement
  filePath: string;       // absolute path
  uri: vscode.Uri;
  definitionLine: number; // 0-based line of CREATE statement
  layer: ModelLayer;      // "staging" | "intermediate" | "marts" | "dbt" | "unknown"
  kind: ModelKind;        // "live_table" | "live_view" | "materialized_view" | "streaming_table" | "unknown"
  references: ModelReference[];  // other models this depends on
  ctes: CteDefinition[];
  columns: ColumnDefinition[];   // explicit columns (materialized views)
  yml?: YmlMetadata;
  country?: string;              // "br" | "mx" | "co" detected from path
  relativePath: string;

  // Populated by SchemaResolver:
  resolvedColumns?: ResolvedColumn[];     // final output columns
  resolutionStatus?: "pending" | "resolving" | "resolved" | "error";
  resolvedCteColumns?: Map<string, ResolvedColumn[]>;  // keyed by CTE name (lowercase)
  rawCteBlocks?: RawCteBlock[];
  rawSelectBody?: string;
}

interface ModelIndex {
  models: Map<string, IndexedModel>;     // keyed by lowercase name
  referencedBy: Map<string, Set<string>>; // reverse dependency graph
  fileToModel: Map<string, string>;       // filePath → model name
  modelFiles: Map<string, Set<string>>;   // lowercase name → all file paths (duplicate detection)
}
```

From `src/schema/types.ts`:
```typescript
interface ResolvedColumn {
  name: string;
  dataType?: string;
  confidence: "known" | "inferred" | "unknown";
  source?: string;     // CRITICAL: origin model name where column was first defined
  comment?: string;
  isNullable?: boolean;
}

interface SelectItem {
  expression: string;
  alias?: string;
  isStar: boolean;
  starQualifier?: string;  // for alias.*
  tableAlias?: string;     // for alias.column
  columnName?: string;     // bare or qualified column name
  lineOffset?: number;
}

interface FromSource {
  sourceName: string;
  alias: string;
  isLiveRef: boolean;   // true if LIVE.xxx
  isDbtRef: boolean;    // true if {{ ref('xxx') }}
}

interface RawCteBlock {
  name: string;
  body: string;
  startLine: number;
}

interface CteScope {
  name: string;
  columns: ResolvedColumn[];
  sources: FromSource[];
  fullyResolved: boolean;
}
```

---

## DLT SQL Syntax Patterns

```sql
-- Model definitions:
CREATE TEMPORARY LIVE VIEW model_name AS (...)
CREATE OR REFRESH LIVE TABLE model_name AS (...)
CREATE OR REPLACE MATERIALIZED VIEW model_name (col TYPE, ...) AS (...)
CREATE STREAMING TABLE model_name AS (...)

-- References to other DLT models:
FROM LIVE.other_model
FROM LIVE.other_model ec  -- implicit alias
FROM LIVE.other_model AS ec  -- explicit alias

-- dbt-style references:
FROM {{ ref('other_model') }}

-- External Databricks tables (not indexed, no columns known):
FROM production.mx_operations.operative_operations

-- CTEs (standard WITH clause):
WITH cte_name AS (
  SELECT ...
),
another_cte AS (...)
SELECT ...

-- Databricks-specific:
QUALIFY ROW_NUMBER() OVER (PARTITION BY ...) = 1
```

---

## SQL Parsing Pipeline

### Step 1: `cteAnalyzer.ts` — `extractCteBlocks(content)`

1. Calls `extractInnerBody(content)` to find the SQL body inside `AS (...)`.
2. Scans from body start, skipping whitespace + `--` comments + `/* */` block comments.
3. Checks if the first non-comment token is `WITH`.
4. If WITH found: iterates parsing `name AS (body)` CTE blocks using `findMatchingParen`.
5. Returns `{ ctes: RawCteBlock[], finalSelectBody: string }`.

**Key function: `findMatchingParen(text, openIndex)`**
- Tracks depth for `(` / `)`.
- State machine: `inSingleQuote`, `inLineComment`, `inBlockComment`.
- Block comment: enters on `/*`, exits on `*/`.

**Key function: `splitUnionAll(body)`**
- Splits a SELECT body on top-level `UNION ALL` / `UNION`.
- Same state machine: `inSingleQuote`, `inLineComment`, `inBlockComment`.
- Tracks `currentLine` for line offset accuracy — block comments must increment `currentLine` on `\n` inside them.

**Key function: `parseFromClause(selectBody)`**
- First strips all `/* */` block comments: `.replace(/\/\*[\s\S]*?\*\//g, " ")`
- Then strips `--` line comments.
- Collapses whitespace.
- Walks string tracking depth, finds `FROM` and `[type] JOIN` keywords at depth 0.
- Parses each source ref via `parseSourceRef()`.

`parseSourceRef()` handles:
- `LIVE.model [AS] alias` → `{ isLiveRef: true }`
- `{{ ref('model') }} [AS] alias` → `{ isDbtRef: true }`
- `{{ source('src', 'table') }} [AS] alias`
- `schema.table [AS] alias` (external tables)
- `simple_name [AS] alias` (CTE reference)

### Step 2: `columnExtractor.ts` — `parseSelectList(selectBody)`

1. Finds `SELECT [DISTINCT]` keyword.
2. Calls `findTopLevelKeyword(afterSelect, "FROM")` to find where column list ends.
3. Calls `splitAtTopLevelCommas(columnListStr)` for each item.
4. Calls `parseSelectItem(seg.text)` on each item.

**`findTopLevelKeyword`** — state machine including `inBlockComment`:
```typescript
if (inBlockComment) {
  if (ch === "*" && text[i+1] === "/") { inBlockComment = false; i++; }
  continue;
}
// ... line comment check ...
if (ch === "/" && text[i+1] === "*") { inBlockComment = true; i++; continue; }
```

`parseSelectItem` detects:
- `*` → `isStar: true`
- `alias.*` → `isStar: true, starQualifier`
- `alias.column [AS name]` → `tableAlias, columnName`
- `column [AS name]` → bare `columnName`
- `` `backtick` `` quoted identifiers
- Complex expressions → `expression` only, alias from `AS`

### Step 3: `schemaResolver.ts` — `SchemaResolver.resolveAll(models)`

Topological sort → resolves dependencies first.

For each model:
1. If `materialized_view` with explicit columns → use those, `source = model.name`.
2. For each CTE in `rawCteBlocks`:
   - `parseFromClause(cteBody)` → sources
   - `buildAliasColumnMap(sources, model, previousScopes)` → alias → columns map
   - `parseSelectList(cteBody)` → items
   - Expand stars, resolve each item → `ResolvedColumn[]`
   - Store in `cteScopes` and `model.resolvedCteColumns`
3. For `rawSelectBody`: same process with full `cteScopes` available.
4. **CRITICAL FALLBACK**: After setting `model.resolvedColumns`, loop and set `col.source = model.name` for any column where source is still undefined.

`buildAliasColumnMap()` resolution order:
1. Check `cteScopes` → use CTE's resolved columns
2. Check `source.isLiveRef || source.isDbtRef` → look up model, resolve it recursively if needed, use its `resolvedColumns`
3. External source → empty array `[]` (no columns known)

`resolveSelectItemColumn()`:
- Qualified `alias.column`: looks up alias in aliasMap, finds srcCol, copies `source` and `dataType`.
- Bare `column`: searches all aliases in order.
- If srcCol found: copies `source` chain (this is how source propagates downstream).
- If not found: `source` stays undefined → fixed by the fallback in `resolveModel`.

---

## Feature Implementations

### Duplicate Model Detection

**`src/indexer.ts`**: Before `models.set(key, model)`:
```typescript
const existing = index.modelFiles.get(key) || new Set<string>();
existing.add(model.filePath);
index.modelFiles.set(key, existing);
```

**`src/diagnostics/diagnosticProvider.ts`**: After getting the model:
```typescript
const duplicateFiles = this.indexer.getDuplicateFiles(model.name);
if (duplicateFiles.length > 0) {
  // DiagnosticSeverity.Error on the CREATE statement line
}
```

### Rename Provider (`src/providers/renameProvider.ts`)

#### Model Rename
`extractModelNameRange()` detects cursor is on:
- `CREATE ... LIVE TABLE/VIEW name` → regex captures name
- `LIVE.name` → `nameStart = match.index + 5`
- `{{ ref('name') }}`

`renameModel(oldName, newName)` produces `WorkspaceEdit`:
1. Replace name in CREATE line of definition file
2. For each model in `getReferencedBy(oldName)`: replace `LIVE.oldName` and `ref('oldName')` across all lines (skips comment lines)
3. `edit.renameFile(oldUri, newUri)` — renames the `.sql` file

#### Column Rename
`renameColumn(document, oldName, newName)`:
1. Gets `currentModel` by file path
2. Finds `col` in `currentModel.resolvedColumns`
3. Traces `col.source` up the chain (visits each model's resolved columns until `source` stops changing → finds root origin)
4. Scans **all** models: if `modelCol.source === effectiveSource` → add to `filesToRename`
5. Calls `replaceColumnInDocument()` for each file

`replaceColumnInDocument()`:
- Per line: skip comment-only lines, strip inline comments
- `\bcolName\b` regex on code part
- Check single-quote parity to skip string literals

**Design decision**: Global `(name, source)` scan — sibling models that get a column from the same upstream source are ALL renamed together. This is intentional and correct.

---

## Bug History & Root Causes

### Bug 1: Column source undefined for external tables

**Symptom**: Column rename from a staging model didn't propagate. Column rename only renamed in current file.

**Root cause**: Staging models that SELECT from `production.xxx.yyy` (external tables not in the index). `buildAliasColumnMap` sets `aliasMap.set(alias, [])` for external sources. `resolveSelectItemColumn` looks up the column in `[]` → not found → `source` stays `undefined`. The root origin tracing in `renameColumn` has nothing to walk.

**Fix** (`schemaResolver.ts`, end of `resolveModel`):
```typescript
if (model.resolvedColumns) {
  for (const col of model.resolvedColumns) {
    if (!col.source) {
      col.source = model.name;   // staging model becomes the "origin"
    }
  }
}
```

### Bug 2: Diagnostic squiggle on comment lines

**Symptom**: "Column not found" diagnostic highlighted a `-- comment` line instead of the actual column line.

**Root cause**: `validateBareColumnReferences` searches lines for the column name with `\bname\b`. Lines like `--keys` contain column names inside comments. The search matched the comment line first.

**Fix** (`diagnosticProvider.ts`, in the line search loop):
```typescript
for (let bl = lineInBody; bl < bodyLines.length; bl++) {
  const bline = bodyLines[bl];
  if (bline.trimStart().startsWith("--")) continue;  // skip comment-only lines
  const commentIdx = bline.indexOf("--");
  const codePart = commentIdx >= 0 ? bline.substring(0, commentIdx) : bline;
  const pattern = new RegExp(`\\b${escapeRegex(searchName)}\\b`, "i");
  const m = pattern.exec(codePart);
  if (m) { foundLine = startLine + bl; foundCol = m.index; break; }
}
```

### Bug 3: Block comments (`/* */`) completely unhandled — ROOT CAUSE of most issues

**Symptom**: Many false-positive "column not found" diagnostics. Real columns from upstream models not being resolved. Specifically, `economic_concepts_operations` CTE in `int_mx_lt_all_transactions.sql` reported all columns as not found.

**Root cause**: `stg_mx_operations__economic_concepts_operations.sql` has:
```sql
CREATE TEMPORARY LIVE VIEW mx_economic_concepts_operations AS (
    /* Economic concepts operations (EC). Tracking cancelled transactions ... */
    -- noqa: disable=ST06
    WITH id_duplicated_cancelled_op AS (
```

The old `extractCteBlocks` used:
```typescript
const withMatch = body.match(/^\s*WITH\s/i);
```
`\s*` does NOT skip `/* */` block comments. WITH was never found. The entire body was treated as `finalSelectBody` with no CTEs. `parseSelectList` parsed the first CTE's SELECT as if it were the final SELECT — giving completely wrong output columns for `mx_economic_concepts_operations`. Downstream diagnostics then flagged every real column as "not found".

**Fixes applied**:

#### `cteAnalyzer.ts` — `extractCteBlocks`: Preamble skip loop
```typescript
// Find WITH keyword, skipping whitespace, -- comments, and /* */ block comments
let withPos = 0, withStartLine = bodyStartLine;
{
  let p = 0;
  while (p < body.length) {
    if (/\s/.test(body[p])) { if (body[p]==="\n") withStartLine++; p++; continue; }
    if (p < body.length-1 && body[p]==="-" && body[p+1]==="-") {
      while (p < body.length && body[p]!=="\n") p++; continue;
    }
    if (p < body.length-1 && body[p]==="/" && body[p+1]==="*") {
      p += 2;
      while (p < body.length-1 && !(body[p]==="*" && body[p+1]==="/")) {
        if (body[p]==="\n") withStartLine++; p++;
      }
      if (p < body.length-1) p += 2;
      continue;
    }
    break;
  }
  withPos = p;
}
const afterPreamble = body.substring(withPos);
const withMatch = afterPreamble.match(/^WITH\s/i);
```

#### `cteAnalyzer.ts` — CTE parsing skip loop: also handles block comments
The while loop that skips whitespace/commas between CTEs now includes:
```typescript
if (pos < body.length-1 && body[pos]==="/" && body[pos+1]==="*") {
  pos += 2;
  while (pos < body.length-1 && !(body[pos]==="*" && body[pos+1]==="/")) {
    if (body[pos]==="\n") currentLine++;
    pos++;
  }
  if (pos < body.length-1) pos += 2;
  skipped = true;
}
```

#### `cteAnalyzer.ts` — `findMatchingParen`: `inBlockComment` state
```typescript
let inBlockComment = false;
// In the loop:
if (inBlockComment) {
  if (ch === "*" && text[i+1] === "/") { inBlockComment = false; i++; }
  continue;
}
// ...
if (ch === "/" && text[i+1] === "*") { inBlockComment = true; i++; continue; }
```

#### `cteAnalyzer.ts` — `splitUnionAll`: `inBlockComment` with newline counting
```typescript
if (inBlockComment) {
  if (ch === "\n") { currentLine++; continue; }
  if (ch === "*" && body[i+1] === "/") { inBlockComment = false; i++; }
  continue;
}
// ...
if (ch === "/" && body[i+1] === "*") { inBlockComment = true; i++; continue; }
```

#### `cteAnalyzer.ts` — `parseFromClause`: strip block comments before processing
```typescript
const cleaned = selectBody
  .replace(/\/\*[\s\S]*?\*\//g, " ")   // strip block comments
  .replace(/--[^\n]*/g, "")             // strip line comments
  .replace(/\s+/g, " ").trim();
```

#### `columnExtractor.ts` — `findTopLevelKeyword`: `inBlockComment` state
```typescript
let inBlockComment = false;
// In the loop (BEFORE line comment check):
if (inBlockComment) {
  if (ch === "*" && text[i+1] === "/") { inBlockComment = false; i++; }
  continue;
}
// ... existing line comment check ...
if (ch === "/" && text[i+1] === "*") { inBlockComment = true; i++; continue; }
```

### Bug 4: Infinite logging loop

**Symptom**: Output channel spammed with `[Diag] validateDocument called: langId=Log` repeating forever.

**Root cause**: VS Code output channels created with `vscode.window.createOutputChannel()` are internally `TextDocument` objects with `languageId: "Log"`. Every `appendLine` call fires `onDidChangeTextDocument`. The diagnostic provider subscribed to `onDidChangeTextDocument` and called `validateDocument`. The old code logged BEFORE checking the language guard:

```typescript
// OLD (broken):
validateDocument(document) {
  this.outputChannel.appendLine(`[Diag] called: langId=${document.languageId}...`);  // <- writes to channel
  if (!SQL_LANGUAGE_IDS.has(document.languageId) && !fileName.endsWith(".sql")) {
    this.outputChannel.appendLine(`[Diag] Skipped...`);  // <- ANOTHER write → infinite loop
    return;
  }
}
```

Loop: `appendLine` → `onDidChangeTextDocument(outputChannelDoc)` → `validateDocument` → `appendLine` → ...

**Fix** (`diagnosticProvider.ts`):
```typescript
// FIXED: guard BEFORE any logging
validateDocument(document: vscode.TextDocument): void {
  if (!SQL_LANGUAGE_IDS.has(document.languageId) && !document.fileName.endsWith(".sql")) {
    return;  // silent return — no appendLine, no feedback loop
  }
  this.outputChannel.appendLine(`[Diag] validateDocument called: ...`);
  // ...
}
```

---

## Diagnostic Provider Details (`src/diagnostics/diagnosticProvider.ts`)

### `SQL_LANGUAGE_IDS` set
```typescript
const SQL_LANGUAGE_IDS = new Set([
  "sql", "jinja-sql", "sql-bigquery", "sql-databricks", "databricks-sql"
]);
```

### Validation pipeline in `validateDocument`:
1. Language guard (FIRST — before any logging)
2. Config check: `dltSqlIndexer.enableDiagnostics`
3. Get model by file path
4. **Check 0**: Duplicate model names → `DiagnosticSeverity.Error` on CREATE line
5. **Check 1**: `LIVE.model` references — does the model exist in the index?
6. **Check 2**: `validateBareColumnReferences` — for each SELECT in each CTE + final SELECT, check bare column names against resolved columns

### `validateBareColumnReferences` approach:
- Re-parses the document's current text (not the cached index) using `extractCteBlocks` + `splitUnionAll` + `parseSelectList`
- Compares against `model.resolvedCteColumns` (per-CTE) and the upstream model's `resolvedColumns`
- For each unknown column: searches forward in the file body to find the actual line (skipping comment lines, stripping inline comments before matching)

---

## Extension Entry Point (`src/extension.ts`)

```typescript
// SQL language selector (covers dbt/sqlfluff extension language IDs)
const sqlSelector: vscode.DocumentSelector = [
  { language: "sql", scheme: "file" },
  { language: "jinja-sql", scheme: "file" },
  { language: "sql-bigquery", scheme: "file" },
  { language: "sql-databricks", scheme: "file" },
  { language: "databricks-sql", scheme: "file" },
  { scheme: "file", pattern: "**/*.sql" },
];

// Registered providers:
vscode.languages.registerDefinitionProvider(sqlSelector, definitionProvider)
vscode.languages.registerReferenceProvider(sqlSelector, referenceProvider)
vscode.languages.registerHoverProvider(sqlSelector, hoverProvider)
vscode.languages.registerCompletionItemProvider(sqlSelector, completionProvider, ".", "'", '"')
vscode.languages.registerRenameProvider(sqlSelector, renameProvider)

// Diagnostics triggered on:
vscode.workspace.onDidChangeTextDocument → validateDocumentDebounced (500ms)
vscode.workspace.onDidOpenTextDocument → validateDocument
vscode.window.onDidChangeActiveTextEditor → validateDocument
```

---

## Real Test Models Used for Debugging

### `stg_mx_operations__economic_concepts_operations.sql`
- Creates: `mx_economic_concepts_operations` (TEMPORARY LIVE VIEW)
- **Has `/* */` block comment between `AS (` and `WITH`** — this was the trigger for Bug 3
- Selects from external: `production.mx_operations.operative_operations`, `production.mx_operations.economic_concepts_operations`
- 4 CTEs: `id_duplicated_cancelled_op`, `id_cancelled_op`, `id_cancelled_op_netsuite`, `id_duplicated_cancelled_ec`
- Final SELECT: ~30 columns including `operation_uuid`, `file_date`, `invoice_type`, `invoice_amount`, etc.

### `int_mx_lt_all_transactions.sql`
- `economic_concepts_operations` CTE (lines 95-151) references `LIVE.mx_economic_concepts_operations`
- Comment lines (`--keys`, `--`) interspersed with column references → triggered Bug 2
- All columns in this CTE were false-positive "not found" due to Bug 3

---

## Build

```bash
# From C:\Users\Sergio\repos\dlt-sql-indexer
npx tsc -p ./ --noEmit   # type-check only
npx tsc -p ./            # compile to out/
npx vsce package         # produce .vsix
```

Zero TypeScript errors expected after all fixes.
