import {
  ModelKind,
  ModelLayer,
  ModelReference,
  CteDefinition,
  ColumnDefinition,
} from "./types";
import type { RawCteBlock } from "./schema/types";
import { extractCteBlocks } from "./schema/cteAnalyzer";

/** Result of parsing a single SQL file */
export interface ParseResult {
  modelName: string | null;
  kind: ModelKind;
  definitionLine: number;
  references: ModelReference[];
  ctes: CteDefinition[];
  columns: ColumnDefinition[];
  comment?: string;
  /** Raw CTE blocks for schema resolution */
  rawCteBlocks?: RawCteBlock[];
  /** Raw final SELECT body for schema resolution */
  rawSelectBody?: string;
}

// ── CREATE statement patterns ────────────────────────────────────────────────
// All observed variants in the codebase:
//   CREATE TEMPORARY LIVE VIEW <name>                  (754 files)
//   CREATE TEMPORARY LIVE TABLE <name>                 (544 files)
//   CREATE OR REFRESH LIVE TABLE <name>                (174 files)
//   CREATE OR REFRESH TEMPORARY LIVE TABLE <name>      (84 files)
//   CREATE OR REFRESH MATERIALIZED VIEW <name>         (21 files)
//   CREATE OR REPLACE MATERIALIZED VIEW <name>         (4 files)
//   CREATE OR REPLACE LIVE TABLE <name>                (1 file)
//   CREATE [OR REFRESH|REPLACE] STREAMING TABLE <name> (possible)

const CREATE_PATTERNS: { pattern: RegExp; kind: ModelKind }[] = [
  // Materialized views (CREATE OR REFRESH/REPLACE MATERIALIZED VIEW)
  {
    pattern: /CREATE\s+OR\s+(?:REFRESH|REPLACE)\s+MATERIALIZED\s+VIEW\s+(\w+)/i,
    kind: "materialized_view",
  },
  // Streaming tables
  {
    pattern: /CREATE\s+(?:OR\s+(?:REFRESH|REPLACE)\s+)?(?:TEMPORARY\s+)?STREAMING\s+TABLE\s+(\w+)/i,
    kind: "streaming_table",
  },
  // LIVE TABLE — all variants (with or without OR REFRESH/REPLACE, with or without TEMPORARY)
  {
    pattern: /CREATE\s+(?:OR\s+(?:REFRESH|REPLACE)\s+)?(?:TEMPORARY\s+)?LIVE\s+TABLE\s+(\w+)/i,
    kind: "live_table",
  },
  // LIVE VIEW — all variants
  {
    pattern: /CREATE\s+(?:OR\s+(?:REFRESH|REPLACE)\s+)?(?:TEMPORARY\s+)?LIVE\s+VIEW\s+(\w+)/i,
    kind: "live_view",
  },
];

// ── Reference patterns ───────────────────────────────────────────────────────

/** LIVE.model_name references */
const LIVE_REF = /\bLIVE\.(\w+)\b/g;

/** ${catalog}.schema.table references (foreign keys, etc.) */
const CATALOG_REF =
  /\$\{catalog\}\.(\w+)\.(\w+)/g;

/** dbt {{ ref('model_name') }} */
const DBT_REF = /\{\{\s*ref\(\s*['"](\w+)['"]\s*\)\s*\}\}/g;

/** dbt {{ source('source_name', 'table_name') }} */
const DBT_SOURCE =
  /\{\{\s*source\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*\)\s*\}\}/g;

// ── CTE pattern ──────────────────────────────────────────────────────────────

const CTE_PATTERN = /\b(\w+)\s+AS\s*\(/gi;

// ── Column definitions in materialized views ─────────────────────────────────

const COLUMN_DEF =
  /^\s+(\w+)\s+(STRING|INT|BIGINT|DECIMAL\([^)]+\)|DOUBLE|FLOAT|BOOLEAN|DATE|TIMESTAMP|ARRAY<[^>]+>|MAP<[^>]+>)(?:\s+(NOT\s+NULL))?(?:\s+COMMENT\s+'([^']*)')?/gim;

// ── Table-level COMMENT ──────────────────────────────────────────────────────

const TABLE_COMMENT = /\bCOMMENT\s+"([^"]*)"/i;

/**
 * Parse a SQL file and extract model metadata.
 */
export function parseSqlFile(content: string, filePath: string): ParseResult {
  const lines = content.split("\n");

  // 1. Find the CREATE statement
  let modelName: string | null = null;
  let kind: ModelKind = "unknown";
  let definitionLine = 0;

  for (let i = 0; i < lines.length; i++) {
    for (const { pattern, kind: k } of CREATE_PATTERNS) {
      const match = pattern.exec(lines[i]);
      if (match) {
        modelName = match[1];
        kind = k;
        definitionLine = i;
        break;
      }
    }
    if (modelName) break;
  }

  // 2. Extract references
  const references: ModelReference[] = [];
  const seenRefs = new Set<string>();

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Skip comment-only lines
    if (line.trimStart().startsWith("--")) {
      // But check for the path-comment pattern: "-- models/staging/..."
      // These are just documentation, skip them
      continue;
    }

    // LIVE.xxx references
    let match: RegExpExecArray | null;
    LIVE_REF.lastIndex = 0;
    while ((match = LIVE_REF.exec(line)) !== null) {
      const refName = match[1];
      const key = `live:${refName}`;
      if (!seenRefs.has(key)) {
        seenRefs.add(key);
        references.push({
          name: refName,
          type: "live",
          line: i,
          character: match.index,
        });
      }
    }

    // ${catalog}.schema.table references
    CATALOG_REF.lastIndex = 0;
    while ((match = CATALOG_REF.exec(line)) !== null) {
      const refName = match[2];
      const key = `catalog:${match[1]}.${refName}`;
      if (!seenRefs.has(key)) {
        seenRefs.add(key);
        references.push({
          name: refName,
          type: "catalog",
          line: i,
          character: match.index,
          sourceName: match[1],
        });
      }
    }

    // {{ ref('...') }}
    DBT_REF.lastIndex = 0;
    while ((match = DBT_REF.exec(line)) !== null) {
      const refName = match[1];
      const key = `ref:${refName}`;
      if (!seenRefs.has(key)) {
        seenRefs.add(key);
        references.push({
          name: refName,
          type: "ref",
          line: i,
          character: match.index,
        });
      }
    }

    // {{ source('...', '...') }}
    DBT_SOURCE.lastIndex = 0;
    while ((match = DBT_SOURCE.exec(line)) !== null) {
      const key = `source:${match[1]}.${match[2]}`;
      if (!seenRefs.has(key)) {
        seenRefs.add(key);
        references.push({
          name: match[2],
          type: "source",
          line: i,
          character: match.index,
          sourceName: match[1],
        });
      }
    }
  }

  // 3. Extract CTEs
  const ctes: CteDefinition[] = [];
  let inWithBlock = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trimStart();

    if (trimmed.startsWith("--")) continue;

    // Detect WITH keyword (start of CTE block)
    if (/^\s*WITH\b/i.test(line)) {
      inWithBlock = true;
      continue;
    }

    if (inWithBlock) {
      CTE_PATTERN.lastIndex = 0;
      const cteMatch = CTE_PATTERN.exec(line);
      if (cteMatch) {
        const cteName = cteMatch[1].toLowerCase();
        // Exclude SQL keywords that look like CTEs
        const keywords = new Set([
          "select", "from", "where", "group", "order", "having",
          "limit", "union", "insert", "update", "delete", "create",
          "case", "when", "then", "else", "end", "and", "or", "not",
          "in", "exists", "between", "like", "is", "null", "join",
          "inner", "left", "right", "full", "outer", "cross", "on",
        ]);
        if (!keywords.has(cteName)) {
          ctes.push({
            name: cteMatch[1],
            line: i,
            character: line.indexOf(cteMatch[1]),
          });
        }
      }

      // End of CTE block: final SELECT
      if (/^\s+SELECT\b/i.test(line) && !CTE_PATTERN.test(line)) {
        // Check if this is the main SELECT (not inside a CTE)
        // Simple heuristic: if we haven't seen a closing paren for a CTE recently
      }
    }
  }

  // 4. Extract column definitions (from materialized views)
  const columns: ColumnDefinition[] = [];
  if (kind === "materialized_view") {
    COLUMN_DEF.lastIndex = 0;
    let colMatch: RegExpExecArray | null;
    while ((colMatch = COLUMN_DEF.exec(content)) !== null) {
      // Skip CONSTRAINT lines
      if (colMatch[1].toUpperCase() === "CONSTRAINT") continue;
      columns.push({
        name: colMatch[1],
        dataType: colMatch[2],
        isNullable: !colMatch[3],
        comment: colMatch[4]?.replace(/\s+/g, " "),
      });
    }
  }

  // 5. Table-level comment
  const commentMatch = TABLE_COMMENT.exec(content);
  const comment = commentMatch?.[1];

  // 6. Extract raw CTE blocks and final SELECT body for schema resolution
  let rawCteBlocks: RawCteBlock[] | undefined;
  let rawSelectBody: string | undefined;
  try {
    const cteResult = extractCteBlocks(content);
    if (cteResult.ctes.length > 0) {
      rawCteBlocks = cteResult.ctes;
    }
    if (cteResult.finalSelectBody) {
      rawSelectBody = cteResult.finalSelectBody;
    }
  } catch {
    // CTE extraction is best-effort — don't fail the whole parse
  }

  return {
    modelName,
    kind,
    definitionLine,
    references,
    ctes,
    columns,
    comment,
    rawCteBlocks,
    rawSelectBody,
  };
}

/**
 * Infer the model layer from its file path.
 */
export function inferLayer(relativePath: string): ModelLayer {
  const normalized = relativePath.replace(/\\/g, "/").toLowerCase();
  if (normalized.startsWith("staging/") || normalized.includes("/staging/"))
    return "staging";
  if (
    normalized.startsWith("intermediate/") ||
    normalized.includes("/intermediate/")
  )
    return "intermediate";
  if (normalized.startsWith("marts/") || normalized.includes("/marts/"))
    return "marts";
  if (normalized.startsWith("dbt/") || normalized.includes("/dbt/"))
    return "dbt";
  return "unknown";
}

/**
 * Infer country code from the file path.
 */
export function inferCountry(relativePath: string): string | undefined {
  const normalized = relativePath.replace(/\\/g, "/").toLowerCase();
  const match = normalized.match(/\/(?:br|mx|co)\//);
  if (match) return match[0].replace(/\//g, "");

  // Also check filename pattern like stg_br_xxx or int_mx_xxx
  const fileMatch = normalized.match(/(?:stg|int|mart)_(br|mx|co)_/);
  return fileMatch?.[1];
}
