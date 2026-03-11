/** Confidence level for a resolved column's type */
export type TypeConfidence = "known" | "inferred" | "unknown";

/** A column with resolved type information */
export interface ResolvedColumn {
  /** Column name (or alias) */
  name: string;
  /** SQL data type (e.g., STRING, DECIMAL(17,2), DATE) */
  dataType?: string;
  /** Where was this column's type determined */
  confidence: TypeConfidence;
  /** Which model or CTE this column originates from */
  source?: string;
  /** Comment from materialized view or yml */
  comment?: string;
  /** Whether NOT NULL was declared */
  isNullable?: boolean;
}

/** A parsed item from a SELECT list */
export interface SelectItem {
  /** The raw expression text (e.g., "t.company_uuid", "COALESCE(a, b)") */
  expression: string;
  /** The alias if AS was used, or the inferred column name */
  alias?: string;
  /** True if this is SELECT * */
  isStar: boolean;
  /** If qualified star like "t.*", the qualifier alias */
  starQualifier?: string;
  /** If a qualified column ref (alias.col), the table alias */
  tableAlias?: string;
  /** The bare column name (without table alias) */
  columnName?: string;
  /** Line offset within the CTE/SELECT body (for diagnostics) */
  lineOffset?: number;
  /** Character offset within the line */
  charOffset?: number;
}

/** A raw CTE block extracted from the SQL */
export interface RawCteBlock {
  /** CTE name */
  name: string;
  /** Raw SQL body (content between the parentheses) */
  body: string;
  /** Line number in the file where this CTE starts */
  startLine: number;
}

/** A source table/CTE referenced in a FROM/JOIN clause */
export interface FromSource {
  /** The source name (CTE name, or model name from LIVE.xxx) */
  sourceName: string;
  /** The alias used in the query (or the source name if no alias) */
  alias: string;
  /** Whether this is a LIVE.xxx reference */
  isLiveRef: boolean;
  /** Whether this is a dbt ref() */
  isDbtRef: boolean;
}

/** Resolved scope for a CTE — its available columns and source mapping */
export interface CteScope {
  /** CTE name */
  name: string;
  /** Resolved columns for this CTE */
  columns: ResolvedColumn[];
  /** Sources referenced in this CTE's FROM/JOIN */
  sources: FromSource[];
  /** Whether this CTE's columns could be fully resolved */
  fullyResolved: boolean;
}
