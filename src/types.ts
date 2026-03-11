import * as vscode from "vscode";
import type { ResolvedColumn, RawCteBlock } from "./schema/types";

/** The layer a model belongs to in the medallion architecture */
export type ModelLayer = "staging" | "intermediate" | "marts" | "dbt" | "unknown";

/** The type of DLT object created */
export type ModelKind =
  | "live_table"
  | "live_view"
  | "materialized_view"
  | "streaming_table"
  | "unknown";

/** A CTE defined within a model's SQL */
export interface CteDefinition {
  name: string;
  /** Line number (0-based) where the CTE starts */
  line: number;
  /** Character offset within the line */
  character: number;
}

/** A reference from one model to another */
export interface ModelReference {
  /** The referenced model/table name */
  name: string;
  /** How it was referenced */
  type: "live" | "ref" | "source" | "catalog";
  /** Location in the file */
  line: number;
  character: number;
  /** For source refs: the source name */
  sourceName?: string;
}

/** Column definition extracted from materialized views or SELECT lists */
export interface ColumnDefinition {
  name: string;
  dataType?: string;
  comment?: string;
  isNullable?: boolean;
}

/** Metadata extracted from companion .yml files */
export interface YmlMetadata {
  description?: string;
  columns?: {
    name: string;
    description?: string;
  }[];
  alias?: string;
  tags?: string[];
}

/** A fully indexed SQL model */
export interface IndexedModel {
  /** Model name as defined in CREATE statement */
  name: string;
  /** Absolute file path */
  filePath: string;
  /** URI for VS Code */
  uri: vscode.Uri;
  /** Line where the CREATE statement is */
  definitionLine: number;
  /** Layer in the medallion architecture */
  layer: ModelLayer;
  /** Type of DLT object */
  kind: ModelKind;
  /** Other models this model references */
  references: ModelReference[];
  /** CTEs defined in this model */
  ctes: CteDefinition[];
  /** Columns (from materialized view definitions or yml) */
  columns: ColumnDefinition[];
  /** Metadata from companion .yml file */
  yml?: YmlMetadata;
  /** Country code if detected from path (br, mx, co) */
  country?: string;
  /** Relative path from models root */
  relativePath: string;

  // ── Schema resolution fields ───────────────────────────────────────────
  /** Columns resolved through the dependency chain (populated by SchemaResolver) */
  resolvedColumns?: ResolvedColumn[];
  /** Resolution status for cycle detection */
  resolutionStatus?: "pending" | "resolving" | "resolved" | "error";
  /** Raw CTE blocks for schema resolution (populated by parser) */
  rawCteBlocks?: RawCteBlock[];
  /** Raw final SELECT body for schema resolution (populated by parser) */
  rawSelectBody?: string;
}

/** The full workspace index */
export interface ModelIndex {
  /** All models keyed by name (lowercase) */
  models: Map<string, IndexedModel>;
  /** Reverse lookup: which models reference a given model name */
  referencedBy: Map<string, Set<string>>;
  /** File path to model name mapping */
  fileToModel: Map<string, string>;
}
