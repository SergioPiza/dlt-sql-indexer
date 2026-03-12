import { IndexedModel } from "../types";
import { ResolvedColumn, CteScope, FromSource } from "./types";
import { parseSelectList } from "./columnExtractor";
import { extractCteBlocks, parseFromClause } from "./cteAnalyzer";
import { inferType } from "./typeInference";

/** Callback to look up a model by name */
type ModelLookup = (name: string) => IndexedModel | undefined;

/**
 * Resolves column schemas for all models in topological order.
 * Propagates columns through the dependency chain:
 *   staging (explicit SELECT) -> intermediate (CTEs + joins) -> marts (materialized views)
 */
export class SchemaResolver {
  private modelLookup: ModelLookup;
  private log: (msg: string) => void;

  constructor(modelLookup: ModelLookup, log?: (msg: string) => void) {
    this.modelLookup = modelLookup;
    this.log = log || (() => {});
  }

  /**
   * Resolve schemas for all models.
   * Must be called after the index is fully built.
   * Yields to the event loop every 50 models to avoid blocking VS Code.
   */
  async resolveAll(models: IndexedModel[]): Promise<void> {
    // Reset all resolution statuses
    for (const model of models) {
      model.resolutionStatus = "pending";
      // Keep existing columns for materialized views (they have explicit types)
      if (model.kind !== "materialized_view") {
        model.resolvedColumns = undefined;
      }
    }

    // Topological sort: resolve dependencies first
    const sorted = this.topologicalSort(models);

    let resolved = 0;
    let errors = 0;

    for (let i = 0; i < sorted.length; i++) {
      // Yield to event loop every 50 models to avoid blocking VS Code
      if (i > 0 && i % 50 === 0) {
        await new Promise<void>((resolve) => setImmediate(resolve));
      }

      const model = sorted[i];
      try {
        this.resolveModel(model);
        if (model.resolutionStatus === "resolved") resolved++;
      } catch (err) {
        model.resolutionStatus = "error";
        errors++;
        this.log(`Error resolving ${model.name}: ${err}`);
      }
    }

    this.log(
      `Schema resolution complete: ${resolved} resolved, ${errors} errors, ${models.length - resolved - errors} skipped`
    );
  }

  /**
   * Resolve a single model. Called in dependency order.
   */
  resolveModel(model: IndexedModel): void {
    if (model.resolutionStatus === "resolved") return;
    if (model.resolutionStatus === "resolving") {
      // Circular dependency detected
      model.resolutionStatus = "error";
      this.log(`Circular dependency detected for ${model.name}`);
      return;
    }

    model.resolutionStatus = "resolving";

    // Materialized views already have explicit columns with types
    if (model.kind === "materialized_view" && model.columns.length > 0) {
      model.resolvedColumns = model.columns.map((col) => ({
        name: col.name,
        dataType: col.dataType,
        confidence: "known" as const,
        source: model.name,
        comment: col.comment,
        isNullable: col.isNullable,
      }));
      model.resolutionStatus = "resolved";
      return;
    }

    // Need raw SQL bodies from the parser
    if (!model.rawSelectBody && (!model.rawCteBlocks || model.rawCteBlocks.length === 0)) {
      // No parseable SQL body — try extracting from the raw content
      model.resolutionStatus = "error";
      return;
    }

    // Resolve CTEs first
    const cteScopes = new Map<string, CteScope>();

    if (model.rawCteBlocks) {
      for (const cte of model.rawCteBlocks) {
        const scope = this.resolveCte(cte.name, cte.body, model, cteScopes);
        cteScopes.set(cte.name.toLowerCase(), scope);
      }
    }

    // Store per-CTE resolved columns and sources on the model
    if (cteScopes.size > 0) {
      model.resolvedCteColumns = new Map();
      model.resolvedCteSources = new Map();
      for (const [cteName, scope] of cteScopes) {
        model.resolvedCteColumns.set(cteName, scope.columns);
        model.resolvedCteSources.set(cteName, scope.sources);
      }
    }

    // Build alias → source mapping from the final SELECT body only.
    // CTE-internal aliases are scoped to their CTE and shouldn't leak
    // into the global map (multiple CTEs often reuse the same alias letter).
    const aliasToSource = new Map<string, FromSource>();

    // Resolve the final SELECT body
    if (model.rawSelectBody) {
      const finalSources = parseFromClause(model.rawSelectBody);
      for (const src of finalSources) {
        aliasToSource.set(src.alias.toLowerCase(), src);
      }
      const finalColumns = this.resolveSelectBody(
        model.rawSelectBody,
        model,
        cteScopes
      );
      model.resolvedColumns = finalColumns;
    }

    if (aliasToSource.size > 0) {
      model.aliasToSource = aliasToSource;
    }

    // Ensure all final columns have a source (default to this model's name).
    // Columns from external tables (non-LIVE, non-dbt) won't have source set
    // by resolveSelectItemColumn, so we fill it in here.
    if (model.resolvedColumns) {
      for (const col of model.resolvedColumns) {
        if (!col.source) {
          col.source = model.name;
        }
      }
    }

    model.resolutionStatus = "resolved";
  }

  /**
   * Resolve columns for a single CTE.
   */
  private resolveCte(
    cteName: string,
    cteBody: string,
    model: IndexedModel,
    previousScopes: Map<string, CteScope>
  ): CteScope {
    const sources = parseFromClause(cteBody);
    const aliasColumns = this.buildAliasColumnMap(sources, model, previousScopes);

    const selectItems = parseSelectList(cteBody);
    const columns: ResolvedColumn[] = [];
    let fullyResolved = true;

    for (const item of selectItems) {
      if (item.isStar) {
        // Expand SELECT * or alias.*
        const expanded = this.expandStar(item.starQualifier, aliasColumns);
        if (expanded.length === 0) fullyResolved = false;
        columns.push(...expanded);
      } else {
        const col = this.resolveSelectItemColumn(item, aliasColumns);
        if (!col.dataType) fullyResolved = false;
        columns.push(col);
      }
    }

    return {
      name: cteName,
      columns,
      sources,
      fullyResolved,
    };
  }

  /**
   * Resolve columns for the final SELECT body.
   */
  private resolveSelectBody(
    selectBody: string,
    model: IndexedModel,
    cteScopes: Map<string, CteScope>
  ): ResolvedColumn[] {
    const sources = parseFromClause(selectBody);
    const aliasColumns = this.buildAliasColumnMap(sources, model, cteScopes);

    const selectItems = parseSelectList(selectBody);
    const columns: ResolvedColumn[] = [];

    for (const item of selectItems) {
      if (item.isStar) {
        columns.push(...this.expandStar(item.starQualifier, aliasColumns));
      } else {
        columns.push(this.resolveSelectItemColumn(item, aliasColumns));
      }
    }

    return columns;
  }

  /**
   * Build a map of alias -> ResolvedColumn[] for the FROM/JOIN sources.
   */
  private buildAliasColumnMap(
    sources: FromSource[],
    _model: IndexedModel,
    cteScopes: Map<string, CteScope>
  ): Map<string, ResolvedColumn[]> {
    const aliasMap = new Map<string, ResolvedColumn[]>();

    for (const source of sources) {
      const sourceNameLower = source.sourceName.toLowerCase();

      // Check if it's a CTE
      const cteScope = cteScopes.get(sourceNameLower);
      if (cteScope) {
        aliasMap.set(source.alias.toLowerCase(), cteScope.columns);
        continue;
      }

      // Check if it's a LIVE.model reference
      if (source.isLiveRef || source.isDbtRef) {
        const refModel = this.modelLookup(source.sourceName);
        if (refModel) {
          // Ensure the referenced model is resolved first
          if (refModel.resolutionStatus === "pending") {
            this.resolveModel(refModel);
          }
          if (refModel.resolvedColumns) {
            aliasMap.set(
              source.alias.toLowerCase(),
              refModel.resolvedColumns
            );
          }
        }
        continue;
      }

      // External source — no columns known
      aliasMap.set(source.alias.toLowerCase(), []);
    }

    return aliasMap;
  }

  /**
   * Expand SELECT * or alias.* into individual columns.
   */
  private expandStar(
    qualifier: string | undefined,
    aliasColumns: Map<string, ResolvedColumn[]>
  ): ResolvedColumn[] {
    if (qualifier) {
      // alias.* — expand only that alias
      const cols = aliasColumns.get(qualifier.toLowerCase());
      return cols ? [...cols] : [];
    }

    // SELECT * — expand all sources in order
    const all: ResolvedColumn[] = [];
    for (const [, cols] of aliasColumns) {
      all.push(...cols);
    }
    return all;
  }

  /**
   * Resolve a single SELECT item to a ResolvedColumn.
   */
  private resolveSelectItemColumn(
    item: ReturnType<typeof parseSelectList>[0],
    aliasColumns: Map<string, ResolvedColumn[]>
  ): ResolvedColumn {
    const name = item.alias || item.columnName || item.expression;

    // Try to infer the type from the expression
    const dataType = inferType(item.expression, aliasColumns);

    // Determine confidence
    let confidence: ResolvedColumn["confidence"] = "unknown";
    if (dataType) {
      // If we got a type from a known source column, it's "inferred"
      // If it's from CAST or a literal, it's also "inferred"
      confidence = "inferred";
    }

    // If this is a simple column reference, try to find source info
    let source: string | undefined;
    if (item.tableAlias && item.columnName) {
      const cols = aliasColumns.get(item.tableAlias.toLowerCase());
      const srcCol = cols?.find(
        (c) => c.name.toLowerCase() === item.columnName!.toLowerCase()
      );
      if (srcCol) {
        if (srcCol.confidence === "known") confidence = "known";
        source = srcCol.source;
        return {
          name,
          dataType: dataType || srcCol.dataType,
          confidence,
          source,
          comment: srcCol.comment,
          isNullable: srcCol.isNullable,
        };
      }
    }

    // Bare column — search all aliases
    if (item.columnName && !item.tableAlias) {
      for (const [, cols] of aliasColumns) {
        const srcCol = cols.find(
          (c) => c.name.toLowerCase() === item.columnName!.toLowerCase()
        );
        if (srcCol) {
          if (srcCol.confidence === "known") confidence = "known";
          return {
            name,
            dataType: dataType || srcCol.dataType,
            confidence,
            source: srcCol.source,
            comment: srcCol.comment,
            isNullable: srcCol.isNullable,
          };
        }
      }
    }

    return { name, dataType, confidence };
  }

  /**
   * Topological sort: models with no dependencies first.
   */
  private topologicalSort(models: IndexedModel[]): IndexedModel[] {
    const modelMap = new Map<string, IndexedModel>();
    for (const m of models) {
      modelMap.set(m.name.toLowerCase(), m);
    }

    const visited = new Set<string>();
    const result: IndexedModel[] = [];

    const visit = (model: IndexedModel) => {
      const key = model.name.toLowerCase();
      if (visited.has(key)) return;
      visited.add(key);

      // Visit dependencies first
      for (const ref of model.references) {
        const dep = modelMap.get(ref.name.toLowerCase());
        if (dep) visit(dep);
      }

      result.push(model);
    };

    for (const model of models) {
      visit(model);
    }

    return result;
  }
}
