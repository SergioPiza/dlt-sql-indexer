import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";
import { parseFromClause, extractCteBlocks, splitUnionAll } from "../schema/cteAnalyzer";
import { parseSelectList } from "../schema/columnExtractor";
import { ResolvedColumn } from "../schema/types";

/** Language IDs we consider as SQL (dbt/sqlfluff extensions may change the ID) */
const SQL_LANGUAGE_IDS = new Set([
  "sql",
  "jinja-sql",
  "sql-bigquery",
  "sql-databricks",
  "databricks-sql",
]);

/**
 * Provides real-time diagnostics (warning squiggles) for column references
 * that don't exist in the source model or CTE.
 */
export class DltDiagnosticProvider {
  private diagnosticCollection: vscode.DiagnosticCollection;
  private indexer: DltModelIndexer;
  private outputChannel: vscode.OutputChannel;
  private debounceTimers: Map<string, ReturnType<typeof setTimeout>> =
    new Map();

  constructor(
    diagnosticCollection: vscode.DiagnosticCollection,
    indexer: DltModelIndexer,
    outputChannel: vscode.OutputChannel
  ) {
    this.diagnosticCollection = diagnosticCollection;
    this.indexer = indexer;
    this.outputChannel = outputChannel;
  }

  /**
   * Validate a document and update its diagnostics (debounced for typing).
   */
  validateDocumentDebounced(document: vscode.TextDocument): void {
    const key = document.uri.toString();
    const existing = this.debounceTimers.get(key);
    if (existing) clearTimeout(existing);

    this.debounceTimers.set(
      key,
      setTimeout(() => {
        this.debounceTimers.delete(key);
        this.validateDocument(document);
      }, 500)
    );
  }

  /**
   * Validate a document and update its diagnostics immediately.
   */
  validateDocument(document: vscode.TextDocument): void {
    // Guard BEFORE any logging — the output channel is itself a VS Code document,
    // so logging here for non-SQL docs triggers another validateDocument call → infinite loop.
    if (!SQL_LANGUAGE_IDS.has(document.languageId) && !document.fileName.endsWith(".sql")) {
      return;
    }

    this.outputChannel.appendLine(
      `[Diag] validateDocument called: langId=${document.languageId}, file=${document.fileName}`
    );

    // Check if diagnostics are enabled
    const config = vscode.workspace.getConfiguration("dltSqlIndexer");
    if (!config.get<boolean>("enableDiagnostics", true)) {
      this.outputChannel.appendLine(`[Diag] Skipped: diagnostics disabled in config`);
      this.diagnosticCollection.delete(document.uri);
      return;
    }

    const model = this.indexer.getModelByFile(document.uri.fsPath);
    if (!model) {
      this.outputChannel.appendLine(`[Diag] Skipped: no model found for ${document.uri.fsPath}`);
      this.diagnosticCollection.delete(document.uri);
      return;
    }

    const diagnostics: vscode.Diagnostic[] = [];
    const text = document.getText();
    const lines = text.split("\n");

    // 0. Check for duplicate model names
    const duplicateFiles = this.indexer.getDuplicateFiles(model.name);
    if (duplicateFiles.length > 0) {
      const otherFiles = duplicateFiles
        .filter((f) => f !== document.uri.fsPath)
        .map((f) => {
          const ws = vscode.workspace.getWorkspaceFolder(document.uri);
          if (ws) {
            const rel = f.replace(ws.uri.fsPath, "").replace(/\\/g, "/");
            return rel.startsWith("/") ? rel.slice(1) : rel;
          }
          return f;
        });
      if (otherFiles.length > 0) {
        const line = model.definitionLine;
        const lineText = lines[line] || "";
        const range = new vscode.Range(line, 0, line, lineText.length);
        diagnostics.push(
          new vscode.Diagnostic(
            range,
            `Duplicate model name '${model.name}' \u2014 also defined in: ${otherFiles.join(", ")}`,
            vscode.DiagnosticSeverity.Error
          )
        );
      }
    }

    // 1. Validate LIVE.model references — does the model exist?
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.trimStart().startsWith("--")) continue;

      // Strip inline comments
      const commentIdx = line.indexOf("--");
      const codePart = commentIdx >= 0 ? line.substring(0, commentIdx) : line;

      const liveRefPattern = /\bLIVE\.(\w+)\b/g;
      let match: RegExpExecArray | null;
      while ((match = liveRefPattern.exec(codePart)) !== null) {
        const refName = match[1];
        const refModel = this.indexer.getModel(refName);

        if (!refModel) {
          const range = new vscode.Range(
            i,
            match.index + 5,
            i,
            match.index + 5 + refName.length
          );
          diagnostics.push(
            new vscode.Diagnostic(
              range,
              `Model '${refName}' not found in the index`,
              vscode.DiagnosticSeverity.Information
            )
          );
        }
      }
    }

    // 2. Build alias -> resolved columns map from this file's structure,
    //    then validate qualified column references (alias.column).
    const { aliasMap, selectBodies } = this.buildFileAliasMap(document);
    this.outputChannel.appendLine(
      `[Diag] ${model.name}: aliasMap has ${aliasMap.size} entries: ${Array.from(aliasMap.entries()).map(([k, v]) => `${k}(${v.length}cols)`).join(", ")}`
    );

    if (aliasMap.size > 0) {
      this.validateColumnReferences(aliasMap, lines, diagnostics);
      this.validateBareColumnReferences(aliasMap, selectBodies, lines, diagnostics);
    }

    this.outputChannel.appendLine(
      `[Diag] ${model.name}: ${diagnostics.length} diagnostics`
    );

    this.diagnosticCollection.set(document.uri, diagnostics);
  }

  /**
   * Build alias -> ResolvedColumn[] from the file's FROM/JOIN/CTE structure.
   * Also returns per-scope SELECT bodies for bare column validation.
   */
  private buildFileAliasMap(
    document: vscode.TextDocument
  ): { aliasMap: Map<string, ResolvedColumn[]>; selectBodies: { body: string; startLine: number; scopeAliases: Map<string, ResolvedColumn[]> }[] } {
    const aliasMap = new Map<string, ResolvedColumn[]>();
    const selectBodies: { body: string; startLine: number; scopeAliases: Map<string, ResolvedColumn[]> }[] = [];
    const text = document.getText();

    // Try to extract CTE blocks + final SELECT body
    let cteBlocks: { name: string; body: string; startLine: number }[] = [];
    let finalSelectBody = "";
    let finalStartLine = 0;

    try {
      const extracted = extractCteBlocks(text);
      cteBlocks = extracted.ctes;
      finalSelectBody = extracted.finalSelectBody;
      finalStartLine = extracted.finalSelectStartLine ?? 0;
    } catch (err) {
      this.outputChannel.appendLine(
        `[Diag] extractCteBlocks failed: ${err}`
      );
      finalSelectBody = text;
    }

    this.outputChannel.appendLine(
      `[Diag] ${document.fileName}: ${cteBlocks.length} CTEs, finalBody ${finalSelectBody.length} chars`
    );

    const currentModel = this.indexer.getModelByFile(document.uri.fsPath);

    // Process each CTE's FROM sources (split UNION ALL into separate scopes)
    for (const cte of cteBlocks) {
      const unionSegments = splitUnionAll(cte.body);
      for (const seg of unionSegments) {
        const segAliases = new Map<string, ResolvedColumn[]>();
        const sources = parseFromClause(seg.segment);
        for (const source of sources) {
          this.addSourceToAliasMap(source, aliasMap);
          this.addSourceToAliasMap(source, segAliases);
        }
        selectBodies.push({ body: seg.segment, startLine: cte.startLine + seg.lineOffset, scopeAliases: segAliases });
      }

      // Register the CTE name itself with its resolved output columns
      const cteCols = currentModel?.resolvedCteColumns?.get(cte.name.toLowerCase());
      if (cteCols && cteCols.length > 0) {
        aliasMap.set(cte.name.toLowerCase(), cteCols);
      }
    }

    // Process the final SELECT body's FROM/JOIN (split UNION ALL into separate scopes)
    if (finalSelectBody) {
      const unionSegments = splitUnionAll(finalSelectBody);
      for (const seg of unionSegments) {
        const segAliases = new Map<string, ResolvedColumn[]>();
        const sources = parseFromClause(seg.segment);
        for (const source of sources) {
          this.addSourceToAliasMap(source, aliasMap);
          this.addSourceToAliasMap(source, segAliases);
        }
        // Also include CTE names as valid aliases in each final scope segment
        for (const cte of cteBlocks) {
          const cteCols = currentModel?.resolvedCteColumns?.get(cte.name.toLowerCase());
          if (cteCols && cteCols.length > 0) {
            segAliases.set(cte.name.toLowerCase(), cteCols);
          }
        }
        selectBodies.push({ body: seg.segment, startLine: finalStartLine + seg.lineOffset, scopeAliases: segAliases });
      }
    }

    return { aliasMap, selectBodies };
  }

  /**
   * Add a FROM/JOIN source to the alias map by resolving it to columns.
   */
  private addSourceToAliasMap(
    source: { sourceName: string; alias: string; isLiveRef: boolean; isDbtRef: boolean },
    aliasMap: Map<string, ResolvedColumn[]>
  ): void {
    const aliasKey = source.alias.toLowerCase();

    if (source.isLiveRef || source.isDbtRef) {
      const refModel = this.indexer.getModel(source.sourceName);
      if (refModel) {
        // Prefer resolvedColumns, fall back to explicit columns
        const cols = refModel.resolvedColumns || (refModel.columns.length > 0 ? refModel.columns.map(c => ({
          name: c.name,
          dataType: c.dataType,
          confidence: "known" as const,
          source: refModel.name,
          comment: c.comment,
          isNullable: c.isNullable,
        })) : undefined);
        if (cols && cols.length > 0) {
          aliasMap.set(aliasKey, cols);
        } else {
          this.outputChannel.appendLine(
            `[Diag]   alias '${source.alias}' -> model '${source.sourceName}': no columns (resolvedColumns=${refModel.resolvedColumns?.length ?? 'undef'}, columns=${refModel.columns.length})`
          );
        }
      }
      return;
    }

    // CTE/subquery reference: check if we already resolved this name
    const prevCols = aliasMap.get(source.sourceName.toLowerCase());
    if (prevCols) {
      aliasMap.set(aliasKey, prevCols);
    }
  }

  /**
   * Validate qualified column references (alias.column) against the alias map.
   */
  private validateColumnReferences(
    aliasMap: Map<string, ResolvedColumn[]>,
    lines: string[],
    diagnostics: vscode.Diagnostic[]
  ): void {
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.trimStart().startsWith("--")) continue;

      // Strip inline comments so we don't match inside "-- some.path"
      const commentIdx = line.indexOf("--");
      const codePart = commentIdx >= 0 ? line.substring(0, commentIdx) : line;

      const qualifiedRefPattern = /\b(\w+)\.(\w+)\b/g;
      let match: RegExpExecArray | null;

      while ((match = qualifiedRefPattern.exec(codePart)) !== null) {
        const alias = match[1];
        const colName = match[2];

        // Skip non-column patterns
        if (alias.toUpperCase() === "LIVE") continue;
        if (alias.startsWith("$")) continue;
        if (colName === "*") continue;
        // Skip file extensions (.sql, .py, .csv, etc.)
        if (/^(sql|py|csv|json|yml|yaml|txt|md|js|ts)$/i.test(colName)) continue;

        // Look up the alias in our map
        const columns = aliasMap.get(alias.toLowerCase());
        if (!columns || columns.length === 0) continue;

        const exists = columns.some(
          (c) => c.name.toLowerCase() === colName.toLowerCase()
        );
        if (!exists) {
          const range = new vscode.Range(
            i,
            match.index + alias.length + 1,
            i,
            match.index + alias.length + 1 + colName.length
          );
          diagnostics.push(
            new vscode.Diagnostic(
              range,
              `Column '${colName}' not found in '${alias}' (${columns.length} columns known)`,
              vscode.DiagnosticSeverity.Warning
            )
          );
        }
      }
    }
  }

  /**
   * Validate bare (unqualified) column references in SELECT lists.
   * Uses parseSelectList to extract column items, then checks bare names
   * against all columns available in the scope.
   */
  private validateBareColumnReferences(
    _globalAliasMap: Map<string, ResolvedColumn[]>,
    selectBodies: { body: string; startLine: number; scopeAliases: Map<string, ResolvedColumn[]> }[],
    lines: string[],
    diagnostics: vscode.Diagnostic[]
  ): void {
    for (const { body, startLine, scopeAliases } of selectBodies) {
      if (!body || scopeAliases.size === 0) continue;

      // Build union of all known column names in this scope
      const allKnownColumns = new Set<string>();
      for (const [, cols] of scopeAliases) {
        for (const col of cols) {
          allKnownColumns.add(col.name.toLowerCase());
        }
      }
      if (allKnownColumns.size === 0) continue;

      // Parse the SELECT list to get column items
      let selectItems: ReturnType<typeof parseSelectList>;
      try {
        selectItems = parseSelectList(body);
      } catch {
        continue;
      }

      for (const item of selectItems) {
        // Only check bare column references (no table alias, no star, has a columnName)
        if (item.isStar) continue;
        if (item.tableAlias) continue; // qualified refs handled by validateColumnReferences
        if (!item.columnName) continue; // complex expression — can't validate

        const colNameLower = item.columnName.toLowerCase();

        // Skip if the column exists in any source
        if (allKnownColumns.has(colNameLower)) continue;

        // Find the line in the document where this column reference appears
        // item.lineOffset is relative to the body; startLine is the body's offset in the document
        const searchName = item.columnName;
        const bodyLines = body.split("\n");
        const lineInBody = item.lineOffset ?? 0;

        // Search for the column name in the body starting from lineInBody
        let foundLine = -1;
        let foundCol = -1;
        for (let bl = lineInBody; bl < bodyLines.length; bl++) {
          const bline = bodyLines[bl];
          // Skip comment-only lines
          if (bline.trimStart().startsWith("--")) continue;
          // Strip inline comments before matching
          const commentIdx = bline.indexOf("--");
          const codePart = commentIdx >= 0 ? bline.substring(0, commentIdx) : bline;
          // Look for the bare column name as a whole word
          const pattern = new RegExp(`\\b${escapeRegex(searchName)}\\b`, "i");
          const m = pattern.exec(codePart);
          if (m) {
            foundLine = startLine + bl;
            foundCol = m.index;
            break;
          }
        }

        if (foundLine >= 0 && foundLine < lines.length) {
          const range = new vscode.Range(
            foundLine,
            foundCol,
            foundLine,
            foundCol + searchName.length
          );
          diagnostics.push(
            new vscode.Diagnostic(
              range,
              `Column '${searchName}' not found in any source (${allKnownColumns.size} columns known)`,
              vscode.DiagnosticSeverity.Warning
            )
          );
        }
      }
    }
  }

  /**
   * Clear diagnostics for a document.
   */
  clearDiagnostics(uri: vscode.Uri): void {
    this.diagnosticCollection.delete(uri);
  }

  /**
   * Clear all diagnostics.
   */
  clearAll(): void {
    this.diagnosticCollection.clear();
  }
}

/** Escape special regex characters in a string */
function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
