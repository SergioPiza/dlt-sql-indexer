import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";
import { extractCteBlocks, parseFromClause, splitUnionAll } from "../schema/cteAnalyzer";
import { ResolvedColumn } from "../schema/types";

/**
 * Autocomplete:
 * - Model names after LIVE. or inside ref('')
 * - Column names after alias. (when alias maps to a known model/CTE)
 * - Bare column names in SELECT/WHERE/ON/etc. (all columns from sources in scope)
 */
export class DltCompletionProvider implements vscode.CompletionItemProvider {
  constructor(private indexer: DltModelIndexer) {}

  provideCompletionItems(
    document: vscode.TextDocument,
    position: vscode.Position,
    _token: vscode.CancellationToken,
    _context: vscode.CompletionContext
  ): vscode.CompletionItem[] | undefined {
    const linePrefix = document
      .lineAt(position)
      .text.substring(0, position.character);

    // Skip if we're in a comment
    if (linePrefix.trimStart().startsWith("--")) return undefined;
    const commentIdx = linePrefix.indexOf("--");
    if (commentIdx >= 0 && position.character > commentIdx + 2) return undefined;

    // Trigger after "LIVE."
    const isLiveRef = /\bLIVE\.\w*$/i.test(linePrefix);

    // Trigger inside ref('...')
    const isRef = /\{\{\s*ref\(\s*['"][\w]*$/i.test(linePrefix);

    if (isLiveRef || isRef) {
      return this.completeModelNames();
    }

    // Trigger column completion after "alias." (not LIVE.)
    const aliasMatch = linePrefix.match(/\b(\w+)\.\w*$/);
    if (aliasMatch && aliasMatch[1].toUpperCase() !== "LIVE") {
      return this.completeQualifiedColumns(aliasMatch[1], document, position);
    }

    // Bare column completion: suggest all columns from sources in the current scope
    // Only trigger when the user is typing a word (not after operators, commas at start, etc.)
    if (/\w+$/.test(linePrefix)) {
      return this.completeBareColumns(document, position);
    }

    return undefined;
  }

  private completeModelNames(): vscode.CompletionItem[] {
    const models = this.indexer.getAllModels();

    return models.map((model) => {
      const item = new vscode.CompletionItem(
        model.name,
        vscode.CompletionItemKind.Reference
      );

      item.detail = `${model.layer} | ${model.kind}`;
      item.documentation = new vscode.MarkdownString();

      if (model.yml?.description) {
        item.documentation.appendText(model.yml.description + "\n\n");
      }

      item.documentation.appendMarkdown(
        `**Path:** \`${model.relativePath}\`\n\n`
      );

      if (model.country) {
        item.documentation.appendMarkdown(
          `**Country:** ${model.country.toUpperCase()}\n\n`
        );
      }

      // Show resolved columns if available, fall back to explicit columns
      const cols = model.resolvedColumns || model.columns;
      if (cols.length > 0) {
        item.documentation.appendMarkdown("**Columns:**\n");
        for (const col of cols.slice(0, 10)) {
          const type = col.dataType ? ` ${col.dataType}` : "";
          item.documentation.appendMarkdown(
            `- \`${col.name}\`${type}\n`
          );
        }
        if (cols.length > 10) {
          item.documentation.appendMarkdown(
            `- ... and ${cols.length - 10} more\n`
          );
        }
      }

      const layerOrder: Record<string, string> = {
        staging: "0",
        intermediate: "1",
        marts: "2",
        dbt: "3",
        unknown: "9",
      };
      item.sortText = `${layerOrder[model.layer] || "9"}_${model.name}`;

      return item;
    });
  }

  /**
   * Complete column names when typing "alias." inside a query.
   * Uses scope-aware alias resolution to find columns.
   */
  private completeQualifiedColumns(
    alias: string,
    document: vscode.TextDocument,
    position: vscode.Position
  ): vscode.CompletionItem[] | undefined {
    const scopeColumns = this.buildScopeAliasMap(document, position);
    if (!scopeColumns) return undefined;

    const columns = scopeColumns.get(alias.toLowerCase());
    if (!columns || columns.length === 0) return undefined;

    return columns.map((col, idx) => {
      const item = new vscode.CompletionItem(
        col.name,
        vscode.CompletionItemKind.Field
      );
      item.detail = col.dataType || "column";
      if (col.source) {
        item.detail += ` (${col.source})`;
      }
      if (col.comment) {
        item.documentation = new vscode.MarkdownString(col.comment);
      }
      item.sortText = String(idx).padStart(4, "0");
      return item;
    });
  }

  /**
   * Complete bare column names (without table alias prefix).
   * Suggests all columns from all sources in the current scope.
   */
  private completeBareColumns(
    document: vscode.TextDocument,
    position: vscode.Position
  ): vscode.CompletionItem[] | undefined {
    const scopeColumns = this.buildScopeAliasMap(document, position);
    if (!scopeColumns) return undefined;

    // Collect all unique columns from all sources in scope
    const allColumns = new Map<string, ResolvedColumn>();
    for (const [, cols] of scopeColumns) {
      for (const col of cols) {
        const key = col.name.toLowerCase();
        if (!allColumns.has(key)) {
          allColumns.set(key, col);
        }
      }
    }

    if (allColumns.size === 0) return undefined;

    let idx = 0;
    const items: vscode.CompletionItem[] = [];
    for (const [, col] of allColumns) {
      const item = new vscode.CompletionItem(
        col.name,
        vscode.CompletionItemKind.Field
      );
      item.detail = col.dataType || "column";
      if (col.source) {
        item.detail += ` (${col.source})`;
      }
      if (col.comment) {
        item.documentation = new vscode.MarkdownString(col.comment);
      }
      item.sortText = String(idx).padStart(4, "0");
      idx++;
      items.push(item);
    }

    return items;
  }

  /**
   * Build an alias -> ResolvedColumn[] map for the scope containing the cursor.
   * Determines which CTE or final SELECT body the cursor is in,
   * then resolves FROM/JOIN sources within that scope.
   */
  private buildScopeAliasMap(
    document: vscode.TextDocument,
    position: vscode.Position
  ): Map<string, ResolvedColumn[]> | undefined {
    const model = this.indexer.getModelByFile(document.uri.fsPath);
    if (!model) return undefined;

    const text = document.getText();

    let cteBlocks: { name: string; body: string; startLine: number }[] = [];
    let finalSelectBody = "";
    let finalStartLine = 0;

    try {
      const extracted = extractCteBlocks(text);
      cteBlocks = extracted.ctes;
      finalSelectBody = extracted.finalSelectBody;
      finalStartLine = extracted.finalSelectStartLine ?? 0;
    } catch {
      return undefined;
    }

    const cursorLine = position.line;

    // Check each CTE to see if cursor is within its body
    for (const cte of cteBlocks) {
      const bodyLineCount = cte.body.split("\n").length;
      const cteEndLine = cte.startLine + bodyLineCount - 1;
      if (cursorLine >= cte.startLine && cursorLine <= cteEndLine) {
        // Find the specific UNION branch the cursor is in
        const branch = this.findUnionBranch(cte.body, cte.startLine, cursorLine);
        return this.resolveSourcesForBody(branch, model, cteBlocks);
      }
    }

    // Check if cursor is in the final SELECT body
    if (finalSelectBody && cursorLine >= finalStartLine) {
      const branch = this.findUnionBranch(finalSelectBody, finalStartLine, cursorLine);
      return this.resolveSourcesForBody(branch, model, cteBlocks, true);
    }

    return undefined;
  }

  /**
   * Find which UNION ALL branch the cursor is in and return that branch's text.
   * If no UNION ALL, returns the full body.
   */
  private findUnionBranch(body: string, bodyStartLine: number, cursorLine: number): string {
    const segments = splitUnionAll(body);
    if (segments.length <= 1) return body;

    for (let i = segments.length - 1; i >= 0; i--) {
      const seg = segments[i];
      const segStartLine = bodyStartLine + seg.lineOffset;
      if (cursorLine >= segStartLine) {
        return seg.segment;
      }
    }
    return body;
  }

  /**
   * Parse FROM/JOIN sources in a SQL body and resolve them to columns.
   * Returns alias -> ResolvedColumn[] map.
   */
  private resolveSourcesForBody(
    body: string,
    model: { resolvedCteColumns?: Map<string, ResolvedColumn[]> },
    cteBlocks: { name: string; body: string; startLine: number }[],
    isFinalScope: boolean = false
  ): Map<string, ResolvedColumn[]> {
    const aliasMap = new Map<string, ResolvedColumn[]>();
    const sources = parseFromClause(body);

    for (const source of sources) {
      const aliasKey = source.alias.toLowerCase();

      if (source.isLiveRef || source.isDbtRef) {
        const refModel = this.indexer.getModel(source.sourceName);
        if (refModel) {
          const cols =
            refModel.resolvedColumns ||
            (refModel.columns.length > 0
              ? refModel.columns.map((c) => ({
                  name: c.name,
                  dataType: c.dataType,
                  confidence: "known" as const,
                  source: refModel.name,
                  comment: c.comment,
                  isNullable: c.isNullable,
                }))
              : undefined);
          if (cols && cols.length > 0) {
            aliasMap.set(aliasKey, cols);
          }
        }
      } else {
        // CTE or subquery reference
        const cteCols = model.resolvedCteColumns?.get(
          source.sourceName.toLowerCase()
        );
        if (cteCols && cteCols.length > 0) {
          aliasMap.set(aliasKey, cteCols);
        }
      }
    }

    // In the final scope, also register CTE names as valid aliases
    if (isFinalScope) {
      for (const cte of cteBlocks) {
        const cteCols = model.resolvedCteColumns?.get(
          cte.name.toLowerCase()
        );
        if (cteCols && cteCols.length > 0) {
          aliasMap.set(cte.name.toLowerCase(), cteCols);
        }
      }
    }

    return aliasMap;
  }
}
