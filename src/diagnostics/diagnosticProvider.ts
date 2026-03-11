import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";
import { parseFromClause } from "../schema/cteAnalyzer";
import { parseSelectList } from "../schema/columnExtractor";

/**
 * Provides real-time diagnostics (warning squiggles) for column references
 * that don't exist in the source model or CTE.
 */
export class DltDiagnosticProvider {
  private diagnosticCollection: vscode.DiagnosticCollection;
  private indexer: DltModelIndexer;

  constructor(
    diagnosticCollection: vscode.DiagnosticCollection,
    indexer: DltModelIndexer
  ) {
    this.diagnosticCollection = diagnosticCollection;
    this.indexer = indexer;
  }

  /**
   * Validate a document and update its diagnostics.
   */
  validateDocument(document: vscode.TextDocument): void {
    // Only validate SQL files
    if (document.languageId !== "sql") return;

    // Check if diagnostics are enabled
    const config = vscode.workspace.getConfiguration("dltSqlIndexer");
    if (!config.get<boolean>("enableDiagnostics", true)) {
      this.diagnosticCollection.delete(document.uri);
      return;
    }

    const model = this.indexer.getModelByFile(document.uri.fsPath);
    if (!model) {
      this.diagnosticCollection.delete(document.uri);
      return;
    }

    const diagnostics: vscode.Diagnostic[] = [];
    const text = document.getText();
    const lines = text.split("\n");

    // Validate LIVE.model references on each line
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];

      // Skip comment lines
      if (line.trimStart().startsWith("--")) continue;

      // Check LIVE.xxx references — does the model exist?
      const liveRefPattern = /\bLIVE\.(\w+)\b/g;
      let match: RegExpExecArray | null;
      while ((match = liveRefPattern.exec(line)) !== null) {
        const refName = match[1];
        const refModel = this.indexer.getModel(refName);

        if (!refModel) {
          // Model not found in index — could be external, so use Info level
          const range = new vscode.Range(
            i,
            match.index + 5, // after "LIVE."
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

    // Validate column references in SELECT statements against source schemas
    this.validateColumnReferences(model, text, lines, diagnostics);

    this.diagnosticCollection.set(document.uri, diagnostics);
  }

  /**
   * Validate that columns referenced in SELECT statements exist in their source.
   */
  private validateColumnReferences(
    model: { rawCteBlocks?: { name: string; body: string; startLine: number }[]; rawSelectBody?: string },
    _fullText: string,
    lines: string[],
    diagnostics: vscode.Diagnostic[]
  ): void {
    // Build a map of CTE name -> resolved columns from the index
    const currentModel = this.indexer.getModelByFile(
      (model as any).filePath
    );
    if (!currentModel) return;

    // For each line with a qualified column reference (alias.column),
    // check if the column exists in the source
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.trimStart().startsWith("--")) continue;

      // Look for qualified column refs: alias.column_name
      // But only in SELECT context (skip FROM, JOIN, ON, WHERE clauses
      // where alias.column could be a table.column schema ref)
      const qualifiedRefPattern = /\b(\w+)\.(\w+)\b/g;
      let match: RegExpExecArray | null;

      while ((match = qualifiedRefPattern.exec(line)) !== null) {
        const alias = match[1];
        const colName = match[2];

        // Skip known non-column patterns
        if (alias.toUpperCase() === "LIVE") continue;
        if (alias === "${catalog}") continue;
        if (colName === "*") continue;

        // Try to resolve the alias to a model with known columns
        const aliasModel = this.indexer.getModel(alias);
        if (aliasModel?.resolvedColumns && aliasModel.resolvedColumns.length > 0) {
          const exists = aliasModel.resolvedColumns.some(
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
                `Column '${colName}' not found in '${alias}' (${aliasModel.resolvedColumns.length} columns known)`,
                vscode.DiagnosticSeverity.Warning
              )
            );
          }
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
