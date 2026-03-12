import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";

/**
 * Rename/refactor support:
 * - Model names: renames across all files (CREATE, LIVE.xxx, ref('xxx'))
 * - Column names: renames across the dependency chain (source → intermediates → marts)
 */
export class DltRenameProvider implements vscode.RenameProvider {
  constructor(private indexer: DltModelIndexer) {}

  prepareRename(
    document: vscode.TextDocument,
    position: vscode.Position,
    _token: vscode.CancellationToken
  ): vscode.ProviderResult<vscode.Range | { range: vscode.Range; placeholder: string }> {
    // Try model name first
    const modelInfo = this.extractModelNameRange(document, position);
    if (modelInfo) {
      return { range: modelInfo.range, placeholder: modelInfo.name };
    }

    // Try column name (qualified or bare word in SQL context)
    const columnInfo = this.extractColumnRange(document, position);
    if (columnInfo) {
      return { range: columnInfo.range, placeholder: columnInfo.name };
    }

    throw new Error("Cannot rename this element — place cursor on a model name or column name");
  }

  async provideRenameEdits(
    document: vscode.TextDocument,
    position: vscode.Position,
    newName: string,
    _token: vscode.CancellationToken
  ): Promise<vscode.WorkspaceEdit | undefined> {
    // Try model rename first
    const modelInfo = this.extractModelNameRange(document, position);
    if (modelInfo) {
      return this.renameModel(modelInfo.name, newName);
    }

    // Column rename (across dependency chain)
    const columnInfo = this.extractColumnRange(document, position);
    if (columnInfo) {
      return this.renameColumn(document, columnInfo.name, newName);
    }

    return undefined;
  }

  /**
   * Rename a model across all files.
   * Updates CREATE statement, LIVE.xxx references, and ref('xxx') references.
   */
  private async renameModel(
    oldName: string,
    newName: string
  ): Promise<vscode.WorkspaceEdit> {
    const edit = new vscode.WorkspaceEdit();

    // 1. Find and update the definition (CREATE statement)
    const model = this.indexer.getModel(oldName);
    if (model) {
      const doc = await vscode.workspace.openTextDocument(model.uri);
      const line = doc.lineAt(model.definitionLine).text;

      // Replace the model name in the CREATE statement
      const createPatterns = [
        /\b(CREATE\s+(?:OR\s+(?:REFRESH|REPLACE)\s+)?(?:TEMPORARY\s+)?(?:STREAMING\s+)?(?:LIVE\s+)?(?:TABLE|VIEW|MATERIALIZED\s+VIEW)\s+)(\w+)/i,
      ];

      for (const pattern of createPatterns) {
        const match = pattern.exec(line);
        if (match && match[2].toLowerCase() === oldName.toLowerCase()) {
          const nameStart = match.index + match[1].length;
          const range = new vscode.Range(
            model.definitionLine,
            nameStart,
            model.definitionLine,
            nameStart + match[2].length
          );
          edit.replace(model.uri, range, newName);
          break;
        }
      }
    }

    // 2. Find and update all references across all files
    const referencers = this.indexer.getReferencedBy(oldName);
    for (const referencer of referencers) {
      const doc = await vscode.workspace.openTextDocument(referencer.uri);
      const text = doc.getText();
      const lines = text.split("\n");

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (line.trimStart().startsWith("--")) continue;

        const commentIdx = line.indexOf("--");
        const codePart = commentIdx >= 0 ? line.substring(0, commentIdx) : line;

        // LIVE.old_name
        const livePattern = new RegExp(`\\bLIVE\\.(${escapeRegex(oldName)})\\b`, "gi");
        let match: RegExpExecArray | null;
        while ((match = livePattern.exec(codePart)) !== null) {
          const nameStart = match.index + 5; // "LIVE.".length
          const range = new vscode.Range(i, nameStart, i, nameStart + match[1].length);
          edit.replace(referencer.uri, range, newName);
        }

        // ref('old_name') or ref("old_name")
        const refPattern = new RegExp(
          `(\\{\\{\\s*ref\\(\\s*['"])(${escapeRegex(oldName)})(['"]\\s*\\)\\s*\\}\\})`,
          "gi"
        );
        while ((match = refPattern.exec(codePart)) !== null) {
          const nameStart = match.index + match[1].length;
          const range = new vscode.Range(i, nameStart, i, nameStart + match[2].length);
          edit.replace(referencer.uri, range, newName);
        }
      }
    }

    // 3. Rename the file itself (model_name.sql → new_name.sql)
    if (model) {
      const oldPath = model.uri;
      const dir = vscode.Uri.joinPath(oldPath, "..");
      const newUri = vscode.Uri.joinPath(dir, `${newName}.sql`);
      edit.renameFile(oldPath, newUri);
    }

    return edit;
  }

  /**
   * Rename a column across the dependency chain.
   * Traces the column to its origin model via resolvedColumns.source,
   * then renames in every model that has the same column from the same source.
   * This correctly handles sibling models that share the same upstream source.
   */
  private async renameColumn(
    document: vscode.TextDocument,
    oldName: string,
    newName: string
  ): Promise<vscode.WorkspaceEdit> {
    const edit = new vscode.WorkspaceEdit();
    const currentModel = this.indexer.getModelByFile(document.uri.fsPath);

    // If we have no resolved columns, fall back to renaming within the current file only
    if (!currentModel?.resolvedColumns) {
      this.replaceColumnInDocument(edit, document, oldName, newName);
      return edit;
    }

    // Find the column in the current model's resolved columns
    const col = currentModel.resolvedColumns.find(
      (c) => c.name.toLowerCase() === oldName.toLowerCase()
    );

    // Determine the effective source — the origin model where this column was first defined.
    // Walk up the source chain to find the root origin.
    let effectiveSource = currentModel.name.toLowerCase();
    if (col?.source) {
      let tracedSource = col.source.toLowerCase();
      const visited = new Set<string>();
      while (!visited.has(tracedSource)) {
        visited.add(tracedSource);
        const sourceModel = this.indexer.getModel(tracedSource);
        if (!sourceModel) break;
        // Verify the source model also has a column with this SAME name
        const sourceCol = sourceModel.resolvedColumns?.find(
          (c) => c.name.toLowerCase() === oldName.toLowerCase()
        );
        if (!sourceCol) break;
        // If the source column traces further upstream, keep walking
        if (sourceCol.source && sourceCol.source.toLowerCase() !== tracedSource) {
          tracedSource = sourceCol.source.toLowerCase();
        } else {
          break;
        }
      }
      effectiveSource = tracedSource;
    }

    // Collect all model files that have this column originating from the same source
    const filesToRename = new Set<string>();
    for (const model of this.indexer.getAllModels()) {
      const modelCol = model.resolvedColumns?.find(
        (c) => c.name.toLowerCase() === oldName.toLowerCase()
      );
      if (modelCol && modelCol.source?.toLowerCase() === effectiveSource) {
        filesToRename.add(model.filePath);
      }
    }

    // Always include the current file even if resolution didn't track it
    filesToRename.add(document.uri.fsPath);

    // Apply renames in all affected files
    for (const filePath of filesToRename) {
      const uri = vscode.Uri.file(filePath);
      const doc = await vscode.workspace.openTextDocument(uri);
      this.replaceColumnInDocument(edit, doc, oldName, newName);
    }

    return edit;
  }

  /**
   * Replace all whole-word occurrences of a column name in a single document.
   * Skips comments and string literals.
   */
  private replaceColumnInDocument(
    edit: vscode.WorkspaceEdit,
    document: vscode.TextDocument,
    oldName: string,
    newName: string
  ): void {
    const text = document.getText();
    const lines = text.split("\n");
    const pattern = new RegExp(`\\b${escapeRegex(oldName)}\\b`, "g");

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.trimStart().startsWith("--")) continue;

      const commentIdx = line.indexOf("--");
      const codePart = commentIdx >= 0 ? line.substring(0, commentIdx) : line;

      let match: RegExpExecArray | null;
      while ((match = pattern.exec(codePart)) !== null) {
        // Check we're not inside a string literal
        const before = codePart.substring(0, match.index);
        const singleQuotes = (before.match(/'/g) || []).length;
        if (singleQuotes % 2 !== 0) continue;

        const range = new vscode.Range(i, match.index, i, match.index + oldName.length);
        edit.replace(document.uri, range, newName);
      }
    }
  }

  /**
   * Extract model name and its range at the cursor position.
   * Returns undefined if cursor is not on a model name.
   */
  private extractModelNameRange(
    document: vscode.TextDocument,
    position: vscode.Position
  ): { name: string; range: vscode.Range } | undefined {
    const line = document.lineAt(position).text;

    // CREATE ... TABLE/VIEW model_name
    const createPatterns = [
      /CREATE\s+(?:OR\s+(?:REFRESH|REPLACE)\s+)?(?:TEMPORARY\s+)?LIVE\s+(?:TABLE|VIEW)\s+(\w+)/i,
      /CREATE\s+OR\s+(?:REFRESH|REPLACE)\s+MATERIALIZED\s+VIEW\s+(\w+)/i,
      /CREATE\s+(?:OR\s+(?:REFRESH|REPLACE)\s+)?(?:TEMPORARY\s+)?STREAMING\s+TABLE\s+(\w+)/i,
    ];

    for (const pattern of createPatterns) {
      const match = pattern.exec(line);
      if (match) {
        const nameStart = line.indexOf(match[1], match.index);
        const nameEnd = nameStart + match[1].length;
        if (position.character >= nameStart && position.character <= nameEnd) {
          const model = this.indexer.getModel(match[1]);
          if (model) {
            return {
              name: match[1],
              range: new vscode.Range(position.line, nameStart, position.line, nameEnd),
            };
          }
        }
      }
    }

    // LIVE.model_name
    const livePattern = /\bLIVE\.(\w+)\b/g;
    let match: RegExpExecArray | null;
    while ((match = livePattern.exec(line)) !== null) {
      const nameStart = match.index + 5; // "LIVE.".length
      const nameEnd = nameStart + match[1].length;
      if (position.character >= nameStart && position.character <= nameEnd) {
        const model = this.indexer.getModel(match[1]);
        if (model) {
          return {
            name: match[1],
            range: new vscode.Range(position.line, nameStart, position.line, nameEnd),
          };
        }
      }
    }

    // {{ ref('model_name') }}
    const refPattern = /\{\{\s*ref\(\s*['"](\w+)['"]\s*\)\s*\}\}/g;
    while ((match = refPattern.exec(line)) !== null) {
      const nameStart = line.indexOf(match[1], match.index);
      const nameEnd = nameStart + match[1].length;
      if (position.character >= nameStart && position.character <= nameEnd) {
        const model = this.indexer.getModel(match[1]);
        if (model) {
          return {
            name: match[1],
            range: new vscode.Range(position.line, nameStart, position.line, nameEnd),
          };
        }
      }
    }

    return undefined;
  }

  /**
   * Extract a column name range at the cursor position.
   * Handles qualified (alias.column) and bare column names.
   */
  private extractColumnRange(
    document: vscode.TextDocument,
    position: vscode.Position
  ): { name: string; range: vscode.Range } | undefined {
    const line = document.lineAt(position).text;

    // Skip comments
    if (line.trimStart().startsWith("--")) return undefined;
    const commentIdx = line.indexOf("--");
    if (commentIdx >= 0 && position.character > commentIdx) return undefined;

    // Check for qualified reference: alias.column — return just the column part
    const qualifiedPattern = /\b(\w+)\.(\w+)\b/g;
    let match: RegExpExecArray | null;
    while ((match = qualifiedPattern.exec(line)) !== null) {
      const alias = match[1];
      if (alias.toUpperCase() === "LIVE") continue; // Model refs handled above

      const colStart = match.index + alias.length + 1;
      const colEnd = colStart + match[2].length;
      if (position.character >= colStart && position.character <= colEnd) {
        return {
          name: match[2],
          range: new vscode.Range(position.line, colStart, position.line, colEnd),
        };
      }
    }

    // Bare word — column name
    const wordRange = document.getWordRangeAtPosition(position, /\w+/);
    if (wordRange) {
      const word = document.getText(wordRange);
      // Skip SQL keywords
      const keywords = new Set([
        "select", "from", "where", "group", "order", "having", "limit",
        "union", "all", "insert", "update", "delete", "create", "or",
        "refresh", "replace", "temporary", "live", "table", "view",
        "materialized", "streaming", "as", "on", "and", "not", "in",
        "exists", "between", "like", "is", "null", "join", "inner",
        "left", "right", "full", "outer", "cross", "case", "when",
        "then", "else", "end", "cast", "string", "int", "bigint",
        "double", "float", "boolean", "date", "timestamp", "decimal",
        "true", "false", "with", "by", "asc", "desc", "distinct",
        "coalesce", "if", "ifnull", "nullif", "over", "partition",
        "row", "rows", "range", "unbounded", "preceding", "following",
        "current", "first", "last", "constraint",
      ]);
      if (keywords.has(word.toLowerCase())) return undefined;

      return {
        name: word,
        range: wordRange,
      };
    }

    return undefined;
  }
}

/** Escape special regex characters */
function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
