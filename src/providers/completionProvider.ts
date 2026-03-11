import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";

/**
 * Autocomplete:
 * - Model names after LIVE. or inside ref('')
 * - Column names after alias. (when alias maps to a known model/CTE)
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
      return this.completeColumns(aliasMatch[1], document, position);
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
   * Looks up the alias as a model name (for LIVE.model AS alias patterns)
   * or as a CTE name within the current file.
   */
  private completeColumns(
    alias: string,
    document: vscode.TextDocument,
    _position: vscode.Position
  ): vscode.CompletionItem[] | undefined {
    // Try looking up alias as a model name directly
    let model = this.indexer.getModel(alias);

    // If not found, scan the document for "FROM LIVE.xxx AS alias" or "FROM xxx alias"
    if (!model) {
      const text = document.getText();
      // Pattern: LIVE.model_name [AS] alias
      const fromPattern = new RegExp(
        `LIVE\\.(\\w+)\\s+(?:AS\\s+)?${alias}\\b`,
        "i"
      );
      const fromMatch = fromPattern.exec(text);
      if (fromMatch) {
        model = this.indexer.getModel(fromMatch[1]);
      }
    }

    if (!model) return undefined;

    const cols = model.resolvedColumns || model.columns;
    if (cols.length === 0) return undefined;

    return cols.map((col, idx) => {
      const item = new vscode.CompletionItem(
        col.name,
        vscode.CompletionItemKind.Field
      );
      item.detail = col.dataType || "unknown type";
      if ("comment" in col && col.comment) {
        item.documentation = new vscode.MarkdownString(col.comment as string);
      }
      item.sortText = String(idx).padStart(4, "0");
      return item;
    });
  }
}
