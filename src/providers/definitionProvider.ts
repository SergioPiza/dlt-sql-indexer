import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";

/**
 * Go-to-definition: Ctrl+Click on LIVE.model_name or {{ ref('model') }}
 * jumps to the CREATE statement in the target file.
 */
export class DltDefinitionProvider implements vscode.DefinitionProvider {
  constructor(private indexer: DltModelIndexer) {}

  provideDefinition(
    document: vscode.TextDocument,
    position: vscode.Position,
    _token: vscode.CancellationToken
  ): vscode.Definition | undefined {
    const modelName = this.extractModelName(document, position);
    if (!modelName) return undefined;

    // Check if it's a CTE reference within the same file
    const currentModel = this.indexer.getModelByFile(document.uri.fsPath);
    if (currentModel) {
      const cte = currentModel.ctes.find(
        (c) => c.name.toLowerCase() === modelName.toLowerCase()
      );
      if (cte) {
        return new vscode.Location(
          document.uri,
          new vscode.Position(cte.line, cte.character)
        );
      }
    }

    // Look up in the global index
    const target = this.indexer.getModel(modelName);
    if (!target) return undefined;

    return new vscode.Location(
      target.uri,
      new vscode.Position(target.definitionLine, 0)
    );
  }

  private extractModelName(
    document: vscode.TextDocument,
    position: vscode.Position
  ): string | undefined {
    const line = document.lineAt(position).text;

    // LIVE.model_name
    const liveMatch = /\bLIVE\.(\w+)\b/g;
    let match: RegExpExecArray | null;
    while ((match = liveMatch.exec(line)) !== null) {
      const start = match.index + "LIVE.".length;
      const end = start + match[1].length;
      if (position.character >= start && position.character <= end) {
        return match[1];
      }
    }

    // ${catalog}.schema.table
    const catalogMatch = /\$\{catalog\}\.(\w+)\.(\w+)/g;
    while ((match = catalogMatch.exec(line)) !== null) {
      const tableStart = match.index + match[0].length - match[2].length;
      const tableEnd = tableStart + match[2].length;
      if (position.character >= tableStart && position.character <= tableEnd) {
        return match[2];
      }
    }

    // {{ ref('model_name') }}
    const refMatch = /\{\{\s*ref\(\s*['"](\w+)['"]\s*\)\s*\}\}/g;
    while ((match = refMatch.exec(line)) !== null) {
      const nameStart = line.indexOf(match[1], match.index);
      const nameEnd = nameStart + match[1].length;
      if (position.character >= nameStart && position.character <= nameEnd) {
        return match[1];
      }
    }

    // Fall back: word under cursor might be a CTE or model name
    const wordRange = document.getWordRangeAtPosition(position, /\w+/);
    if (wordRange) {
      return document.getText(wordRange);
    }

    return undefined;
  }
}
