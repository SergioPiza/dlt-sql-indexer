import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";

/**
 * Find all references: shows every file that references a given model.
 * Works on model names in CREATE statements and LIVE.xxx references.
 */
export class DltReferenceProvider implements vscode.ReferenceProvider {
  constructor(private indexer: DltModelIndexer) {}

  provideReferences(
    document: vscode.TextDocument,
    position: vscode.Position,
    _context: vscode.ReferenceContext,
    _token: vscode.CancellationToken
  ): vscode.Location[] | undefined {
    const modelName = this.extractModelName(document, position);
    if (!modelName) return undefined;

    // Verify this is actually an indexed model
    const model = this.indexer.getModel(modelName);
    if (!model) return undefined;

    const locations: vscode.Location[] = [];

    // Include the definition itself
    locations.push(
      new vscode.Location(
        model.uri,
        new vscode.Position(model.definitionLine, 0)
      )
    );

    // Find all models that reference this one
    const referencers = this.indexer.getReferencedBy(modelName);
    for (const referencer of referencers) {
      for (const ref of referencer.references) {
        if (ref.name.toLowerCase() === modelName.toLowerCase()) {
          locations.push(
            new vscode.Location(
              referencer.uri,
              new vscode.Position(ref.line, ref.character)
            )
          );
        }
      }
    }

    return locations;
  }

  private extractModelName(
    document: vscode.TextDocument,
    position: vscode.Position
  ): string | undefined {
    const line = document.lineAt(position).text;

    // Check CREATE statement (the model being defined)
    const createPatterns = [
      /CREATE\s+TEMPORARY\s+LIVE\s+(?:TABLE|VIEW)\s+(\w+)/i,
      /CREATE\s+OR\s+REFRESH\s+MATERIALIZED\s+VIEW\s+(\w+)/i,
      /CREATE\s+(?:OR\s+REFRESH\s+)?STREAMING\s+TABLE\s+(\w+)/i,
    ];

    for (const pattern of createPatterns) {
      const match = pattern.exec(line);
      if (match) {
        const nameStart = line.indexOf(match[1], match.index);
        const nameEnd = nameStart + match[1].length;
        if (position.character >= nameStart && position.character <= nameEnd) {
          return match[1];
        }
      }
    }

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

    // {{ ref('model') }}
    const refMatch = /\{\{\s*ref\(\s*['"](\w+)['"]\s*\)\s*\}\}/g;
    while ((match = refMatch.exec(line)) !== null) {
      const nameStart = line.indexOf(match[1], match.index);
      const nameEnd = nameStart + match[1].length;
      if (position.character >= nameStart && position.character <= nameEnd) {
        return match[1];
      }
    }

    return undefined;
  }
}
