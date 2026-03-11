import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";
import { IndexedModel } from "../types";

/**
 * Show rich hover info when hovering over model references.
 * Displays: description, layer, kind, columns, dependencies.
 */
export class DltHoverProvider implements vscode.HoverProvider {
  constructor(private indexer: DltModelIndexer) {}

  provideHover(
    document: vscode.TextDocument,
    position: vscode.Position,
    _token: vscode.CancellationToken
  ): vscode.Hover | undefined {
    const modelName = this.extractModelName(document, position);
    if (!modelName) return undefined;

    const model = this.indexer.getModel(modelName);
    if (!model) return undefined;

    const markdown = this.buildHoverContent(model);
    return new vscode.Hover(markdown);
  }

  private buildHoverContent(model: IndexedModel): vscode.MarkdownString {
    const md = new vscode.MarkdownString();
    md.isTrusted = true;
    md.supportHtml = true;

    // Title with layer badge
    const layerIcon: Record<string, string> = {
      staging: "$(database)",
      intermediate: "$(arrow-right)",
      marts: "$(star)",
      dbt: "$(package)",
      unknown: "$(file)",
    };
    md.appendMarkdown(
      `### ${layerIcon[model.layer] || ""} ${model.name}\n\n`
    );

    // Description from YML
    if (model.yml?.description) {
      md.appendMarkdown(`${model.yml.description}\n\n`);
    }

    // Metadata table
    md.appendMarkdown(`| | |\n|---|---|\n`);
    md.appendMarkdown(`| **Layer** | ${model.layer} |\n`);
    md.appendMarkdown(`| **Type** | ${model.kind.replace(/_/g, " ")} |\n`);
    if (model.country) {
      md.appendMarkdown(
        `| **Country** | ${model.country.toUpperCase()} |\n`
      );
    }
    md.appendMarkdown(`| **Path** | \`${model.relativePath}\` |\n\n`);

    // Columns — prefer resolvedColumns (schema propagation), fall back to explicit columns
    const displayColumns = model.resolvedColumns || (model.columns.length > 0 ? model.columns : undefined);
    if (displayColumns && displayColumns.length > 0) {
      const isResolved = !!model.resolvedColumns;
      md.appendMarkdown(
        `**Columns** (${displayColumns.length}${isResolved ? ", resolved" : ""}):\n\n`
      );
      md.appendMarkdown(`| Column | Type | Info |\n|---|---|---|\n`);
      for (const col of displayColumns.slice(0, 20)) {
        const typeStr = col.dataType || "-";
        // Show confidence badge for resolved columns
        const confidence =
          "confidence" in col
            ? (col as any).confidence === "known"
              ? ""
              : (col as any).confidence === "inferred"
                ? " *"
                : " ?"
            : "";
        const desc = ("comment" in col ? (col as any).comment : "") || "";
        const truncDesc =
          desc.length > 50 ? desc.substring(0, 47) + "..." : desc;
        md.appendMarkdown(
          `| \`${col.name}\` | ${typeStr}${confidence} | ${truncDesc} |\n`
        );
      }
      if (displayColumns.length > 20) {
        md.appendMarkdown(
          `\n*... and ${displayColumns.length - 20} more columns*\n\n`
        );
      }
      if (isResolved) {
        md.appendMarkdown(`\n*\\* = inferred type, ? = unknown type*\n\n`);
      }
    }

    // Dependencies (what this model references)
    if (model.references.length > 0) {
      md.appendMarkdown(`\n**Depends on:**\n`);
      for (const ref of model.references) {
        const icon = ref.type === "source" ? "$(cloud)" : "$(link)";
        const label =
          ref.type === "source"
            ? `${ref.sourceName}.${ref.name}`
            : ref.name;
        md.appendMarkdown(`- ${icon} ${label}\n`);
      }
    }

    // Reverse dependencies (who uses this model)
    const usedBy = this.indexer.getReferencedBy(model.name);
    if (usedBy.length > 0) {
      md.appendMarkdown(`\n**Used by:**\n`);
      for (const dep of usedBy.slice(0, 10)) {
        md.appendMarkdown(`- ${dep.name} (${dep.layer})\n`);
      }
      if (usedBy.length > 10) {
        md.appendMarkdown(`- ... and ${usedBy.length - 10} more\n`);
      }
    }

    return md;
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
