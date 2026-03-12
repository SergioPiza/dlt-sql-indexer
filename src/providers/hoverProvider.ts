import * as vscode from "vscode";
import { DltModelIndexer } from "../indexer";
import { IndexedModel } from "../types";
import type { ResolvedColumn, FromSource } from "../schema/types";

type HoverTarget =
  | { kind: "model"; name: string }
  | { kind: "cte"; name: string }
  | { kind: "alias"; name: string };

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
    const target = this.extractHoverTarget(document, position);
    if (!target) return undefined;

    if (target.kind === "model") {
      const model = this.indexer.getModel(target.name);
      if (!model) return undefined;
      return new vscode.Hover(this.buildHoverContent(model));
    }

    const model = this.indexer.getModelByFile(document.uri.fsPath);
    if (!model) return undefined;

    if (target.kind === "cte") {
      // CTE hover — look up resolved CTE columns + sources
      const key = target.name.toLowerCase();
      const cteColumns = model.resolvedCteColumns?.get(key);
      const cteSources = model.resolvedCteSources?.get(key);
      return new vscode.Hover(
        this.buildCteHoverContent(target.name, cteColumns, cteSources)
      );
    }

    // Alias hover — resolve alias to its source model or CTE
    const src = model.aliasToSource?.get(target.name.toLowerCase());
    if (!src) return undefined;

    if (src.isLiveRef || src.isDbtRef) {
      // Alias points to a model
      const sourceModel = this.indexer.getModel(src.sourceName);
      if (sourceModel) {
        return new vscode.Hover(this.buildHoverContent(sourceModel));
      }
    }

    // Alias points to a CTE
    const cteKey = src.sourceName.toLowerCase();
    const cteColumns = model.resolvedCteColumns?.get(cteKey);
    const cteSources = model.resolvedCteSources?.get(cteKey);
    if (cteColumns || cteSources) {
      return new vscode.Hover(
        this.buildCteHoverContent(src.sourceName, cteColumns, cteSources)
      );
    }

    return undefined;
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

  private buildCteHoverContent(
    cteName: string,
    columns?: ResolvedColumn[],
    sources?: FromSource[]
  ): vscode.MarkdownString {
    const md = new vscode.MarkdownString();
    md.isTrusted = true;
    md.supportHtml = true;

    md.appendMarkdown(`### $(symbol-structure) ${cteName} *(CTE)*\n\n`);

    if (columns && columns.length > 0) {
      md.appendMarkdown(
        `**Columns** (${columns.length}):\n\n`
      );
      md.appendMarkdown(`| Column | Type | Info |\n|---|---|---|\n`);
      for (const col of columns.slice(0, 20)) {
        const typeStr = col.dataType || "-";
        const confidence =
          col.confidence === "known"
            ? ""
            : col.confidence === "inferred"
              ? " *"
              : " ?";
        const desc = col.comment || "";
        const truncDesc =
          desc.length > 50 ? desc.substring(0, 47) + "..." : desc;
        md.appendMarkdown(
          `| \`${col.name}\` | ${typeStr}${confidence} | ${truncDesc} |\n`
        );
      }
      if (columns.length > 20) {
        md.appendMarkdown(
          `\n*... and ${columns.length - 20} more columns*\n\n`
        );
      }
      md.appendMarkdown(`\n*\\* = inferred type, ? = unknown type*\n\n`);
    } else {
      md.appendMarkdown(`*No resolved columns available*\n\n`);
    }

    // Sources (what this CTE reads from)
    if (sources && sources.length > 0) {
      md.appendMarkdown(`**Sources:**\n`);
      for (const src of sources) {
        if (src.isLiveRef || src.isDbtRef) {
          md.appendMarkdown(`- $(link) ${src.sourceName}\n`);
        } else {
          md.appendMarkdown(`- $(symbol-structure) ${src.sourceName}\n`);
        }
      }
    }

    return md;
  }

  private extractHoverTarget(
    document: vscode.TextDocument,
    position: vscode.Position
  ): HoverTarget | undefined {
    const line = document.lineAt(position).text;
    let match: RegExpExecArray | null;

    // LIVE.model_name
    const liveMatch = /\bLIVE\.(\w+)\b/g;
    while ((match = liveMatch.exec(line)) !== null) {
      const start = match.index + "LIVE.".length;
      const end = start + match[1].length;
      if (position.character >= start && position.character <= end) {
        return { kind: "model", name: match[1] };
      }
    }

    // ${catalog}.schema.table
    const catalogMatch = /\$\{catalog\}\.(\w+)\.(\w+)/g;
    while ((match = catalogMatch.exec(line)) !== null) {
      const tableStart = match.index + match[0].length - match[2].length;
      const tableEnd = tableStart + match[2].length;
      if (position.character >= tableStart && position.character <= tableEnd) {
        return { kind: "model", name: match[2] };
      }
    }

    // {{ ref('model') }}
    const refMatch = /\{\{\s*ref\(\s*['"](\w+)['"]\s*\)\s*\}\}/g;
    while ((match = refMatch.exec(line)) !== null) {
      const nameStart = line.indexOf(match[1], match.index);
      const nameEnd = nameStart + match[1].length;
      if (position.character >= nameStart && position.character <= nameEnd) {
        return { kind: "model", name: match[1] };
      }
    }

    // Model name declaration: CREATE ... LIVE TABLE/VIEW model_name
    // or CREATE ... MATERIALIZED VIEW model_name
    const createMatch =
      /\bCREATE\b.*?\b(?:LIVE\s+(?:TABLE|VIEW)|MATERIALIZED\s+VIEW|STREAMING\s+TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\b/gi;
    while ((match = createMatch.exec(line)) !== null) {
      const nameStart = match.index + match[0].length - match[1].length;
      const nameEnd = nameStart + match[1].length;
      if (position.character >= nameStart && position.character <= nameEnd) {
        return { kind: "model", name: match[1] };
      }
    }

    // CTE declaration/reference or alias — check the word under cursor
    const model = this.indexer.getModelByFile(document.uri.fsPath);
    const wordRange = document.getWordRangeAtPosition(position, /\w+/);
    if (!wordRange) return undefined;

    const word = document.getText(wordRange);
    const charBefore =
      wordRange.start.character > 0
        ? line[wordRange.start.character - 1]
        : "";

    // Skip if preceded by a dot (e.g., LIVE.xxx — already handled above)
    if (charBefore === ".") return undefined;

    // Check if it's a known CTE name
    if (model?.ctes && model.ctes.length > 0) {
      const isCte = model.ctes.some(
        (c) => c.name.toLowerCase() === word.toLowerCase()
      );
      if (isCte) {
        return { kind: "cte", name: word };
      }
    }

    // Check if it's a known alias (e.g., "m" in "FROM LIVE.model AS m" or "m.col")
    if (model?.aliasToSource?.has(word.toLowerCase())) {
      return { kind: "alias", name: word };
    }

    return undefined;
  }
}
