import { SelectItem } from "./types";

/**
 * Parse a SELECT clause and extract individual column items.
 * Handles nested parentheses, CASE blocks, string literals, and qualified refs.
 */
export function parseSelectList(selectBody: string): SelectItem[] {
  // Find the SELECT keyword (skip leading whitespace/comments)
  const selectMatch = selectBody.match(/\bSELECT\s+(DISTINCT\s+)?/i);
  if (!selectMatch) return [];

  const afterSelect = selectBody.substring(
    selectMatch.index! + selectMatch[0].length
  );

  // Find where the column list ends (at FROM, WHERE, or end of string)
  const fromIndex = findTopLevelKeyword(afterSelect, "FROM");
  const columnListStr =
    fromIndex >= 0 ? afterSelect.substring(0, fromIndex) : afterSelect;

  // Split on commas at depth 0
  const segments = splitAtTopLevelCommas(columnListStr);

  return segments.map((seg) => parseSelectItem(seg.text, seg.lineOffset));
}

/**
 * Parse a single SELECT item (one comma-separated segment).
 */
function parseSelectItem(text: string, lineOffset?: number): SelectItem {
  const trimmed = text.trim();

  // SELECT *
  if (trimmed === "*") {
    return { expression: "*", isStar: true, lineOffset };
  }

  // alias.*
  const qualifiedStarMatch = trimmed.match(/^(\w+)\.\*$/);
  if (qualifiedStarMatch) {
    return {
      expression: trimmed,
      isStar: true,
      starQualifier: qualifiedStarMatch[1],
      lineOffset,
    };
  }

  // Check for AS alias at the end (must be at top level, not inside parens)
  const aliasInfo = extractAlias(trimmed);
  const expression = aliasInfo.expression;
  const alias = aliasInfo.alias;

  // Simple qualified column: alias.column_name
  const qualifiedMatch = expression.match(/^(\w+)\.(\w+)$/);
  if (qualifiedMatch) {
    return {
      expression,
      alias: alias || qualifiedMatch[2],
      isStar: false,
      tableAlias: qualifiedMatch[1],
      columnName: qualifiedMatch[2],
      lineOffset,
    };
  }

  // Simple bare column name
  const bareMatch = expression.match(/^(\w+)$/);
  if (bareMatch) {
    return {
      expression,
      alias: alias || bareMatch[1],
      isStar: false,
      columnName: bareMatch[1],
      lineOffset,
    };
  }

  // Backtick-quoted identifier: `column_name`
  const backtickMatch = expression.match(/^`(\w+)`$/);
  if (backtickMatch) {
    return {
      expression,
      alias: alias || backtickMatch[1],
      isStar: false,
      columnName: backtickMatch[1],
      lineOffset,
    };
  }

  // Qualified backtick: alias.`column`
  const qualifiedBacktickMatch = expression.match(/^(\w+)\.`(\w+)`$/);
  if (qualifiedBacktickMatch) {
    return {
      expression,
      alias: alias || qualifiedBacktickMatch[2],
      isStar: false,
      tableAlias: qualifiedBacktickMatch[1],
      columnName: qualifiedBacktickMatch[2],
      lineOffset,
    };
  }

  // Complex expression — alias is required to know the output column name
  return {
    expression,
    alias,
    isStar: false,
    lineOffset,
  };
}

/**
 * Extract the AS alias from the end of an expression.
 * Must be at top level (not inside parentheses or quotes).
 */
function extractAlias(text: string): {
  expression: string;
  alias?: string;
} {
  // Walk backwards from the end to find the last top-level AS keyword
  // Strategy: find the last occurrence of " AS " (case-insensitive) at depth 0
  let depth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let lastAsIndex = -1;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];

    if (inSingleQuote) {
      if (ch === "'" && text[i - 1] !== "\\") inSingleQuote = false;
      continue;
    }
    if (inDoubleQuote) {
      if (ch === '"' && text[i - 1] !== "\\") inDoubleQuote = false;
      continue;
    }
    if (inBacktick) {
      if (ch === "`") inBacktick = false;
      continue;
    }

    if (ch === "'") {
      inSingleQuote = true;
    } else if (ch === '"') {
      inDoubleQuote = true;
    } else if (ch === "`") {
      inBacktick = true;
    } else if (ch === "(") {
      depth++;
    } else if (ch === ")") {
      depth--;
    } else if (depth === 0) {
      // Check for " AS " at this position
      if (
        i > 0 &&
        /\s/.test(text[i - 1]) &&
        text.substring(i, i + 2).toUpperCase() === "AS" &&
        i + 2 < text.length &&
        /\s/.test(text[i + 2])
      ) {
        lastAsIndex = i;
      }
    }
  }

  if (lastAsIndex >= 0) {
    const expression = text.substring(0, lastAsIndex).trimEnd();
    const alias = text
      .substring(lastAsIndex + 2)
      .trim()
      .replace(/^`|`$/g, "")  // Remove backticks
      .replace(/^"|"$/g, ""); // Remove double quotes
    if (alias && /^\w+$/.test(alias)) {
      return { expression, alias };
    }
  }

  return { expression: text };
}

/**
 * Split a string on commas at the top level (depth 0).
 * Respects parentheses, single quotes, double quotes, backticks,
 * and inline comments (--).
 */
function splitAtTopLevelCommas(
  text: string
): { text: string; lineOffset: number }[] {
  const segments: { text: string; lineOffset: number }[] = [];
  let depth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inLineComment = false;
  let start = 0;
  let currentLine = 0;
  let segmentStartLine = 0;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];

    if (ch === "\n") {
      currentLine++;
      inLineComment = false;
      continue;
    }

    if (inLineComment) continue;

    if (inSingleQuote) {
      if (ch === "'" && text[i - 1] !== "\\") inSingleQuote = false;
      continue;
    }
    if (inDoubleQuote) {
      if (ch === '"' && text[i - 1] !== "\\") inDoubleQuote = false;
      continue;
    }
    if (inBacktick) {
      if (ch === "`") inBacktick = false;
      continue;
    }

    if (ch === "-" && text[i + 1] === "-") {
      inLineComment = true;
      continue;
    }

    if (ch === "'") {
      inSingleQuote = true;
    } else if (ch === '"') {
      inDoubleQuote = true;
    } else if (ch === "`") {
      inBacktick = true;
    } else if (ch === "(") {
      depth++;
    } else if (ch === ")") {
      depth--;
    } else if (ch === "," && depth === 0) {
      segments.push({
        text: text.substring(start, i),
        lineOffset: segmentStartLine,
      });
      start = i + 1;
      segmentStartLine = currentLine;
    }
  }

  // Last segment
  const remaining = text.substring(start).trim();
  if (remaining) {
    segments.push({ text: remaining, lineOffset: segmentStartLine });
  }

  return segments;
}

/**
 * Find a top-level SQL keyword (FROM, WHERE, etc.) not inside parens/quotes.
 * Returns the index in the string, or -1 if not found.
 */
function findTopLevelKeyword(text: string, keyword: string): number {
  let depth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inLineComment = false;
  const upper = keyword.toUpperCase();

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];

    if (ch === "\n") {
      inLineComment = false;
      continue;
    }
    if (inLineComment) continue;

    if (inSingleQuote) {
      if (ch === "'" && text[i - 1] !== "\\") inSingleQuote = false;
      continue;
    }
    if (inDoubleQuote) {
      if (ch === '"' && text[i - 1] !== "\\") inDoubleQuote = false;
      continue;
    }

    if (ch === "-" && text[i + 1] === "-") {
      inLineComment = true;
      continue;
    }
    if (ch === "'") {
      inSingleQuote = true;
      continue;
    }
    if (ch === '"') {
      inDoubleQuote = true;
      continue;
    }
    if (ch === "(") {
      depth++;
      continue;
    }
    if (ch === ")") {
      depth--;
      continue;
    }

    if (depth === 0) {
      const slice = text.substring(i, i + upper.length).toUpperCase();
      if (slice === upper) {
        // Ensure it's a word boundary
        const before = i > 0 ? text[i - 1] : " ";
        const after = text[i + upper.length] || " ";
        if (/\s/.test(before) && /[\s(]/.test(after)) {
          return i;
        }
      }
    }
  }

  return -1;
}
