import { RawCteBlock, FromSource } from "./types";

/**
 * Extract all CTE blocks and the final SELECT body from SQL content.
 * Uses parenthesis-depth tracking to find exact CTE boundaries.
 *
 * Input: The full SQL content of a model (after the CREATE ... AS ( wrapper).
 * Returns: Array of CTE blocks + the final select body.
 */
export function extractCteBlocks(content: string): {
  ctes: RawCteBlock[];
  finalSelectBody: string;
} {
  const ctes: RawCteBlock[] = [];

  // Find the innermost body: content inside the CREATE ... AS (...)
  const innerBody = extractInnerBody(content);
  if (!innerBody.body) {
    return { ctes: [], finalSelectBody: content };
  }

  const body = innerBody.body;
  const bodyStartLine = innerBody.startLine;

  // Check if there's a WITH keyword
  const withMatch = body.match(/^\s*WITH\s/i);
  if (!withMatch) {
    // No CTEs, the whole body is the final SELECT
    return { ctes: [], finalSelectBody: body };
  }

  // Parse CTE blocks: "name AS (" ... balanced parens ... ")"
  let pos = withMatch.index! + withMatch[0].length;
  const lines = content.substring(0, innerBody.startOffset).split("\n");
  let currentLine = bodyStartLine;

  while (pos < body.length) {
    // Skip whitespace and commas
    while (pos < body.length && /[\s,]/.test(body[pos])) {
      if (body[pos] === "\n") currentLine++;
      pos++;
    }

    // Try to match "name AS ("
    const remaining = body.substring(pos);
    const cteHeaderMatch = remaining.match(/^(\w+)\s+AS\s*\(/i);
    if (!cteHeaderMatch) {
      // No more CTEs — the rest is the final SELECT
      break;
    }

    const cteName = cteHeaderMatch[1];
    const cteStartLine = currentLine;

    // Count newlines in the header
    for (const ch of cteHeaderMatch[0]) {
      if (ch === "\n") currentLine++;
    }

    // Find the matching closing parenthesis
    const openParenOffset = pos + cteHeaderMatch[0].length - 1; // position of "("
    const closeParenOffset = findMatchingParen(body, openParenOffset);

    if (closeParenOffset < 0) {
      // Unbalanced parens — take the rest as this CTE's body
      const cteBody = body.substring(openParenOffset + 1);
      ctes.push({ name: cteName, body: cteBody, startLine: cteStartLine });
      return { ctes, finalSelectBody: "" };
    }

    const cteBody = body.substring(openParenOffset + 1, closeParenOffset);
    ctes.push({ name: cteName, body: cteBody, startLine: cteStartLine });

    // Count newlines in CTE body
    for (const ch of cteBody) {
      if (ch === "\n") currentLine++;
    }
    // +1 for the closing paren line
    if (body[closeParenOffset] === "\n") currentLine++;

    pos = closeParenOffset + 1;
  }

  // Everything after the last CTE is the final SELECT body
  const finalBody = body.substring(pos).trim();
  return { ctes, finalSelectBody: finalBody };
}

/**
 * Extract the inner body of a CREATE statement.
 * For "CREATE ... AS (...body...)", returns the body inside the outer parens.
 * For models without parens after AS, returns the body after AS.
 */
function extractInnerBody(content: string): {
  body: string;
  startLine: number;
  startOffset: number;
} {
  // Find "AS (" or "AS\n("
  const asMatch = content.match(
    /\bAS\s*\(/i
  );
  if (asMatch) {
    const openParen = asMatch.index! + asMatch[0].length - 1;
    const closeParen = findMatchingParen(content, openParen);
    const body =
      closeParen >= 0
        ? content.substring(openParen + 1, closeParen)
        : content.substring(openParen + 1);

    const startLine = content.substring(0, openParen + 1).split("\n").length - 1;
    return { body, startLine, startOffset: openParen + 1 };
  }

  // Fallback: find "AS" and take everything after it
  const asSimpleMatch = content.match(/\bAS\s+/i);
  if (asSimpleMatch) {
    const offset = asSimpleMatch.index! + asSimpleMatch[0].length;
    const startLine = content.substring(0, offset).split("\n").length - 1;
    return {
      body: content.substring(offset),
      startLine,
      startOffset: offset,
    };
  }

  return { body: "", startLine: 0, startOffset: 0 };
}

/**
 * Find the matching closing parenthesis for an opening paren.
 * Respects string literals (single quotes) and line comments (--).
 */
function findMatchingParen(text: string, openIndex: number): number {
  let depth = 1;
  let inSingleQuote = false;
  let inLineComment = false;

  for (let i = openIndex + 1; i < text.length; i++) {
    const ch = text[i];

    if (ch === "\n") {
      inLineComment = false;
      continue;
    }
    if (inLineComment) continue;

    if (inSingleQuote) {
      if (ch === "'" && text[i + 1] === "'") {
        i++; // skip escaped quote
      } else if (ch === "'") {
        inSingleQuote = false;
      }
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
    if (ch === "(") {
      depth++;
    } else if (ch === ")") {
      depth--;
      if (depth === 0) return i;
    }
  }

  return -1; // unbalanced
}

/**
 * Parse the FROM/JOIN clause of a SELECT body to find source tables/CTEs and their aliases.
 * Only parses the top-level FROM/JOIN (depth 0 — not inside subqueries).
 */
export function parseFromClause(selectBody: string): FromSource[] {
  const sources: FromSource[] = [];

  // Normalize: strip comments, collapse whitespace
  const cleaned = selectBody
    .replace(/--[^\n]*/g, "")
    .replace(/\s+/g, " ")
    .trim();

  // We need to find FROM and JOIN keywords at depth 0 (not inside subqueries or CTEs)
  // Walk through the string tracking paren depth
  const tokens = findTopLevelFromJoin(cleaned);

  for (const afterKeyword of tokens) {
    const source = parseSourceRef(afterKeyword);
    if (source) {
      sources.push(source);
    }
  }

  return sources;
}

/**
 * Find all FROM and JOIN keywords at parenthesis depth 0,
 * returning the text after each keyword for source parsing.
 */
function findTopLevelFromJoin(sql: string): string[] {
  const results: string[] = [];
  let depth = 0;
  let inSingleQuote = false;
  let i = 0;

  while (i < sql.length) {
    const ch = sql[i];

    if (inSingleQuote) {
      if (ch === "'" && sql[i + 1] === "'") {
        i += 2;
      } else if (ch === "'") {
        inSingleQuote = false;
        i++;
      } else {
        i++;
      }
      continue;
    }

    if (ch === "'") {
      inSingleQuote = true;
      i++;
      continue;
    }
    if (ch === "(") {
      depth++;
      i++;
      continue;
    }
    if (ch === ")") {
      depth--;
      i++;
      continue;
    }

    if (depth > 0) {
      i++;
      continue;
    }

    // At depth 0, look for FROM or JOIN keywords
    const remaining = sql.substring(i);

    // Match FROM (but not "CROSS JOIN" etc. which we handle separately)
    const fromMatch = remaining.match(/^FROM\s+/i);
    if (fromMatch) {
      results.push(sql.substring(i + fromMatch[0].length));
      i += fromMatch[0].length;
      continue;
    }

    // Match [INNER|LEFT|RIGHT|FULL|CROSS] JOIN
    const joinMatch = remaining.match(
      /^(?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN\s+/i
    );
    if (joinMatch) {
      results.push(sql.substring(i + joinMatch[0].length));
      i += joinMatch[0].length;
      continue;
    }

    i++;
  }

  return results;
}

/** SQL keywords that should never be treated as aliases or source names */
const SQL_KEYWORDS = new Set([
  "where", "group", "order", "having", "limit", "union", "select", "qualify",
  "on", "using", "lateral", "pivot", "unpivot", "inner", "left", "right",
  "full", "cross", "join", "outer", "and", "or", "not", "in", "between",
  "like", "is", "null", "case", "when", "then", "else", "end", "as",
  "set", "into", "values", "update", "delete", "insert", "from", "with",
]);

/** Check if a word is a SQL keyword (should not be used as alias/source) */
function isSqlKeyword(word: string): boolean {
  return SQL_KEYWORDS.has(word.toLowerCase());
}

/**
 * Parse a single source reference: "LIVE.model_name [AS] alias" or "cte_name [AS] alias"
 */
function parseSourceRef(text: string): FromSource | null {
  const trimmed = text.trim();

  // LIVE.model_name [AS] alias
  const liveMatch = trimmed.match(
    /^LIVE\.(\w+)(?:\s+AS\s+(\w+)|\s+(\w+))?/i
  );
  if (liveMatch) {
    const explicitAlias = liveMatch[2]; // after AS
    const implicitAlias = liveMatch[3]; // positional (no AS)
    const alias = explicitAlias || (implicitAlias && !isSqlKeyword(implicitAlias) ? implicitAlias : null);
    return {
      sourceName: liveMatch[1],
      alias: alias || liveMatch[1],
      isLiveRef: true,
      isDbtRef: false,
    };
  }

  // {{ ref('model_name') }} [AS] alias
  const refMatch = trimmed.match(
    /^\{\{\s*ref\(\s*['"](\w+)['"]\s*\)\s*\}\}(?:\s+AS\s+(\w+)|\s+(\w+))?/i
  );
  if (refMatch) {
    const explicitAlias = refMatch[2];
    const implicitAlias = refMatch[3];
    const alias = explicitAlias || (implicitAlias && !isSqlKeyword(implicitAlias) ? implicitAlias : null);
    return {
      sourceName: refMatch[1],
      alias: alias || refMatch[1],
      isLiveRef: false,
      isDbtRef: true,
    };
  }

  // {{ source('name', 'table') }} [AS] alias
  const sourceMatch = trimmed.match(
    /^\{\{\s*source\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*\)\s*\}\}(?:\s+AS\s+(\w+)|\s+(\w+))?/i
  );
  if (sourceMatch) {
    const explicitAlias = sourceMatch[3];
    const implicitAlias = sourceMatch[4];
    const alias = explicitAlias || (implicitAlias && !isSqlKeyword(implicitAlias) ? implicitAlias : null);
    return {
      sourceName: `source:${sourceMatch[1]}.${sourceMatch[2]}`,
      alias: alias || sourceMatch[2],
      isLiveRef: false,
      isDbtRef: false,
    };
  }

  // schema.table (e.g., production.acquisition.table) [AS] alias
  const schemaTableMatch = trimmed.match(
    /^(\w+\.\w+(?:\.\w+)?)(?:\s+AS\s+(\w+)|\s+(\w+))?/i
  );
  if (schemaTableMatch) {
    const name = schemaTableMatch[1];
    if (isSqlKeyword(name.split(".")[0])) {
      return null;
    }
    const explicitAlias = schemaTableMatch[2];
    const implicitAlias = schemaTableMatch[3];
    const alias = explicitAlias || (implicitAlias && !isSqlKeyword(implicitAlias) ? implicitAlias : null);
    return {
      sourceName: name,
      alias: alias || name.split(".").pop()!,
      isLiveRef: false,
      isDbtRef: false,
    };
  }

  // Simple name (CTE reference) [AS] alias
  const simpleMatch = trimmed.match(/^(\w+)(?:\s+AS\s+(\w+)|\s+(\w+))?/i);
  if (simpleMatch) {
    const name = simpleMatch[1];
    if (isSqlKeyword(name)) {
      return null;
    }
    const explicitAlias = simpleMatch[2];
    const implicitAlias = simpleMatch[3];
    const alias = explicitAlias || (implicitAlias && !isSqlKeyword(implicitAlias) ? implicitAlias : null);
    return {
      sourceName: name,
      alias: alias || name,
      isLiveRef: false,
      isDbtRef: false,
    };
  }

  return null;
}
