import { ResolvedColumn } from "./types";

/**
 * Infer the SQL data type of an expression.
 * Uses a hierarchy of pattern-matching rules.
 *
 * @param expression  The SQL expression text
 * @param aliasColumns  Map of table alias -> resolved columns (for inheriting types)
 */
export function inferType(
  expression: string,
  aliasColumns?: Map<string, ResolvedColumn[]>
): string | undefined {
  const trimmed = expression.trim();

  // 1. CAST(x AS TYPE)
  const castMatch = trimmed.match(
    /^CAST\s*\(.+\s+AS\s+(\w+(?:\s*\([^)]*\))?)\s*\)$/i
  );
  if (castMatch) return normalizeType(castMatch[1]);

  // 2. TRY_CAST(x AS TYPE)
  const tryCastMatch = trimmed.match(
    /^TRY_CAST\s*\(.+\s+AS\s+(\w+(?:\s*\([^)]*\))?)\s*\)$/i
  );
  if (tryCastMatch) return normalizeType(tryCastMatch[1]);

  // 3. Literal string
  if (/^'[^']*'$/.test(trimmed)) return "STRING";

  // 4. Literal number
  if (/^-?\d+$/.test(trimmed)) return "BIGINT";
  if (/^-?\d+\.\d+$/.test(trimmed)) return "DOUBLE";

  // 5. NULL
  if (/^NULL$/i.test(trimmed)) return undefined;

  // 6. TRUE / FALSE
  if (/^(TRUE|FALSE)$/i.test(trimmed)) return "BOOLEAN";

  // 7. COALESCE(a, b, ...) -> type of first resolvable arg
  const coalesceMatch = trimmed.match(/^COALESCE\s*\((.+)\)$/i);
  if (coalesceMatch) {
    const args = splitTopLevelArgs(coalesceMatch[1]);
    for (const arg of args) {
      const argType = inferType(arg.trim(), aliasColumns);
      if (argType) return argType;
    }
    return undefined;
  }

  // 8. IF(cond, then, else) -> type of then
  const ifMatch = trimmed.match(/^IF\s*\((.+)\)$/i);
  if (ifMatch) {
    const args = splitTopLevelArgs(ifMatch[1]);
    if (args.length >= 2) {
      return inferType(args[1].trim(), aliasColumns);
    }
  }

  // 9. CASE WHEN ... THEN x ... END -> type of first THEN
  const caseMatch = trimmed.match(/\bTHEN\s+(.+?)(?:\s+WHEN|\s+ELSE|\s+END)/i);
  if (caseMatch) {
    return inferType(caseMatch[1].trim(), aliasColumns);
  }

  // 10. Known function return types
  if (/^(?:LOWER|UPPER|TRIM|LTRIM|RTRIM|CONCAT|MD5|SHA1|SHA2|REPLACE|REGEXP_REPLACE|SUBSTRING|SUBSTR|LEFT|RIGHT|LPAD|RPAD|INITCAP|REVERSE|SPLIT_PART|TRANSLATE|BASE64|UNBASE64|HEX|UNHEX|ARRAY_JOIN)\s*\(/i.test(trimmed))
    return "STRING";

  if (/^(?:COUNT|ROW_NUMBER|RANK|DENSE_RANK|NTILE)\s*\(/i.test(trimmed))
    return "BIGINT";

  if (/^(?:SUM|AVG)\s*\(/i.test(trimmed)) return "DOUBLE";

  if (/^(?:MAX|MIN)\s*\(/i.test(trimmed)) {
    // Try to infer from inner expression
    const innerMatch = trimmed.match(/^\w+\s*\((.+)\)$/i);
    if (innerMatch) return inferType(innerMatch[1].trim(), aliasColumns);
    return undefined;
  }

  if (/^(?:DATE|TO_DATE|DATE_TRUNC|TRUNC|DATE_ADD|DATE_SUB|ADD_MONTHS|LAST_DAY|NEXT_DAY)\s*\(/i.test(trimmed))
    return "DATE";

  if (/^(?:TIMESTAMP|TO_TIMESTAMP|CURRENT_TIMESTAMP|NOW)\s*\(/i.test(trimmed))
    return "TIMESTAMP";

  if (/^(?:ROUND|FLOOR|CEIL|CEILING|ABS|MOD|POWER|SQRT|LOG|LOG2|LOG10|EXP|SIGN)\s*\(/i.test(trimmed))
    return "DOUBLE";

  if (/^(?:SIZE|LENGTH|CHAR_LENGTH|BIT_LENGTH|OCTET_LENGTH)\s*\(/i.test(trimmed))
    return "INT";

  if (/^(?:ARRAY|COLLECT_LIST|COLLECT_SET|ARRAY_AGG)\s*\(/i.test(trimmed))
    return "ARRAY<STRING>";

  if (/^(?:MAP)\s*\(/i.test(trimmed)) return "MAP<STRING,STRING>";

  // 11. Qualified column reference (alias.col) -> inherit from source
  if (aliasColumns) {
    const qualMatch = trimmed.match(/^(\w+)\.(\w+)$/);
    if (qualMatch) {
      const cols = aliasColumns.get(qualMatch[1]);
      const col = cols?.find(
        (c) => c.name.toLowerCase() === qualMatch[2].toLowerCase()
      );
      if (col?.dataType) return col.dataType;
    }

    // Bare column reference -> search all aliases
    const bareMatch = trimmed.match(/^(\w+)$/);
    if (bareMatch) {
      for (const [, cols] of aliasColumns) {
        const col = cols.find(
          (c) => c.name.toLowerCase() === bareMatch[1].toLowerCase()
        );
        if (col?.dataType) return col.dataType;
      }
    }
  }

  // 12. Arithmetic expressions -> DOUBLE
  if (/[+\-*/]/.test(trimmed) && !trimmed.startsWith("'")) {
    return "DOUBLE";
  }

  // 13. Comparison / boolean expressions
  if (
    /\b(IS\s+(NOT\s+)?NULL|BETWEEN|LIKE|ILIKE|RLIKE|IN\s*\(|NOT\s+IN|EXISTS)\b/i.test(trimmed) ||
    /[><=!]{1,2}/.test(trimmed)
  ) {
    return "BOOLEAN";
  }

  return undefined;
}

/**
 * Normalize a SQL type name to a canonical form.
 */
function normalizeType(rawType: string): string {
  const upper = rawType.trim().toUpperCase();

  // Common aliases
  const aliases: Record<string, string> = {
    INTEGER: "INT",
    LONG: "BIGINT",
    SHORT: "SMALLINT",
    FLOAT: "FLOAT",
    REAL: "FLOAT",
    DEC: "DECIMAL",
    NUMERIC: "DECIMAL",
    CHAR: "STRING",
    VARCHAR: "STRING",
    TEXT: "STRING",
    BOOL: "BOOLEAN",
    DATETIME: "TIMESTAMP",
  };

  // Check for parameterized types like DECIMAL(17,2)
  const baseType = upper.replace(/\s*\(.*\)/, "");
  if (aliases[baseType]) {
    return upper.replace(baseType, aliases[baseType]);
  }

  return upper;
}

/**
 * Split function arguments at top-level commas (respecting nested parens/quotes).
 */
function splitTopLevelArgs(text: string): string[] {
  const args: string[] = [];
  let depth = 0;
  let inQuote = false;
  let start = 0;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inQuote) {
      if (ch === "'" && text[i + 1] !== "'") inQuote = false;
      continue;
    }
    if (ch === "'") {
      inQuote = true;
    } else if (ch === "(") {
      depth++;
    } else if (ch === ")") {
      depth--;
    } else if (ch === "," && depth === 0) {
      args.push(text.substring(start, i));
      start = i + 1;
    }
  }

  args.push(text.substring(start));
  return args;
}
