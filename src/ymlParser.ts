import { YmlMetadata } from "./types";
import * as yaml from "yaml";

interface DbtYmlModel {
  name?: string;
  description?: string;
  config?: {
    alias?: string;
    tags?: string[];
  };
  columns?: {
    name?: string;
    description?: string;
  }[];
}

interface DbtYmlSource {
  name?: string;
  description?: string;
  tables?: {
    name?: string;
    identifier?: string;
    description?: string;
  }[];
}

interface DbtYmlFile {
  version?: number;
  models?: DbtYmlModel[];
  sources?: DbtYmlSource[];
}

/**
 * Parse a dbt .yml file and return a map of model name -> metadata.
 */
export function parseYmlFile(
  content: string
): Map<string, YmlMetadata> {
  const result = new Map<string, YmlMetadata>();

  let parsed: DbtYmlFile;
  try {
    parsed = yaml.parse(content) as DbtYmlFile;
  } catch {
    return result;
  }

  if (!parsed) return result;

  // Parse model definitions
  if (Array.isArray(parsed.models)) {
    for (const model of parsed.models) {
      if (!model.name) continue;

      const metadata: YmlMetadata = {};

      if (model.description) {
        metadata.description = model.description.trim();
      }
      if (model.config?.alias) {
        metadata.alias = model.config.alias;
      }
      if (model.config?.tags) {
        metadata.tags = model.config.tags;
      }
      if (Array.isArray(model.columns)) {
        metadata.columns = model.columns
          .filter((c) => c.name)
          .map((c) => ({
            name: c.name!,
            description: c.description?.trim(),
          }));
      }

      result.set(model.name, metadata);
    }
  }

  // Parse source definitions (for hover info on source references)
  if (Array.isArray(parsed.sources)) {
    for (const source of parsed.sources) {
      if (!source.name || !Array.isArray(source.tables)) continue;

      for (const table of source.tables) {
        if (!table.name) continue;
        const key = `source:${source.name}.${table.name}`;
        result.set(key, {
          description:
            table.description?.trim() || source.description?.trim(),
        });
      }
    }
  }

  return result;
}
