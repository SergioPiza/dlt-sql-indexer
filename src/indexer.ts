import * as vscode from "vscode";
import * as path from "path";
import { IndexedModel, ModelIndex } from "./types";
import { parseSqlFile, inferLayer, inferCountry } from "./parser";
import { parseYmlFile } from "./ymlParser";
import { SchemaResolver } from "./schema/schemaResolver";

export class DltModelIndexer {
  private index: ModelIndex = {
    models: new Map(),
    referencedBy: new Map(),
    fileToModel: new Map(),
  };

  private outputChannel: vscode.OutputChannel;
  private watcher: vscode.FileSystemWatcher | undefined;
  private ymlWatcher: vscode.FileSystemWatcher | undefined;
  private ymlCache: Map<string, Map<string, ReturnType<typeof parseYmlFile> extends Map<string, infer V> ? V : never>> = new Map();

  constructor(outputChannel: vscode.OutputChannel) {
    this.outputChannel = outputChannel;
  }

  /** Get the configured models path glob */
  private getModelsGlob(): string {
    const config = vscode.workspace.getConfiguration("dltSqlIndexer");
    const modelsPath = config.get<string>("modelsPath", "models");
    return `**/${modelsPath}/**`;
  }

  /** Build the full index from scratch */
  async buildIndex(): Promise<void> {
    const startTime = Date.now();
    this.index = {
      models: new Map(),
      referencedBy: new Map(),
      fileToModel: new Map(),
    };

    const modelsGlob = this.getModelsGlob();

    // Index YML files first (for metadata)
    const ymlFiles = await vscode.workspace.findFiles(
      `${modelsGlob}/*.yml`,
      "**/node_modules/**"
    );
    for (const uri of ymlFiles) {
      await this.indexYmlFile(uri);
    }

    // Index SQL files
    const sqlFiles = await vscode.workspace.findFiles(
      `${modelsGlob}/*.sql`,
      "**/node_modules/**"
    );

    for (const uri of sqlFiles) {
      await this.indexSqlFile(uri);
    }

    // Build reverse reference index
    this.buildReverseIndex();

    // Resolve column schemas through the dependency chain
    this.resolveSchemas();

    const elapsed = Date.now() - startTime;
    this.outputChannel.appendLine(
      `Indexed ${this.index.models.size} models from ${sqlFiles.length} SQL files and ${ymlFiles.length} YML files in ${elapsed}ms`
    );
  }

  /** Resolve column schemas for all models in topological order */
  private resolveSchemas(): void {
    const resolver = new SchemaResolver(
      (name) => this.getModel(name),
      (msg) => this.outputChannel.appendLine(`[Schema] ${msg}`)
    );
    resolver.resolveAll(this.getAllModels());
  }

  /** Index a single SQL file */
  private async indexSqlFile(uri: vscode.Uri): Promise<void> {
    try {
      const content = Buffer.from(
        await vscode.workspace.fs.readFile(uri)
      ).toString("utf-8");

      const result = parseSqlFile(content, uri.fsPath);
      if (!result.modelName) return;

      const relativePath = this.getRelativePath(uri);
      const layer = inferLayer(relativePath);
      const country = inferCountry(relativePath);

      // Try to find YML metadata for this model
      const ymlDir = path.dirname(uri.fsPath);
      const ymlMeta = this.findYmlMetadata(result.modelName, ymlDir);

      const model: IndexedModel = {
        name: result.modelName,
        filePath: uri.fsPath,
        uri,
        definitionLine: result.definitionLine,
        layer,
        kind: result.kind,
        references: result.references,
        ctes: result.ctes,
        columns: result.columns,
        yml: ymlMeta,
        country,
        relativePath,
        rawCteBlocks: result.rawCteBlocks,
        rawSelectBody: result.rawSelectBody,
      };

      const key = result.modelName.toLowerCase();
      this.index.models.set(key, model);
      this.index.fileToModel.set(uri.fsPath, key);
    } catch (err) {
      this.outputChannel.appendLine(
        `Error indexing ${uri.fsPath}: ${err}`
      );
    }
  }

  /** Index a single YML file */
  private async indexYmlFile(uri: vscode.Uri): Promise<void> {
    try {
      const content = Buffer.from(
        await vscode.workspace.fs.readFile(uri)
      ).toString("utf-8");

      const metadata = parseYmlFile(content);
      if (metadata.size > 0) {
        this.ymlCache.set(uri.fsPath, metadata as any);
      }
    } catch (err) {
      this.outputChannel.appendLine(
        `Error parsing YML ${uri.fsPath}: ${err}`
      );
    }
  }

  /** Find YML metadata for a model by searching cached yml data */
  private findYmlMetadata(
    modelName: string,
    _dir: string
  ): IndexedModel["yml"] {
    for (const [, metadata] of this.ymlCache) {
      const meta = metadata.get(modelName);
      if (meta) return meta as any;
    }
    return undefined;
  }

  /** Build the reverse reference index (who references whom) */
  private buildReverseIndex(): void {
    this.index.referencedBy.clear();

    for (const [modelKey, model] of this.index.models) {
      for (const ref of model.references) {
        const refKey = ref.name.toLowerCase();
        if (!this.index.referencedBy.has(refKey)) {
          this.index.referencedBy.set(refKey, new Set());
        }
        this.index.referencedBy.get(refKey)!.add(modelKey);
      }
    }
  }

  /** Get relative path from the workspace models folder */
  private getRelativePath(uri: vscode.Uri): string {
    const workspaceFolder = vscode.workspace.getWorkspaceFolder(uri);
    if (!workspaceFolder) return path.basename(uri.fsPath);

    const config = vscode.workspace.getConfiguration("dltSqlIndexer");
    const modelsPath = config.get<string>("modelsPath", "models");
    const modelsRoot = path.join(workspaceFolder.uri.fsPath, modelsPath);
    const rel = path.relative(modelsRoot, uri.fsPath);
    return rel.replace(/\\/g, "/");
  }

  /** Start watching for file changes */
  startWatching(): void {
    const modelsGlob = this.getModelsGlob();

    this.watcher = vscode.workspace.createFileSystemWatcher(
      `${modelsGlob}/*.sql`
    );
    this.ymlWatcher = vscode.workspace.createFileSystemWatcher(
      `${modelsGlob}/*.yml`
    );

    // SQL file changes
    this.watcher.onDidChange(async (uri) => {
      this.outputChannel.appendLine(`File changed: ${uri.fsPath}`);
      await this.indexSqlFile(uri);
      this.buildReverseIndex();
      this.resolveSchemas();
    });
    this.watcher.onDidCreate(async (uri) => {
      this.outputChannel.appendLine(`File created: ${uri.fsPath}`);
      await this.indexSqlFile(uri);
      this.buildReverseIndex();
      this.resolveSchemas();
    });
    this.watcher.onDidDelete((uri) => {
      this.outputChannel.appendLine(`File deleted: ${uri.fsPath}`);
      const key = this.index.fileToModel.get(uri.fsPath);
      if (key) {
        this.index.models.delete(key);
        this.index.fileToModel.delete(uri.fsPath);
        this.buildReverseIndex();
      }
    });

    // YML file changes
    this.ymlWatcher.onDidChange(async (uri) => {
      await this.indexYmlFile(uri);
      // Re-index SQL files to pick up new metadata
      await this.buildIndex();
    });
    this.ymlWatcher.onDidCreate(async (uri) => {
      await this.indexYmlFile(uri);
    });
    this.ymlWatcher.onDidDelete((uri) => {
      this.ymlCache.delete(uri.fsPath);
    });
  }

  /** Stop watching */
  dispose(): void {
    this.watcher?.dispose();
    this.ymlWatcher?.dispose();
  }

  // ── Public query methods ─────────────────────────────────────────────────

  /** Get a model by name (case-insensitive) */
  getModel(name: string): IndexedModel | undefined {
    return this.index.models.get(name.toLowerCase());
  }

  /** Get all models */
  getAllModels(): IndexedModel[] {
    return Array.from(this.index.models.values());
  }

  /** Get all model names */
  getAllModelNames(): string[] {
    return Array.from(this.index.models.keys());
  }

  /** Get models that reference a given model */
  getReferencedBy(modelName: string): IndexedModel[] {
    const refs = this.index.referencedBy.get(modelName.toLowerCase());
    if (!refs) return [];
    return Array.from(refs)
      .map((key) => this.index.models.get(key))
      .filter((m): m is IndexedModel => m !== undefined);
  }

  /** Get the model defined in a given file */
  getModelByFile(filePath: string): IndexedModel | undefined {
    const key = this.index.fileToModel.get(filePath);
    return key ? this.index.models.get(key) : undefined;
  }

  /** Search models by partial name */
  searchModels(query: string): IndexedModel[] {
    const lowerQuery = query.toLowerCase();
    return this.getAllModels().filter(
      (m) =>
        m.name.toLowerCase().includes(lowerQuery) ||
        m.relativePath.toLowerCase().includes(lowerQuery)
    );
  }

  /** Get the total number of indexed models */
  get modelCount(): number {
    return this.index.models.size;
  }
}
