import * as vscode from "vscode";
import * as path from "path";
import { DltModelIndexer } from "../indexer";
import { IndexedModel, ModelLayer } from "../types";

type TreeNode = LayerNode | CountryNode | ModelNode;

class LayerNode extends vscode.TreeItem {
  constructor(
    public readonly layer: ModelLayer,
    public readonly count: number
  ) {
    super(layer, vscode.TreeItemCollapsibleState.Collapsed);
    this.description = `${count} models`;
    this.contextValue = "layer";

    const icons: Record<ModelLayer, string> = {
      staging: "database",
      intermediate: "arrow-right",
      marts: "star",
      dbt: "package",
      unknown: "file",
    };
    this.iconPath = new vscode.ThemeIcon(icons[layer] || "file");
  }
}

class CountryNode extends vscode.TreeItem {
  constructor(
    public readonly country: string,
    public readonly layer: ModelLayer,
    public readonly count: number
  ) {
    super(country.toUpperCase(), vscode.TreeItemCollapsibleState.Collapsed);
    this.description = `${count} models`;
    this.contextValue = "country";
    this.iconPath = new vscode.ThemeIcon("globe");
  }
}

class ModelNode extends vscode.TreeItem {
  constructor(public readonly model: IndexedModel) {
    super(model.name, vscode.TreeItemCollapsibleState.None);

    this.description = model.kind.replace(/_/g, " ");
    this.tooltip = model.yml?.description || model.relativePath;
    this.contextValue = "model";

    this.command = {
      command: "vscode.open",
      title: "Open Model",
      arguments: [
        model.uri,
        { selection: new vscode.Range(model.definitionLine, 0, model.definitionLine, 0) },
      ],
    };

    const kindIcons: Record<string, string> = {
      live_table: "table",
      live_view: "eye",
      materialized_view: "layers",
      streaming_table: "pulse",
      unknown: "file-code",
    };
    this.iconPath = new vscode.ThemeIcon(kindIcons[model.kind] || "file-code");
  }
}

/**
 * Tree data provider for the DLT Models sidebar panel.
 * Hierarchy: Layer -> Country -> Model
 */
export class DltModelTreeProvider
  implements vscode.TreeDataProvider<TreeNode>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeNode | undefined | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  constructor(private indexer: DltModelIndexer) {}

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: TreeNode): vscode.TreeItem {
    return element;
  }

  getChildren(element?: TreeNode): TreeNode[] {
    if (!element) {
      return this.getLayerNodes();
    }

    if (element instanceof LayerNode) {
      return this.getCountryOrModelNodes(element.layer);
    }

    if (element instanceof CountryNode) {
      return this.getModelNodes(element.layer, element.country);
    }

    return [];
  }

  private getLayerNodes(): LayerNode[] {
    const models = this.indexer.getAllModels();
    const layers = new Map<ModelLayer, number>();

    for (const model of models) {
      layers.set(model.layer, (layers.get(model.layer) || 0) + 1);
    }

    const order: ModelLayer[] = [
      "staging",
      "intermediate",
      "marts",
      "dbt",
      "unknown",
    ];

    return order
      .filter((l) => layers.has(l))
      .map((l) => new LayerNode(l, layers.get(l)!));
  }

  private getCountryOrModelNodes(layer: ModelLayer): TreeNode[] {
    const models = this.indexer
      .getAllModels()
      .filter((m) => m.layer === layer);

    // Group by country
    const countries = new Map<string, IndexedModel[]>();
    const noCountry: IndexedModel[] = [];

    for (const model of models) {
      if (model.country) {
        const list = countries.get(model.country) || [];
        list.push(model);
        countries.set(model.country, list);
      } else {
        noCountry.push(model);
      }
    }

    const nodes: TreeNode[] = [];

    // Country groups
    for (const [country, countryModels] of countries) {
      nodes.push(new CountryNode(country, layer, countryModels.length));
    }

    // Sort country nodes
    nodes.sort((a, b) => {
      if (a instanceof CountryNode && b instanceof CountryNode) {
        return a.country.localeCompare(b.country);
      }
      return 0;
    });

    // Models without a country
    for (const model of noCountry.sort((a, b) =>
      a.name.localeCompare(b.name)
    )) {
      nodes.push(new ModelNode(model));
    }

    return nodes;
  }

  private getModelNodes(layer: ModelLayer, country: string): ModelNode[] {
    return this.indexer
      .getAllModels()
      .filter((m) => m.layer === layer && m.country === country)
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((m) => new ModelNode(m));
  }
}
