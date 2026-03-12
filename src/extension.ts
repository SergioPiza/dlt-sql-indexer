import * as vscode from "vscode";
import { DltModelIndexer } from "./indexer";
import { DltDefinitionProvider } from "./providers/definitionProvider";
import { DltCompletionProvider } from "./providers/completionProvider";
import { DltHoverProvider } from "./providers/hoverProvider";
import { DltReferenceProvider } from "./providers/referenceProvider";
import { DltRenameProvider } from "./providers/renameProvider";
import { DltModelTreeProvider } from "./views/modelTreeProvider";
import { DltDiagnosticProvider } from "./diagnostics/diagnosticProvider";

let indexer: DltModelIndexer;

export async function activate(
  context: vscode.ExtensionContext
): Promise<void> {
  const outputChannel = vscode.window.createOutputChannel("DLT SQL Indexer");
  outputChannel.appendLine("DLT SQL Indexer activating...");

  // Create the indexer
  indexer = new DltModelIndexer(outputChannel);

  // Build initial index
  await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: "DLT SQL Indexer: Building model index...",
      cancellable: false,
    },
    async () => {
      await indexer.buildIndex();
    }
  );

  // Show status
  const statusBar = vscode.window.createStatusBarItem(
    vscode.StatusBarAlignment.Left,
    100
  );
  statusBar.text = `$(database) DLT: ${indexer.modelCount} models`;
  statusBar.tooltip = "DLT SQL Indexer — click to rebuild";
  statusBar.command = "dltSqlIndexer.rebuildIndex";
  statusBar.show();
  context.subscriptions.push(statusBar);

  // SQL language selector — cover language IDs set by dbt/sqlfluff extensions
  const sqlSelector: vscode.DocumentSelector = [
    { language: "sql", scheme: "file" },
    { language: "jinja-sql", scheme: "file" },
    { language: "sql-bigquery", scheme: "file" },
    { language: "sql-databricks", scheme: "file" },
    { language: "databricks-sql", scheme: "file" },
    { pattern: "**/*.sql", scheme: "file" },
  ];

  // Register providers
  context.subscriptions.push(
    vscode.languages.registerDefinitionProvider(
      sqlSelector,
      new DltDefinitionProvider(indexer)
    ),
    vscode.languages.registerCompletionItemProvider(
      sqlSelector,
      new DltCompletionProvider(indexer),
      "." // Trigger on dot after LIVE
    ),
    vscode.languages.registerHoverProvider(
      sqlSelector,
      new DltHoverProvider(indexer)
    ),
    vscode.languages.registerReferenceProvider(
      sqlSelector,
      new DltReferenceProvider(indexer)
    ),
    vscode.languages.registerRenameProvider(
      sqlSelector,
      new DltRenameProvider(indexer)
    )
  );

  // Register sidebar tree view
  const treeProvider = new DltModelTreeProvider(indexer);
  const treeView = vscode.window.createTreeView("dltModelTree", {
    treeDataProvider: treeProvider,
    showCollapseAll: true,
  });
  context.subscriptions.push(treeView);

  // Register commands
  context.subscriptions.push(
    vscode.commands.registerCommand(
      "dltSqlIndexer.rebuildIndex",
      async () => {
        await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: "DLT SQL Indexer: Rebuilding index...",
            cancellable: false,
          },
          async () => {
            await indexer.buildIndex();
            treeProvider.refresh();
            statusBar.text = `$(database) DLT: ${indexer.modelCount} models`;
            vscode.window.showInformationMessage(
              `DLT SQL Indexer: Indexed ${indexer.modelCount} models`
            );
          }
        );
      }
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      "dltSqlIndexer.showDependencyGraph",
      () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
          vscode.window.showWarningMessage("Open a SQL file first");
          return;
        }

        const model = indexer.getModelByFile(editor.document.uri.fsPath);
        if (!model) {
          vscode.window.showWarningMessage(
            "No DLT model found in this file"
          );
          return;
        }

        // Show dependencies in a quick-pick
        const deps = model.references.map((r) => ({
          label: `$(arrow-right) ${r.name}`,
          description: `${r.type} reference`,
          detail: `Line ${r.line + 1}`,
          ref: r,
        }));

        const usedBy = indexer.getReferencedBy(model.name).map((m) => ({
          label: `$(arrow-left) ${m.name}`,
          description: `${m.layer} | ${m.kind.replace(/_/g, " ")}`,
          detail: m.relativePath,
          model: m,
        }));

        const items = [
          { label: "Dependencies (this model uses)", kind: vscode.QuickPickItemKind.Separator } as vscode.QuickPickItem,
          ...deps,
          { label: "Used by (other models reference this)", kind: vscode.QuickPickItemKind.Separator } as vscode.QuickPickItem,
          ...usedBy,
        ];

        vscode.window
          .showQuickPick(items, {
            title: `Dependencies: ${model.name}`,
            placeHolder: "Select a model to navigate to",
          })
          .then((selected) => {
            if (!selected) return;
            if ("model" in selected) {
              const m = (selected as any).model;
              vscode.window.showTextDocument(m.uri, {
                selection: new vscode.Range(m.definitionLine, 0, m.definitionLine, 0),
              });
            } else if ("ref" in selected) {
              const ref = (selected as any).ref;
              const target = indexer.getModel(ref.name);
              if (target) {
                vscode.window.showTextDocument(target.uri, {
                  selection: new vscode.Range(target.definitionLine, 0, target.definitionLine, 0),
                });
              }
            }
          });
      }
    )
  );

  // Register diagnostics
  const diagnosticCollection =
    vscode.languages.createDiagnosticCollection("dlt-schema");
  context.subscriptions.push(diagnosticCollection);

  const diagnosticProvider = new DltDiagnosticProvider(
    diagnosticCollection,
    indexer,
    outputChannel
  );

  // Validate open documents
  for (const doc of vscode.workspace.textDocuments) {
    diagnosticProvider.validateDocument(doc);
  }

  // Validate on open, save, and while typing (debounced)
  context.subscriptions.push(
    vscode.workspace.onDidOpenTextDocument((doc) => {
      diagnosticProvider.validateDocument(doc);
    }),
    vscode.workspace.onDidSaveTextDocument((doc) => {
      diagnosticProvider.validateDocument(doc);
    }),
    vscode.workspace.onDidChangeTextDocument((e) => {
      diagnosticProvider.validateDocumentDebounced(e.document);
    }),
    vscode.workspace.onDidCloseTextDocument((doc) => {
      diagnosticProvider.clearDiagnostics(doc.uri);
    })
  );

  // Start file watching
  indexer.startWatching();

  // Refresh tree on index changes
  const watcher = vscode.workspace.createFileSystemWatcher(
    "**/*.sql"
  );
  watcher.onDidChange(() => treeProvider.refresh());
  watcher.onDidCreate(() => treeProvider.refresh());
  watcher.onDidDelete(() => treeProvider.refresh());
  context.subscriptions.push(watcher);

  context.subscriptions.push(indexer);

  outputChannel.appendLine(
    `DLT SQL Indexer activated with ${indexer.modelCount} models`
  );
}

export function deactivate(): void {
  indexer?.dispose();
}
