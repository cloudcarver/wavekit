"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  CheckIcon,
  DragHandleDots2Icon,
  PlayIcon,
  ReloadIcon,
  TrashIcon,
} from "@radix-ui/react-icons";
import {
  DndContext,
  KeyboardSensor,
  PointerSensor,
  closestCenter,
  type DragEndEvent,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import {
  SortableContext,
  arrayMove,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { useRouter } from "next/navigation";

import {
  BackgroundDdlStatus,
  DefaultService,
  NotebookCellType,
  type BackgroundDdlJob,
  type BackgroundDdlProgress,
  type Cluster,
  type Notebook,
  type NotebookCell,
  type RelationCategory,
  type SqlExecutionResult,
} from "@/api-gen";
import { CodeEditor } from "@/components/code-editor";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Select } from "@/components/ui/select";
import { getErrorMessage } from "@/lib/api";
import { cn } from "@/lib/utils";

type NotebookWorkspaceProps = {
  notebookId: string;
};

type ClusterExplorerState = {
  expanded: boolean;
  loadingDatabases?: boolean;
  databases?: string[];
  expandedDatabases: Record<string, boolean>;
  expandedSchemas: Record<string, boolean>;
  expandedTypes: Record<string, boolean>;
  expandedRelations: Record<string, boolean>;
  loadingRelations: Record<string, boolean>;
  relationsByDatabase: Record<string, RelationCategory[]>;
};

type ExecutionState = {
  cellUuid: string;
  title: string;
  result?: SqlExecutionResult;
  error?: string;
  notice?: string;
};

const emptyExplorerState = (): ClusterExplorerState => ({
  expanded: false,
  expandedDatabases: {},
  expandedSchemas: {},
  expandedTypes: {},
  expandedRelations: {},
  loadingRelations: {},
  relationsByDatabase: {},
});

const explorerButtonClass =
  "flex w-full items-center gap-1.5 rounded-sm border border-transparent px-1.5 py-1 text-left text-[11px] font-medium tracking-[-0.01em] text-foreground outline-none transition-[background-color,border-color,color,box-shadow] hover:border-[#eaecf6] hover:bg-[#f7f8ff] hover:text-[#1f2230] focus-visible:border-[#d9ddff] focus-visible:ring-2 focus-visible:ring-ring/15";

export function NotebookWorkspace({ notebookId }: NotebookWorkspaceProps) {
  const router = useRouter();
  const [notebook, setNotebook] = useState<Notebook | null>(null);
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [explorer, setExplorer] = useState<Record<string, ClusterExplorerState>>({});
  const [activeExecution, setActiveExecution] = useState<ExecutionState | null>(null);
  const [backgroundDdlJobs, setBackgroundDdlJobs] = useState<BackgroundDdlJob[]>([]);
  const [backgroundDdlJobsLoading, setBackgroundDdlJobsLoading] = useState(true);
  const [backgroundDdlJobsError, setBackgroundDdlJobsError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [explorerWidth, setExplorerWidth] = useState(280);
  const [notebookWidth, setNotebookWidth] = useState(760);
  const [isResizingExplorer, setIsResizingExplorer] = useState(false);
  const [isResizingNotebook, setIsResizingNotebook] = useState(false);
  const workspaceLayoutRef = useRef<HTMLElement | null>(null);
  const notebookPanelRef = useRef<HTMLDivElement | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [notebookResponse, clusterResponse] = await Promise.all([
        DefaultService.getNotebook(notebookId),
        DefaultService.listClusters(),
      ]);
      setNotebook(sortCells(notebookResponse));
      setClusters(clusterResponse.clusters);
      setExplorer((current) => {
        const next = { ...current };
        for (const cluster of clusterResponse.clusters) {
          next[cluster.clusterUuid] = next[cluster.clusterUuid] ?? emptyExplorerState();
        }
        return next;
      });
    } catch (refreshError) {
      setError(getErrorMessage(refreshError));
    } finally {
      setLoading(false);
    }
  }, [notebookId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const loadBackgroundDdlJobs = useCallback(async () => {
    setBackgroundDdlJobsLoading(true);
    setBackgroundDdlJobsError(null);
    try {
      const response = await DefaultService.listBackgroundDdls();
      setBackgroundDdlJobs(response.jobs);
    } catch (jobsError) {
      setBackgroundDdlJobsError(getErrorMessage(jobsError));
    } finally {
      setBackgroundDdlJobsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadBackgroundDdlJobs();

    const interval = window.setInterval(() => {
      void loadBackgroundDdlJobs();
    }, 3_000);

    return () => {
      window.clearInterval(interval);
    };
  }, [loadBackgroundDdlJobs]);

  async function refreshExplorer() {
    const snapshot = explorer;

    setBusy(true);
    setError(null);
    try {
      await refresh();
      for (const [clusterUuid, clusterState] of Object.entries(snapshot)) {
        if (!clusterState.expanded) {
          continue;
        }
        await loadDatabases(clusterUuid);
        for (const [database, expanded] of Object.entries(clusterState.expandedDatabases)) {
          if (expanded) {
            await loadRelations(clusterUuid, database);
          }
        }
      }
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    if (!isResizingExplorer && !isResizingNotebook) {
      return;
    }

    const handleMouseMove = (event: MouseEvent) => {
      if (isResizingExplorer) {
        const containerLeft = workspaceLayoutRef.current?.getBoundingClientRect().left ?? 0;
        const nextWidth = event.clientX - containerLeft;
        setExplorerWidth(Math.min(420, Math.max(220, nextWidth)));
      }

      if (isResizingNotebook) {
        const panelLeft = notebookPanelRef.current?.getBoundingClientRect().left ?? 0;
        const nextWidth = event.clientX - panelLeft;
        setNotebookWidth(Math.min(1080, Math.max(560, nextWidth)));
      }
    };

    const stopResizing = () => {
      setIsResizingExplorer(false);
      setIsResizingNotebook(false);
    };

    const previousCursor = document.body.style.cursor;
    const previousUserSelect = document.body.style.userSelect;
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", stopResizing);

    return () => {
      document.body.style.cursor = previousCursor;
      document.body.style.userSelect = previousUserSelect;
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", stopResizing);
    };
  }, [isResizingExplorer, isResizingNotebook]);

  async function loadDatabases(clusterUuid: string) {
    setExplorer((current) => ({
      ...current,
      [clusterUuid]: {
        ...(current[clusterUuid] ?? emptyExplorerState()),
        loadingDatabases: true,
      },
    }));

    try {
      const response = await DefaultService.listClusterDatabases(clusterUuid);
      setExplorer((current) => ({
        ...current,
        [clusterUuid]: {
          ...(current[clusterUuid] ?? emptyExplorerState()),
          loadingDatabases: false,
          databases: response.databases.map((database) => database.name),
        },
      }));
    } catch (loadError) {
      setError(getErrorMessage(loadError));
      setExplorer((current) => ({
        ...current,
        [clusterUuid]: {
          ...(current[clusterUuid] ?? emptyExplorerState()),
          loadingDatabases: false,
        },
      }));
    }
  }

  async function loadRelations(clusterUuid: string, database: string) {
    setExplorer((current) => ({
      ...current,
      [clusterUuid]: {
        ...(current[clusterUuid] ?? emptyExplorerState()),
        loadingRelations: {
          ...(current[clusterUuid]?.loadingRelations ?? {}),
          [database]: true,
        },
      },
    }));

    try {
      const response = await DefaultService.listClusterRelations(clusterUuid, database);
      setExplorer((current) => ({
        ...current,
        [clusterUuid]: {
          ...(current[clusterUuid] ?? emptyExplorerState()),
          loadingRelations: {
            ...(current[clusterUuid]?.loadingRelations ?? {}),
            [database]: false,
          },
          relationsByDatabase: {
            ...(current[clusterUuid]?.relationsByDatabase ?? {}),
            [database]: response.categories,
          },
        },
      }));
    } catch (loadError) {
      setError(getErrorMessage(loadError));
      setExplorer((current) => ({
        ...current,
        [clusterUuid]: {
          ...(current[clusterUuid] ?? emptyExplorerState()),
          loadingRelations: {
            ...(current[clusterUuid]?.loadingRelations ?? {}),
            [database]: false,
          },
        },
      }));
    }
  }

  async function toggleCluster(clusterUuid: string) {
    const clusterState = explorer[clusterUuid] ?? emptyExplorerState();
    const expanded = !clusterState.expanded;
    setExplorer((current) => ({
      ...current,
      [clusterUuid]: {
        ...(current[clusterUuid] ?? emptyExplorerState()),
        expanded,
      },
    }));
    if (expanded && !clusterState.databases && !clusterState.loadingDatabases) {
      await loadDatabases(clusterUuid);
    }
  }

  async function toggleDatabase(clusterUuid: string, database: string) {
    const clusterState = explorer[clusterUuid] ?? emptyExplorerState();
    const expanded = !clusterState.expandedDatabases[database];
    setExplorer((current) => ({
      ...current,
      [clusterUuid]: {
        ...(current[clusterUuid] ?? emptyExplorerState()),
        expandedDatabases: {
          ...(current[clusterUuid]?.expandedDatabases ?? {}),
          [database]: expanded,
        },
      },
    }));
    if (expanded && !clusterState.relationsByDatabase[database] && !clusterState.loadingRelations[database]) {
      await loadRelations(clusterUuid, database);
    }
  }

  function toggleSchema(clusterUuid: string, database: string, schemaName: string) {
    const schemaKey = getSchemaNodeKey(database, schemaName);
    setExplorer((current) => ({
      ...current,
      [clusterUuid]: {
        ...(current[clusterUuid] ?? emptyExplorerState()),
        expandedSchemas: {
          ...(current[clusterUuid]?.expandedSchemas ?? {}),
          [schemaKey]: !(current[clusterUuid]?.expandedSchemas ?? {})[schemaKey],
        },
      },
    }));
  }

  function toggleType(clusterUuid: string, database: string, schemaName: string, typeName: string) {
    const typeKey = getTypeNodeKey(database, schemaName, typeName);
    setExplorer((current) => ({
      ...current,
      [clusterUuid]: {
        ...(current[clusterUuid] ?? emptyExplorerState()),
        expandedTypes: {
          ...(current[clusterUuid]?.expandedTypes ?? {}),
          [typeKey]: !(current[clusterUuid]?.expandedTypes ?? {})[typeKey],
        },
      },
    }));
  }

  function toggleRelation(clusterUuid: string, database: string, schemaName: string, relationName: string) {
    const relationKey = getRelationNodeKey(database, schemaName, relationName);
    setExplorer((current) => ({
      ...current,
      [clusterUuid]: {
        ...(current[clusterUuid] ?? emptyExplorerState()),
        expandedRelations: {
          ...(current[clusterUuid]?.expandedRelations ?? {}),
          [relationKey]: !(current[clusterUuid]?.expandedRelations ?? {})[relationKey],
        },
      },
    }));
  }

  function updateLocalCell(cellUuid: string, updater: (cell: NotebookCell) => NotebookCell) {
    setNotebook((current) => {
      if (!current) {
        return current;
      }
      return {
        ...current,
        cells: current.cells.map((cell) => (cell.cellUuid === cellUuid ? updater(cell) : cell)),
      };
    });
  }

  async function saveCell(cell: NotebookCell) {
    setBusy(true);
    setError(null);
    try {
      const updated = await DefaultService.updateNotebookCell(notebookId, cell.cellUuid, {
        cellType: cell.cellType,
        clusterUuid: cell.clusterUuid ?? undefined,
        database: cell.database ?? undefined,
        backgroundDdl: cell.backgroundDdl,
        content: cell.content,
      });
      updateLocalCell(cell.cellUuid, () => updated);
      await refresh();
    } catch (saveError) {
      setError(getErrorMessage(saveError));
    } finally {
      setBusy(false);
    }
  }

  async function addCell(cellType: NotebookCellType) {
    setBusy(true);
    setError(null);
    try {
      await DefaultService.createNotebookCell(notebookId, {
        cellType,
        clusterUuid: null,
        database: null,
        backgroundDdl: false,
        content: cellType === NotebookCellType.SQL ? "SELECT 1;" : "echo 'hello wavekit'",
      });
      await refresh();
    } catch (createError) {
      setError(getErrorMessage(createError));
    } finally {
      setBusy(false);
    }
  }

  async function deleteCell(cellUuid: string) {
    setBusy(true);
    setError(null);
    try {
      await DefaultService.deleteNotebookCell(notebookId, cellUuid);
      setActiveExecution((current) => (current?.cellUuid === cellUuid ? null : current));
      await refresh();
    } catch (deleteError) {
      setError(getErrorMessage(deleteError));
    } finally {
      setBusy(false);
    }
  }

  async function handleDragEnd(event: DragEndEvent) {
    if (!notebook || !event.over || event.active.id === event.over.id) {
      return;
    }

    const oldIndex = notebook.cells.findIndex((cell) => cell.cellUuid === event.active.id);
    const newIndex = notebook.cells.findIndex((cell) => cell.cellUuid === event.over?.id);
    if (oldIndex < 0 || newIndex < 0) {
      return;
    }

    const reordered = arrayMove(notebook.cells, oldIndex, newIndex).map((cell, index) => ({
      ...cell,
      orderIndex: index,
    }));
    const previousNotebook = notebook;

    setNotebook({
      ...notebook,
      cells: reordered,
    });
    setBusy(true);
    setError(null);
    try {
      await DefaultService.reorderNotebookCells(notebookId, {
        cellUuids: reordered.map((item) => item.cellUuid),
      });
    } catch (reorderError) {
      setNotebook(previousNotebook);
      setError(getErrorMessage(reorderError));
    } finally {
      setBusy(false);
    }
  }

  async function runCell(cell: NotebookCell) {
    if (cell.cellType === NotebookCellType.SHELL) {
      setActiveExecution({
        cellUuid: cell.cellUuid,
        title: "Shell execution",
        error: "Shell execution is reserved for future work. For now, notebook execution supports SQL cells only.",
      });
      return;
    }

    if (!cell.clusterUuid || !cell.database) {
      setActiveExecution({
        cellUuid: cell.cellUuid,
        title: "SQL execution",
        error: "Select both a cluster and a database before running a SQL cell.",
      });
      return;
    }

    setBusy(true);
    setError(null);
    try {
      const title = `${findClusterName(clusters, cell.clusterUuid)} / ${cell.database}`;

      if (cell.backgroundDdl) {
        const job = await DefaultService.createBackgroundDdl({
          clusterUuid: cell.clusterUuid,
          database: cell.database,
          statement: cell.content,
        });
        setActiveExecution({
          cellUuid: cell.cellUuid,
          title,
          notice: `Background DDL job queued: ${job.id}`,
        });
        void loadBackgroundDdlJobs();
        return;
      }

      const result = await DefaultService.executeClusterSql(cell.clusterUuid, cell.database, {
        statement: cell.content,
      });
      if (result.error) {
        setActiveExecution({
          cellUuid: cell.cellUuid,
          title,
          error: result.error,
        });
        return;
      }
      setActiveExecution({
        cellUuid: cell.cellUuid,
        title,
        result,
      });
    } catch (runError) {
      setActiveExecution({
        cellUuid: cell.cellUuid,
        title: `${findClusterName(clusters, cell.clusterUuid)} / ${cell.database}`,
        error: getErrorMessage(runError),
      });
    } finally {
      setBusy(false);
    }
  }

  const clusterOptions = useMemo(
    () => clusters.map((cluster) => ({ value: cluster.clusterUuid, label: cluster.clusterName })),
    [clusters],
  );
  const cellIds = useMemo(() => (notebook?.cells ?? []).map((cell) => cell.cellUuid), [notebook?.cells]);
  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: { distance: 6 },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    }),
  );

  return (
    <main className="min-h-screen bg-background px-3 py-4 md:px-4 lg:px-5">
      <div className="mx-auto flex max-w-[1600px] flex-col gap-5">
        <Card className="border-border bg-card">
          <CardContent className="grid gap-3 p-4 md:grid-cols-[minmax(0,1fr)_auto] md:items-center md:p-5">
            <div className="max-w-xl">
              <Badge variant="secondary">
                Notebook workspace
              </Badge>
              <p className="mt-2 text-lg font-semibold tracking-tight">
                {notebook?.notebookName ?? "Loading notebook…"}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button onClick={() => void refresh()} size="sm" variant="outline">
                Refresh
              </Button>
              <Button onClick={() => router.push("/")} size="sm" variant="secondary">
                Back to dashboard
              </Button>
            </div>
          </CardContent>
        </Card>

        {error ? (
          <Alert variant="destructive">
            <AlertTitle>Request failed</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : null}

        <section
          ref={workspaceLayoutRef}
          className="grid min-h-[70vh] gap-5 xl:[grid-template-columns:var(--explorer-width)_var(--notebook-width)_minmax(280px,1fr)]"
          style={{ ["--explorer-width" as string]: `${explorerWidth}px`, ["--notebook-width" as string]: `${notebookWidth}px` }}
        >
          <div className="relative self-start xl:sticky xl:top-4">
            <div className="flex flex-col gap-5">
              <Card className="border-border bg-card">
              <CardHeader className="flex flex-row items-start justify-between gap-3 border-b border-border/70 pb-3">
                <div>
                  <CardTitle>Cluster explorer</CardTitle>
                  <CardDescription>Expand databases, then browse by schema, type, and relation.</CardDescription>
                </div>
                <Button disabled={busy} onClick={() => void refreshExplorer()} size="sm" variant="outline">
                  <ReloadIcon className="h-3.5 w-3.5" />
                  Refresh
                </Button>
              </CardHeader>
              <CardContent className="space-y-3 p-3 md:p-4">
                {clusters.length > 0 ? (
                  <div className="rounded-md border border-border/80 bg-white px-2 py-2">
                    {clusters.map((cluster) => {
                      const clusterState = explorer[cluster.clusterUuid] ?? emptyExplorerState();
                      return (
                        <div key={cluster.clusterUuid}>
                          <button
                            className={explorerButtonClass}
                            onClick={() => void toggleCluster(cluster.clusterUuid)}
                            type="button"
                          >
                            <ExplorerChevron open={clusterState.expanded} />
                            <ExplorerIcon tone="cluster" />
                            <div className="min-w-0 flex-1">
                              <div className="truncate text-[11px] font-medium tracking-[-0.01em] text-foreground">
                                {cluster.clusterName}
                              </div>
                            </div>
                          </button>

                          {clusterState.expanded ? (
                            <div className="ml-[9px] border-l border-border/60 pl-3">
                              {clusterState.loadingDatabases ? (
                                <p className="px-1.5 py-1 text-[11px] text-muted-foreground">Loading databases…</p>
                              ) : null}
                              {(clusterState.databases ?? []).map((database) => (
                                <div key={database}>
                                  <button
                                    className={explorerButtonClass}
                                    onClick={() => void toggleDatabase(cluster.clusterUuid, database)}
                                    type="button"
                                  >
                                    <ExplorerChevron open={!!clusterState.expandedDatabases[database]} />
                                    <ExplorerIcon tone="database" />
                                    <span className="min-w-0 truncate text-[11px] font-medium tracking-[-0.01em] text-foreground">
                                      {database}
                                    </span>
                                  </button>

                                  {clusterState.expandedDatabases[database] ? (
                                    <div className="ml-[9px] border-l border-border/50 pl-3">
                                      {clusterState.loadingRelations[database] ? (
                                        <p className="px-1.5 py-1 text-[11px] text-muted-foreground">Loading relations…</p>
                                      ) : null}
                                      {groupSchemaExplorer(clusterState.relationsByDatabase[database] ?? []).map((schemaGroup) => {
                                        const schemaKey = getSchemaNodeKey(database, schemaGroup.schemaName);
                                        const schemaExpanded = !!clusterState.expandedSchemas[schemaKey];

                                        return (
                                          <div key={`${database}-${schemaGroup.schemaName}`}>
                                            <button
                                              className={explorerButtonClass}
                                              onClick={() => toggleSchema(cluster.clusterUuid, database, schemaGroup.schemaName)}
                                              type="button"
                                            >
                                              <ExplorerChevron open={schemaExpanded} />
                                              <ExplorerIcon tone="schema" />
                                              <span className="min-w-0 truncate text-[11px] font-medium tracking-[-0.01em] text-foreground">
                                                {schemaGroup.schemaName}
                                              </span>
                                            </button>

                                            {schemaExpanded ? (
                                              <div className="ml-[9px] border-l border-border/40 pl-3">
                                                {schemaGroup.categories.map((category) => {
                                                  const typeKey = getTypeNodeKey(
                                                    database,
                                                    schemaGroup.schemaName,
                                                    category.name,
                                                  );
                                                  const typeExpanded = !!clusterState.expandedTypes[typeKey];

                                                  return (
                                                    <div key={`${database}-${schemaGroup.schemaName}-${category.name}`}>
                                                      <button
                                                        className={explorerButtonClass}
                                                        onClick={() =>
                                                          toggleType(
                                                            cluster.clusterUuid,
                                                            database,
                                                            schemaGroup.schemaName,
                                                            category.name,
                                                          )
                                                        }
                                                        type="button"
                                                      >
                                                        <ExplorerChevron open={typeExpanded} />
                                                        <ExplorerIcon tone="type" />
                                                        <span className="min-w-0 truncate text-[10px] font-medium uppercase tracking-[0.14em] text-muted-foreground">
                                                          {category.name}
                                                        </span>
                                                      </button>

                                                      {typeExpanded ? (
                                                        <div className="ml-[9px] border-l border-border/40 pl-3">
                                                          {category.relations.map((relation) => {
                                                            const relationKey = getRelationNodeKey(
                                                              database,
                                                              schemaGroup.schemaName,
                                                              relation.relationName,
                                                            );
                                                            const relationExpanded = !!clusterState.expandedRelations[relationKey];

                                                            return (
                                                              <div key={relationKey}>
                                                                <button
                                                                  className={explorerButtonClass}
                                                                  onClick={() =>
                                                                    toggleRelation(
                                                                      cluster.clusterUuid,
                                                                      database,
                                                                      schemaGroup.schemaName,
                                                                      relation.relationName,
                                                                    )
                                                                  }
                                                                  type="button"
                                                                >
                                                                  <ExplorerChevron open={relationExpanded} />
                                                                  <ExplorerIcon tone="relation" />
                                                                  <span className="min-w-0 truncate text-[11px] font-medium tracking-[-0.01em] text-foreground">
                                                                    {relation.relationName}
                                                                  </span>
                                                                </button>

                                                                {relationExpanded ? (
                                                                  <div className="ml-[9px] border-l border-border/40 pl-3">
                                                                    {(relation.columns ?? []).map((column) => (
                                                                      <div
                                                                        key={`${schemaGroup.schemaName}.${relation.relationName}.${column.name}`}
                                                                        className="flex items-center gap-2 rounded-sm px-1.5 py-1"
                                                                      >
                                                                        <ExplorerIcon tone="column" />
                                                                        <span className="min-w-0 flex-1 truncate text-[11px] font-medium tracking-[-0.01em] text-foreground">
                                                                          {column.name}
                                                                        </span>
                                                                        <span className="truncate text-[10px] font-medium tracking-[-0.01em] text-muted-foreground">
                                                                          {column.dataType}
                                                                        </span>
                                                                      </div>
                                                                    ))}
                                                                  </div>
                                                                ) : null}
                                                              </div>
                                                            );
                                                          })}
                                                        </div>
                                                      ) : null}
                                                    </div>
                                                  );
                                                })}
                                              </div>
                                            ) : null}
                                          </div>
                                        );
                                      })}
                                    </div>
                                  ) : null}
                                </div>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      );
                    })}
                  </div>
                ) : null}
                {!loading && clusters.length === 0 ? (
                  <EmptyState
                    description="Return to the dashboard to add a cluster before exploring metadata."
                    title="No clusters connected"
                  />
                ) : null}
              </CardContent>
            </Card>

              <Card className="border-border bg-card">
                <CardHeader className="border-b border-border/70 pb-3">
                  <CardTitle>Background DDL jobs</CardTitle>
                  <CardDescription>Polling background DDL jobs and per-statement progress every 3 seconds.</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3 p-3 md:p-4">
                  {backgroundDdlJobsError ? (
                    <Alert variant="destructive">
                      <AlertTitle>Job polling failed</AlertTitle>
                      <AlertDescription>{backgroundDdlJobsError}</AlertDescription>
                    </Alert>
                  ) : null}
                  {backgroundDdlJobsLoading && backgroundDdlJobs.length === 0 ? (
                    <p className="text-xs text-muted-foreground">Loading jobs…</p>
                  ) : null}
                  {!backgroundDdlJobsLoading && backgroundDdlJobs.length === 0 ? (
                    <EmptyState description="Run a SQL cell with Background DDL enabled to create a tracked job." title="No background DDL jobs" />
                  ) : null}
                  {backgroundDdlJobs.map((job) => (
                    <BackgroundDdlJobCard key={job.id} clusters={clusters} job={job} />
                  ))}
                </CardContent>
              </Card>
            </div>
            <button
              aria-label="Resize cluster explorer"
              className="absolute -right-2 top-0 hidden h-full w-4 cursor-col-resize xl:flex xl:items-center xl:justify-center"
              onMouseDown={() => setIsResizingExplorer(true)}
              type="button"
            >
              <span
                className={cn(
                  "h-12 w-[3px] rounded-full bg-border/80 transition-colors",
                  isResizingExplorer && "bg-[#bfc6ff]",
                )}
              />
            </button>
          </div>

          <div ref={notebookPanelRef} className="relative min-w-0">
            <Card className="min-w-0 border-border bg-card">
              <CardHeader className="flex flex-col gap-3 border-b border-border/70 pb-3 md:flex-row md:items-start md:justify-between">
              <div>
                <CardTitle>Notebook cells</CardTitle>
                <CardDescription>Each cell stores its own cluster and database context.</CardDescription>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button onClick={() => void addCell(NotebookCellType.SQL)} size="sm" variant="outline">
                  Add SQL cell
                </Button>
                <Button onClick={() => void addCell(NotebookCellType.SHELL)} size="sm" variant="outline">
                  Add shell cell
                </Button>
              </div>
            </CardHeader>
              <CardContent className="space-y-3 p-3 md:p-4">
                {!loading && notebook && notebook.cells.length === 0 ? (
                  <EmptyState
                    description="Add a SQL cell to start building a notebook-driven workflow."
                    title="No cells yet"
                  />
                ) : null}
                <DndContext collisionDetection={closestCenter} onDragEnd={handleDragEnd} sensors={sensors}>
                  <SortableContext items={cellIds} strategy={verticalListSortingStrategy}>
                    {(notebook?.cells ?? []).map((cell, index) => {
                      const clusterState = cell.clusterUuid ? explorer[cell.clusterUuid] : undefined;
                      const databaseOptions = clusterState?.databases ?? [];

                      return (
                        <SortableNotebookCellCard
                          key={cell.cellUuid}
                          backgroundDDL={cell.backgroundDdl}
                          cell={cell}
                          databaseOptions={databaseOptions}
                          index={index}
                          onBackgroundDDLChange={(checked) =>
                            updateLocalCell(cell.cellUuid, (current) => ({
                              ...current,
                              backgroundDdl: checked,
                            }))
                          }
                          onCellTypeChange={(value) =>
                            updateLocalCell(cell.cellUuid, (current) => ({
                              ...current,
                              cellType: value,
                            }))
                          }
                          onClusterChange={(value) => {
                            const nextCluster = value || null;
                            updateLocalCell(cell.cellUuid, (current) => ({
                              ...current,
                              clusterUuid: nextCluster,
                              database: null,
                            }));
                            if (nextCluster) {
                              void loadDatabases(nextCluster);
                            }
                          }}
                          onDatabaseChange={(value) =>
                            updateLocalCell(cell.cellUuid, (current) => ({
                              ...current,
                              database: value || null,
                            }))
                          }
                          onDelete={() => void deleteCell(cell.cellUuid)}
                          onRun={() => void runCell(cell)}
                          onSave={() => void saveCell(cell)}
                          onValueChange={(value) =>
                            updateLocalCell(cell.cellUuid, (current) => ({
                              ...current,
                              content: value,
                            }))
                          }
                          clusterOptions={clusterOptions}
                        />
                      );
                    })}
                  </SortableContext>
                </DndContext>
              </CardContent>
            </Card>
            <button
              aria-label="Resize notebook cells"
              className="absolute -right-2 top-0 hidden h-full w-4 cursor-col-resize xl:flex xl:items-center xl:justify-center"
              onMouseDown={() => setIsResizingNotebook(true)}
              type="button"
            >
              <span
                className={cn(
                  "h-12 w-[3px] rounded-full bg-border/80 transition-colors",
                  isResizingNotebook && "bg-[#bfc6ff]",
                )}
              />
            </button>
          </div>

          <Card className="self-start border-border bg-card xl:sticky xl:top-4">
            <CardHeader className="border-b border-border/70 pb-3">
              <CardTitle>Execution results</CardTitle>
              <CardDescription>The latest executed cell result stays visible for inspection.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-3 p-3 md:p-4">
              {activeExecution?.error ? (
                <Alert variant="destructive">
                  <AlertTitle className="text-[11px] uppercase tracking-[0.14em]">{activeExecution.title}</AlertTitle>
                  <AlertDescription>{activeExecution.error}</AlertDescription>
                </Alert>
              ) : null}
              {activeExecution?.notice ? (
                <Alert>
                  <AlertTitle className="text-[11px] uppercase tracking-[0.14em]">{activeExecution.title}</AlertTitle>
                  <AlertDescription>{activeExecution.notice}</AlertDescription>
                </Alert>
              ) : null}
              {activeExecution?.result ? (
                <div className="space-y-3">
                  <Card className="border-border/80 bg-white shadow-none">
                    <CardContent className="px-3 pb-3 pt-4">
                      <div className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground">Context</div>
                      <div className="mt-1.5 text-sm font-medium">{activeExecution.title}</div>
                      <div className="mt-1 text-xs text-muted-foreground">
                        {activeExecution.result.commandTag} · {activeExecution.result.rowsAffected} row(s) affected
                      </div>
                    </CardContent>
                  </Card>

                  <div className="overflow-hidden rounded-md border border-border/80 bg-white">
                    <div className="overflow-x-auto">
                      <table className="min-w-full border-collapse text-left text-xs">
                        <thead className="bg-[#f7f8fc] text-muted-foreground">
                          <tr>
                            {(activeExecution.result.columns ?? []).map((column) => (
                              <th key={column} className="border-b px-3 py-2 font-medium">
                                {column}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {(activeExecution.result.rows ?? []).map((row, index) => (
                            <tr
                              key={`${activeExecution.cellUuid}-${index}`}
                              className={index % 2 === 0 ? "align-top bg-white" : "align-top bg-[#fcfcff]"}
                            >
                              {(row.values ?? []).map((value, valueIndex) => (
                                <td
                                  key={`${activeExecution.cellUuid}-${index}-${valueIndex}`}
                                  className="border-b px-3 py-2 font-mono text-[11px] text-foreground"
                                >
                                  {value ?? "NULL"}
                                </td>
                              ))}
                            </tr>
                          ))}
                          {(activeExecution.result.rows ?? []).length === 0 ? (
                            <tr>
                              <td className="px-3 py-5 text-xs text-muted-foreground" colSpan={Math.max((activeExecution.result.columns ?? []).length, 1)}>
                                Query completed successfully with no tabular rows returned.
                              </td>
                            </tr>
                          ) : null}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              ) : null}
              {!activeExecution ? (
                <EmptyState description="Run a SQL cell to render its result table here." title="Nothing executed yet" />
              ) : null}
            </CardContent>
          </Card>
        </section>
      </div>

      {busy ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-3 mx-auto w-fit rounded-md border border-border bg-card px-3 py-1.5 text-xs text-muted-foreground shadow-sm">
          Working…
        </div>
      ) : null}
    </main>
  );
}

function sortCells(notebook: Notebook): Notebook {
  return {
    ...notebook,
    cells: [...notebook.cells].sort((left, right) => left.orderIndex - right.orderIndex),
  };
}

function findClusterName(clusters: Cluster[], clusterUuid: string) {
  return clusters.find((cluster) => cluster.clusterUuid === clusterUuid)?.clusterName ?? clusterUuid;
}

function getSchemaNodeKey(database: string, schemaName: string) {
  return `${database}:${schemaName}`;
}

function getTypeNodeKey(database: string, schemaName: string, typeName: string) {
  return `${database}:${schemaName}:${typeName}`;
}

function getRelationNodeKey(database: string, schemaName: string, relationName: string) {
  return `${database}:${schemaName}.${relationName}`;
}

function groupSchemaExplorer(categories: RelationCategory[]) {
  const schemaMap = new Map<string, { schemaName: string; categories: Array<{ name: string; relations: NonNullable<RelationCategory["schemas"]>[number]["relations"] }> }>();

  for (const category of categories) {
    for (const schema of category.schemas ?? []) {
      const existing = schemaMap.get(schema.schemaName) ?? { schemaName: schema.schemaName, categories: [] };
      existing.categories.push({
        name: category.name,
        relations: schema.relations ?? [],
      });
      schemaMap.set(schema.schemaName, existing);
    }
  }

  return [...schemaMap.values()].sort((left, right) => left.schemaName.localeCompare(right.schemaName));
}

function ExplorerChevron({ open }: { open: boolean }) {
  return <span className="mt-[1px] w-3 shrink-0 text-[10px] text-muted-foreground">{open ? "▾" : "▸"}</span>;
}

function ExplorerIcon({ tone }: { tone: "cluster" | "database" | "schema" | "type" | "relation" | "column" }) {
  const className =
    tone === "cluster"
      ? "h-2.5 w-2.5 rounded-[3px] border border-[#cfd4ff] bg-[#eef1ff]"
      : tone === "database"
        ? "h-2.5 w-2.5 rounded-[3px] border border-[#d9dce8] bg-[#f4f5f8]"
        : tone === "schema"
          ? "h-2 w-2 rounded-[2px] border border-[#d7dbe7] bg-[#f5f6fa]"
          : tone === "type"
            ? "h-2 w-2 rounded-[2px] border border-[#dddff3] bg-[#f7f8fc]"
            : tone === "relation"
              ? "h-2 w-2 rounded-[2px] border border-[#d7daf8] bg-[#f2f4ff]"
              : "h-1.5 w-1.5 rounded-full bg-[#a8adbd]";

  return <span className={className} />;
}

function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="rounded-md border border-dashed border-border/80 bg-[#fbfbfd] px-3 py-6 text-center">
      <h3 className="text-sm font-semibold">{title}</h3>
      <p className="mx-auto mt-1.5 max-w-md text-xs leading-5 text-muted-foreground">{description}</p>
    </div>
  );
}

function BackgroundDdlJobCard({ job, clusters }: { job: BackgroundDdlJob; clusters: Cluster[] }) {
  return (
    <Card className="border-border/80 bg-white shadow-none">
      <CardContent className="space-y-3 p-3">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="text-sm font-semibold">{findClusterName(clusters, job.clusterUuid)} / {job.database}</div>
            <div className="text-[11px] text-muted-foreground">{job.id}</div>
          </div>
          <BackgroundDdlStatusBadge status={job.status} />
        </div>

        <div className="rounded-md border border-border/70 bg-[#fbfbfd] px-2.5 py-2 font-mono text-[11px] text-foreground whitespace-pre-wrap break-words">
          {job.statement}
        </div>

        <div className="grid gap-1 text-[11px] text-muted-foreground">
          <div>Created: {formatTimestamp(job.createdAt)}</div>
          {job.startedAt ? <div>Started: {formatTimestamp(job.startedAt)}</div> : null}
          {job.cancelRequestedAt ? <div>Cancel requested: {formatTimestamp(job.cancelRequestedAt)}</div> : null}
          {job.cancelledAt ? <div>Cancelled: {formatTimestamp(job.cancelledAt)}</div> : null}
          {job.finishedAt ? <div>Finished: {formatTimestamp(job.finishedAt)}</div> : null}
          {job.failedAt ? <div>Failed: {formatTimestamp(job.failedAt)}</div> : null}
        </div>

        {job.failureReason ? (
          <Alert variant="destructive">
            <AlertTitle>Job failed</AlertTitle>
            <AlertDescription>{job.failureReason}</AlertDescription>
          </Alert>
        ) : null}

        <div className="space-y-2">
          {job.progresses.map((progress) => (
            <BackgroundDdlProgressCard key={progress.id} progress={progress} />
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function BackgroundDdlProgressCard({ progress }: { progress: BackgroundDdlProgress }) {
  return (
    <div className="space-y-2 rounded-md border border-border/80 bg-[#fbfbfd] p-2.5">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-xs font-semibold tracking-[0.01em] text-foreground">
            #{progress.seq + 1} · {progress.statementKind}
          </div>
          <div className="text-[11px] text-muted-foreground">{formatBackgroundDdlTarget(progress)}</div>
        </div>
        <BackgroundDdlStatusBadge status={progress.status} compact />
      </div>

      <div className="rounded-md border border-border/70 bg-white px-2.5 py-2 font-mono text-[11px] text-foreground whitespace-pre-wrap break-words">
        {progress.statement}
      </div>

      <div className="grid gap-1 text-[11px] text-muted-foreground">
        {progress.startedAt ? <div>Started: {formatTimestamp(progress.startedAt)}</div> : null}
        {progress.lastProgress != null ? <div>Tracked progress: {progress.lastProgress.toFixed(1)}%</div> : null}
        {progress.lastProgressTrackedAt ? <div>Last tracked at: {formatTimestamp(progress.lastProgressTrackedAt)}</div> : null}
        {progress.estimatedFinishedAt ? <div>Estimated finish: {formatTimestamp(progress.estimatedFinishedAt)}</div> : null}
        {progress.rwJobIds.length > 0 ? <div>RW job ids: {progress.rwJobIds.join(", ")}</div> : null}
        {progress.finishedAt ? <div>Finished: {formatTimestamp(progress.finishedAt)}</div> : null}
        {progress.cancelledAt ? <div>Cancelled: {formatTimestamp(progress.cancelledAt)}</div> : null}
        {progress.failedAt ? <div>Failed: {formatTimestamp(progress.failedAt)}</div> : null}
      </div>

      {progress.failureReason ? (
        <Alert variant="destructive">
          <AlertTitle>Statement failed</AlertTitle>
          <AlertDescription>{progress.failureReason}</AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}

function BackgroundDdlStatusBadge({ status, compact = false }: { status: BackgroundDdlStatus; compact?: boolean }) {
  return (
    <Badge
      className={cn(
        "border-transparent capitalize",
        compact && "px-1.5 py-0 text-[10px]",
        status === BackgroundDdlStatus.FAILED && "bg-red-100 text-red-700 hover:bg-red-100",
        status === BackgroundDdlStatus.CANCELLED && "bg-slate-200 text-slate-700 hover:bg-slate-200",
        status === BackgroundDdlStatus.CANCEL_REQUESTED && "bg-amber-100 text-amber-700 hover:bg-amber-100",
        status === BackgroundDdlStatus.RUNNING && "bg-blue-100 text-blue-700 hover:bg-blue-100",
        status === BackgroundDdlStatus.FINISHED && "bg-emerald-100 text-emerald-700 hover:bg-emerald-100",
        status === BackgroundDdlStatus.PENDING && "bg-violet-100 text-violet-700 hover:bg-violet-100",
      )}
      variant="secondary"
    >
      {formatBackgroundDdlStatus(status)}
    </Badge>
  );
}

function formatBackgroundDdlStatus(status: BackgroundDdlStatus) {
  switch (status) {
    case BackgroundDdlStatus.CANCEL_REQUESTED:
      return "Cancel requested";
    case BackgroundDdlStatus.CANCELLED:
      return "Cancelled";
    case BackgroundDdlStatus.FAILED:
      return "Failed";
    case BackgroundDdlStatus.FINISHED:
      return "Finished";
    case BackgroundDdlStatus.RUNNING:
      return "Running";
    case BackgroundDdlStatus.PENDING:
    default:
      return "Pending";
  }
}

function formatBackgroundDdlTarget(progress: BackgroundDdlProgress) {
  if (progress.targetKind === "none") {
    return "Session/direct SQL";
  }
  const targetName = [progress.targetSchema, progress.targetName].filter(Boolean).join(".");
  const targetType = progress.targetType ?? progress.targetKind;
  return targetName ? `${targetType} ${targetName}` : targetType;
}

function formatTimestamp(value?: string | null) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

type SelectOption = {
  value: string;
  label: string;
};

type SortableNotebookCellCardProps = {
  cell: NotebookCell;
  index: number;
  clusterOptions: SelectOption[];
  databaseOptions: string[];
  backgroundDDL: boolean;
  onBackgroundDDLChange: (checked: boolean) => void;
  onCellTypeChange: (value: NotebookCellType) => void;
  onClusterChange: (value: string) => void;
  onDatabaseChange: (value: string) => void;
  onValueChange: (value: string) => void;
  onSave: () => void;
  onRun: () => void;
  onDelete: () => void;
};

function SortableNotebookCellCard({
  cell,
  index,
  clusterOptions,
  databaseOptions,
  backgroundDDL,
  onBackgroundDDLChange,
  onCellTypeChange,
  onClusterChange,
  onDatabaseChange,
  onValueChange,
  onSave,
  onRun,
  onDelete,
}: SortableNotebookCellCardProps) {
  const { attributes, listeners, setActivatorNodeRef, setNodeRef, transform, transition, isDragging } = useSortable({
    id: cell.cellUuid,
  });

  return (
    <div
      ref={setNodeRef}
      style={{
        transform: CSS.Transform.toString(transform),
        transition,
      }}
    >
      <Card
        className={cn(
          "border-border/80 bg-white shadow-none transition-[box-shadow,opacity]",
          isDragging && "opacity-80 shadow-[0_12px_30px_rgba(94,106,210,0.12)]",
        )}
      >
        <CardContent className="space-y-3 px-3 pb-3 pt-4">
          <div className="flex flex-col gap-3 border-b border-border/70 pb-3 xl:grid xl:grid-cols-[minmax(0,1fr)_auto] xl:items-center">
            <div className="flex items-center gap-2.5 xl:min-w-0">
              <button
                aria-label={`Reorder cell ${index + 1}`}
                className="inline-flex h-7 w-7 shrink-0 cursor-grab items-center justify-center rounded-md border border-border bg-background text-muted-foreground transition-colors hover:bg-muted hover:text-foreground active:cursor-grabbing"
                ref={setActivatorNodeRef}
                type="button"
                {...attributes}
                {...listeners}
              >
                <DragHandleDots2Icon className="h-4 w-4" />
              </button>

              <div className="grid min-w-0 flex-1 gap-2.5 md:grid-cols-3">
                <Select
                  aria-label={`Cell ${index + 1} type`}
                  onChange={(event) => onCellTypeChange(event.target.value as NotebookCellType)}
                  value={cell.cellType}
                >
                  <option value={NotebookCellType.SQL}>SQL</option>
                  <option value={NotebookCellType.SHELL}>Shell</option>
                </Select>

                <Select
                  aria-label={`Cell ${index + 1} cluster`}
                  onChange={(event) => onClusterChange(event.target.value)}
                  value={cell.clusterUuid ?? ""}
                >
                  <option value="">Select cluster</option>
                  {clusterOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </Select>

                <Select
                  aria-label={`Cell ${index + 1} database`}
                  onChange={(event) => onDatabaseChange(event.target.value)}
                  value={cell.database ?? ""}
                >
                  <option value="">Select database</option>
                  {databaseOptions.map((database) => (
                    <option key={`${cell.cellUuid}-${database}`} value={database}>
                      {database}
                    </option>
                  ))}
                </Select>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2 xl:justify-end">
              {cell.cellType === NotebookCellType.SQL ? (
                <label className="inline-flex h-8 items-center gap-2 rounded-md border border-border/80 bg-[#f8f9fd] px-2.5 text-[11px] text-foreground">
                  <input
                    checked={backgroundDDL}
                    className="h-3.5 w-3.5 rounded border-border align-middle"
                    onChange={(event) => onBackgroundDDLChange(event.target.checked)}
                    type="checkbox"
                  />
                  <span>Background DDL</span>
                </label>
              ) : null}
              <Button onClick={onSave} size="sm" variant="secondary">
                <CheckIcon className="h-3.5 w-3.5" />
                Save
              </Button>
              <Button onClick={onRun} size="sm">
                <PlayIcon className="h-3.5 w-3.5" />
                Run
              </Button>
              <Button onClick={onDelete} size="sm" variant="destructive">
                <TrashIcon className="h-3.5 w-3.5" />
                Delete
              </Button>
            </div>
          </div>

          <CodeEditor cellType={cell.cellType} onChange={onValueChange} value={cell.content} />
        </CardContent>
      </Card>
    </div>
  );
}
